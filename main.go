package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
	"github.com/itchyny/gojq"
	"go.uber.org/ratelimit"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)
var (
	dry                    = flag.Bool("dry", false, "dry run (only print messages that would be processed)")
	delete                 = flag.Bool("delete", false, "delete messages from source queue")
	move                   = flag.Bool("move", false, "move messages from source queue to target queue")
	targetQueue            = flag.String("target", "", "the queue URL, e.g. https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
	sourceQueue            = flag.String("source", "", "the queue URL, e.g. https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
	beforeFilter           = flag.String("before", "", "get only messages sent before a certain date in format 2006-01-02T15:04:05Z07:00")
	afterFilter            = flag.String("after", "", "get only messages sent after a certain date in format 2006-01-02T15:04:05Z07:00")
	numMessages            = flag.Int("count", 0, "maximum number of messages to process")
	numWorkers             = flag.Int("workers", 8, "number of workers")
	bodyFilter             = flag.String("body-filter", "", "filter messages by JSON body field content using JQ expression")
	attributeFilter        = flag.String("attribute-filter", "", "filter messages by a certain attribute using JQ expression")
	messageAttributeFilter = flag.String("message-attribute-filter", "", "filter messages by a certain message attribute using JQ expression")
	rateLimit              = flag.Int("rate-limit", 10, "Max number of messages processed per second")
	pollingDuration        = flag.Duration("polling-duration", 30*time.Second, "Polling duration")
	versionFlag            = flag.Bool("version", false, "Print version and exit")
)

var (
	after                  time.Time
	before                 time.Time
	attributesQuery        *gojq.Code
	bodyQuery              *gojq.Code
	messageAttributesQuery *gojq.Code
)

const maxSQSBatchSize = 10

func main() {
	flag.Parse()

	if *versionFlag {
		printVersion()
		return
	}

	if err := validateFlags(); err != nil {
		printVersion()
		fmt.Println(err)
		fmt.Println()
		flag.Usage()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), *pollingDuration)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Println("Configuration error", err)
		return
	}

	sqsClient := sqs.NewFromConfig(cfg)

	batches := make(chan []types.Message, *numMessages)
	wg := &sync.WaitGroup{}

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, sqsClient, batches, wg)
	}

	startPolling(ctx, sqsClient, batches)

	close(batches)
	wg.Wait()

	log.Println("Finished processing")
}

func printVersion() {
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		fmt.Printf("%s@%s commit %s (%s) %s\n", buildInfo.Path, version, commit, date, buildInfo.GoVersion)
	}
}

func validateFlags() error {
	if *move == *delete {
		return fmt.Errorf("Must specify either -delete or -move")

	}

	if *sourceQueue == "" {
		return fmt.Errorf("Missing source queue")

	}

	if *move && *targetQueue == "" {
		return fmt.Errorf("Missing target queue")

	}

	var err error
	if *attributeFilter != "" {
		attributesQuery, err = parseAndCompileJQ(*attributeFilter)
		if err != nil {
			return fmt.Errorf("Invalid attribute filter %w", err)

		}
	}

	if *messageAttributeFilter != "" {
		messageAttributesQuery, err = parseAndCompileJQ(*messageAttributeFilter)
		if err != nil {
			return fmt.Errorf("Invalid message attribute filter %w", err)
		}
	}

	if *bodyFilter != "" {
		bodyQuery, err = parseAndCompileJQ(*bodyFilter)
		if err != nil {
			return fmt.Errorf("Invalid body filter %w", err)

		}
	}

	if *beforeFilter != "" {
		before, err = time.Parse(time.RFC3339, *beforeFilter)
		if err != nil {
			return fmt.Errorf("Invalid before date %w", err)

		}
	}

	if *afterFilter != "" {
		after, err = time.Parse(time.RFC3339, *afterFilter)
		if err != nil {
			return fmt.Errorf("Invalid after date %w", err)

		}
	}

	return nil
}

func parseAndCompileJQ(jq string) (*gojq.Code, error) {
	query, err := gojq.Parse(jq)
	if err != nil {
		return nil, err
	}

	code, err := gojq.Compile(query)
	if err != nil {
		return nil, err
	}

	return code, nil
}

func worker(ctx context.Context, sqsClient *sqs.Client, batches <-chan []types.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range batches {
		switch {
		case *dry:
			printSQSMessages(batch)
		case *move:
			redriveSQSMessages(ctx, batch, sqsClient)
		case *delete:
			deleteSQSMessages(ctx, batch, sqsClient)
		}
	}
}

func startPolling(ctx context.Context, sqsClient *sqs.Client, batches chan []types.Message) {
	var processedCount int
	var receivedCount int
	rateLimit := ratelimit.New(*rateLimit)

	defer func() {
		log.Printf("%d/%d messages were processed in total\n", processedCount, receivedCount)
	}()

	for {
		if processedCount >= *numMessages {
			return
		}

		select {
		case <-ctx.Done():
			return

		default:
			polledBatch, err := pollForMessageBatch(ctx, sqsClient)
			if err != nil {
				log.Printf("Failed to receive message: %v", err)
				return
			}
			receivedCount += len(polledBatch)

			if polledBatch == nil {
				continue
			}

			filteredBatch := filterBatch(polledBatch)

			if len(filteredBatch) == 0 {
				log.Printf("0/%d messages to process, skipping\n", len(polledBatch))
				continue
			}

			processedCount += len(filteredBatch)

			rateLimit.Take()

			batches <- filteredBatch
		}
	}
}

func filterBatch(polledBatch []types.Message) []types.Message {
	filteredBatch := make([]types.Message, 0, maxSQSBatchSize)
	for _, msg := range polledBatch {
		if shouldProcessMessage(msg) {
			filteredBatch = append(filteredBatch, msg)
		}
	}
	return filteredBatch
}

func printSQSMessages(batch []types.Message) {
	for _, msg := range batch {
		log.Printf("SentTimestamp:%v\n Attributes: %v\n MessageAttributes: %v\n Body: %v\n", getTimestamp(msg), toJSONMap(msg.Attributes), toJSONMap(msg.MessageAttributes), *msg.Body)
	}
}

func deleteSQSMessages(ctx context.Context, batch []types.Message, sqsClient *sqs.Client) {
	sqsBatch := make([]types.DeleteMessageBatchRequestEntry, 0, maxSQSBatchSize)
	for _, msg := range batch {
		sqsBatch = append(sqsBatch, types.DeleteMessageBatchRequestEntry{Id: msg.MessageId, ReceiptHandle: msg.ReceiptHandle})
	}

	result, err := sqsClient.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(*sourceQueue),
		Entries:  sqsBatch,
	})

	if err != nil {
		log.Printf("Error deleting message: %v", err)
		return
	}

	log.Printf("Deleted %d Failed %d", len(result.Successful), len(result.Failed))
}

func redriveSQSMessages(ctx context.Context, batch []types.Message, sqsClient *sqs.Client) {
	sqsBatch := make([]types.SendMessageBatchRequestEntry, 0, maxSQSBatchSize)
	for _, msg := range batch {
		sqsBatch = append(sqsBatch, types.SendMessageBatchRequestEntry{
			Id:                aws.String(uuid.New().String()),
			MessageAttributes: msg.MessageAttributes,
			MessageBody:       msg.Body,
		})
	}

	result, err := sqsClient.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(*targetQueue),
		Entries:  sqsBatch,
	})

	if err != nil {
		log.Printf("Error redriving message: %v", err)
	}

	log.Printf("Sent %d Failed %d", len(result.Successful), len(result.Failed))
}

func getTimestamp(msg types.Message) time.Time {
	if msgTimestamp, ok := msg.Attributes["SentTimestamp"]; ok {
		parsedTimestamp, err := strconv.ParseInt(msgTimestamp, 10, 64)
		if err != nil {
			log.Printf("Error parsing message timestamp: %v", err)
		}

		return time.Unix(parsedTimestamp/1000, 0)
	}

	return time.Time{}
}

func shouldProcessMessage(msg types.Message) bool {
	timestamp := getTimestamp(msg)

	if !before.IsZero() && timestamp.Unix() >= before.Unix() {
		return false
	}

	if !after.IsZero() && timestamp.Unix() <= after.Unix() {
		return false
	}

	if attributesQuery != nil && !matchesJqQuery(toJSONMap(msg.Attributes), attributesQuery) {
		return false
	}

	if messageAttributesQuery != nil && !matchesJqQuery(toJSONMap(msg.MessageAttributes), messageAttributesQuery) {
		return false
	}

	if bodyQuery != nil && !matchesJqQuery(toJSONMap(msg.Body), bodyQuery) {
		return false
	}

	return true
}

func matchesJqQuery(input map[string]any, query *gojq.Code) bool {
	if query == nil {
		return false
	}

	iter := query.Run(input)

	if result, ok := iter.Next(); ok {
		if _, ok := result.(error); ok {
			return false
		}

		if result, ok := result.(bool); ok {
			return result
		}
	}

	return false
}

func toJSONMap(input any) (jsonMap map[string]any) {
	if input == nil {
		return nil
	}

	var bytes []byte
	var err error

	switch input := input.(type) {
	case *string:
		bytes = []byte(*input)
	case string:
		bytes = []byte(input)
	case json.RawMessage:
		bytes = []byte(input)
	default:
		bytes, err = json.Marshal(input)
		if err != nil {
			log.Printf("Could not marshal: %v %v", input, err)
		}
	}

	if err = json.Unmarshal(bytes, &jsonMap); err != nil {
		log.Printf("Could not convert to JSON map: %v %v", string(bytes), err)
	}

	return
}

func pollForMessageBatch(ctx context.Context, sqsClient *sqs.Client) ([]types.Message, error) {
	result, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(*sourceQueue),
		MaxNumberOfMessages:   maxSQSBatchSize,
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
		MessageAttributeNames: []string{string(types.QueueAttributeNameAll)},
	})

	if err != nil {
		return nil, err
	}

	if len(result.Messages) == 0 {
		return nil, nil
	}

	return result.Messages, nil
}
