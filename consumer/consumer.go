package consumer

import (
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/inaciogu/go-sqs/consumer/logger"
	"github.com/inaciogu/go-sqs/consumer/message"
)

type SQSService interface {
	GetQueueUrl(ctx context.Context, input *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	ReceiveMessage(ctx context.Context, input *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, input *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	DeleteMessage(ctx context.Context, input *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ListQueues(ctx context.Context, input *sqs.ListQueuesInput, opts ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error)
}

type Logger interface {
	Log(message string, v ...interface{})
}

type SQSClientInterface interface {
	GetQueueUrl(context.Context) *string
	ReceiveMessages(ctx context.Context, queueUrl string, ch chan types.Message) error
	ProcessMessage(ctx context.Context, message types.Message, queueUrl string)
	Poll(context.Context)
	GetQueues(ctx context.Context, prefix string) []string
	Start(context.Context)
}

type SQSClientOptions struct {
	QueueName string
	// Handle is the function that will be called when a message is received.
	// Return true if you want to delete the message from the queue, otherwise, return false
	Handle   func(message *message.Message) bool
	Region   string
	Endpoint string
	// PrefixBased is a flag that indicates if the queue name is a prefix
	PrefixBased         bool
	MaxNumberOfMessages int32
	VisibilityTimeout   int32
	WaitTimeSeconds     int32
	LogLevel            string
	// BackoffMultiplier is the multiplier used to calculate the backoff time (visibility timeout)
	BackoffMultiplier float64
}

var (
	_ SQSClientInterface = &SQSClient{}
	_ SQSService         = &sqs.Client{}
)

type SQSClient struct {
	Client        SQSService
	ClientOptions *SQSClientOptions
	Logger        Logger
}

const (
	DefaultMaxNumberOfMessages = 10
	DefaultVisibilityTimeout   = 30
	DefaultWaitTimeSeconds     = 20
	DefaultRegion              = "us-east-1"
)

func New(sqsService SQSService, options SQSClientOptions) *SQSClient {
	if options.QueueName == "" {
		panic("QueueName is required")
	}

	if sqsService == nil {

		sdkConfig, err := config.LoadDefaultConfig(context.Background(),
			config.WithRegion(options.Region),
			config.WithSharedCredentialsFiles([]string{config.DefaultSharedCredentialsFilename()}),
		)
		if err != nil {
			panic(err)
		}

		sqsService = sqs.NewFromConfig(sdkConfig)
	}

	setDefaultOptions(&options)

	logger := logger.New(logger.DefaultLoggerConfig{LogLevel: options.LogLevel})
	return &SQSClient{
		Client:        sqsService,
		ClientOptions: &options,
		Logger:        logger,
	}
}

func setDefaultOptions(options *SQSClientOptions) {
	if options.MaxNumberOfMessages == 0 {
		options.MaxNumberOfMessages = DefaultMaxNumberOfMessages
	}

	if options.VisibilityTimeout == 0 {
		options.VisibilityTimeout = DefaultVisibilityTimeout
	}

	if options.WaitTimeSeconds == 0 {
		options.WaitTimeSeconds = DefaultWaitTimeSeconds
	}

	if options.Region == "" {
		options.Region = DefaultRegion
	}

	if options.LogLevel == "" {
		options.LogLevel = "info"
	}

	if options.BackoffMultiplier == 0 {
		options.BackoffMultiplier = 2
	}
}

func (s *SQSClient) SetLogger(logger Logger) {
	s.Logger = logger
}

// GetQueueUrl returns the URL of the queue based on the queue name
func (s *SQSClient) GetQueueUrl(ctx context.Context) *string {
	urlResult, err := s.Client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(s.ClientOptions.QueueName),
	})
	if err != nil {
		panic(err)
	}

	return urlResult.QueueUrl
}

// GetQueues returns a list of queues based on the prefix
func (s *SQSClient) GetQueues(ctx context.Context, prefix string) []string {
	input := &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String(prefix),
	}

	result, err := s.Client.ListQueues(ctx, input)
	if err != nil {
		panic(err)
	}

	return result.QueueUrls
}

// ReceiveMessages polls messages from the queue
func (s *SQSClient) ReceiveMessages(ctx context.Context, queueUrl string, ch chan types.Message) error {
	splittedUrl := strings.Split(queueUrl, "/")

	queueName := splittedUrl[len(splittedUrl)-1]

	for {
		s.Logger.Log("polling messages from queue %s", queueName)

		result, err := s.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueUrl),
			MaxNumberOfMessages: s.ClientOptions.MaxNumberOfMessages,
			WaitTimeSeconds:     s.ClientOptions.WaitTimeSeconds,
			VisibilityTimeout:   s.ClientOptions.VisibilityTimeout,
			AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
		})
		if err != nil {
			panic(err)
		}

		s.Logger.Log("received %d messages from queue %s", len(result.Messages), queueName)

		for _, message := range result.Messages {
			ch <- message
		}
	}
}

// calculateBackoff calculates the backoff (visibility timeout) time based on the number of attempts to process the message
func (s *SQSClient) calculateBackoff(attempts int) float64 {
	return math.Pow(s.ClientOptions.BackoffMultiplier, float64(attempts))
}

// ProcessMessage deletes or changes the visibility of the message based on the Handle function return.
func (s *SQSClient) ProcessMessage(ctx context.Context, sqsMessage types.Message, queueUrl string) {
	message := message.New(sqsMessage)

	handled := s.ClientOptions.Handle(message)

	if !handled {
		attempts, _ := strconv.Atoi(message.Metadata.MessageAttributes["ApproximateReceiveCount"])

		backoff := s.calculateBackoff(attempts)

		_, err := s.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(queueUrl),
			ReceiptHandle:     &message.Metadata.ReceiptHandle,
			VisibilityTimeout: int32(backoff),
		})
		if err != nil {
			panic(err)
		}

		s.Logger.Log("failed to handle message with ID: %s", message.Metadata.MessageId)

		return
	}

	_, err := s.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueUrl),
		ReceiptHandle: &message.Metadata.ReceiptHandle,
	})
	if err != nil {
		panic(err)
	}

	s.Logger.Log("message handled ID: %s", message.Metadata.MessageId)
}

// Poll starts polling messages from the queue
func (s *SQSClient) Poll(ctx context.Context) {
	if s.ClientOptions.PrefixBased {
		queues := s.GetQueues(ctx, s.ClientOptions.QueueName)

		for _, queue := range queues {
			ch := make(chan types.Message)

			go s.ReceiveMessages(ctx, queue, ch)

			go func(queueUrl string) {
				for message := range ch {
					go s.ProcessMessage(ctx, message, queueUrl)
				}
			}(queue)
		}

		select {}
	}

	ch := make(chan types.Message)

	queueUrl := s.GetQueueUrl(ctx)

	go s.ReceiveMessages(ctx, *queueUrl, ch)

	for message := range ch {
		go s.ProcessMessage(ctx, message, *queueUrl)
	}
}

func (s *SQSClient) Start(ctx context.Context) {
	s.Poll(ctx)
}
