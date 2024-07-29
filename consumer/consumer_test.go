package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/inaciogu/go-sqs/consumer"

	"github.com/inaciogu/go-sqs/consumer/message"
	"github.com/inaciogu/go-sqs/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Log(message string, v ...interface{}) {
	m.Called(message, v)
}

type UnitTest struct {
	suite.Suite
	mockSQSService *mocks.SQSService
}

func (u *UnitTest) SetupTest() {
	u.mockSQSService = new(mocks.SQSService)
}

func TestUnitSuites(t *testing.T) {
	suite.Run(t, &UnitTest{})
}

func (ut *UnitTest) TestNewWithoutSQSService() {
	client := consumer.New(nil, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
	})

	assert.NotNil(ut.T(), client)
}

func (ut *UnitTest) TestNewWithSQSService() {
	client := consumer.New(ut.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
	})

	assert.NotNil(ut.T(), client)
}

func (ut *UnitTest) TestNewWithoutQueueName() {
	assert.Panics(ut.T(), func() {
		consumer.New(nil, consumer.SQSClientOptions{})
	})
}

func (ut *UnitTest) TestSetLogger() {
	logger := new(MockLogger)

	client := consumer.New(nil, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
	})

	client.SetLogger(logger)

	assert.NotNil(ut.T(), client)
}

func (ut *UnitTest) TestGetQueueUrl() {
	expectedOutput := &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}
	ut.mockSQSService.On("GetQueueUrl", mock.Anything).Return(expectedOutput, nil)

	client := consumer.New(ut.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
	})

	queueURL := client.GetQueueUrl(context.TODO())

	assert.Equal(ut.T(), "https://fake-queue-url", *queueURL)

	ut.mockSQSService.AssertCalled(ut.T(), "GetQueueUrl", &sqs.GetQueueUrlInput{
		QueueName: aws.String("fake-queue-name"),
	})
}

func (ut *UnitTest) TestQueueUrl_Error() {
	ut.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{}, errors.New("erro"))

	client := consumer.New(ut.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
	})

	assert.Panics(ut.T(), func() {
		client.GetQueueUrl(context.TODO())
	})
}

func (ut *UnitTest) TestReceiveMessage() {
	ut.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	client := consumer.New(ut.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	expectedOutput := &sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				Body:          aws.String(`{"content": "fake-content"}`),
				ReceiptHandle: aws.String("fake-receipt-handle"),
				MessageId:     aws.String("fake-message-id"),
			},
		},
	}

	ut.mockSQSService.On("ReceiveMessage", mock.Anything).Return(expectedOutput, nil)

	ut.mockSQSService.On("DeleteMessage", mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)

	ch := make(chan types.Message, 1)

	go client.ReceiveMessages(context.TODO(), "https://fake-queue-url", ch)

	time.Sleep(600 * time.Millisecond)

	fmt.Println(len(ch))

	ut.mockSQSService.AssertCalled(ut.T(), "ReceiveMessage", &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String("https://fake-queue-url"),
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     20,
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	ut.Assert().Equal(1, len(ch))
}

func (ut *UnitTest) TestReceiveMessage_Error() {
	ut.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	ut.mockSQSService.On("ReceiveMessage", mock.Anything).Return(&sqs.ReceiveMessageOutput{}, errors.New("erro"))

	client := consumer.New(ut.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	ch := make(chan types.Message)

	assert.Panics(ut.T(), func() {
		client.ReceiveMessages(context.TODO(), "https://fake-queue-url", ch)
	})
}

func (uts *UnitTest) TestProcessMessage_Handled_Error() {
	uts.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	message := types.Message{
		Body:          aws.String(`{"content": "fake-content"}`),
		ReceiptHandle: aws.String("fake-receipt-handle"),
		MessageId:     aws.String("fake-message-id"),
	}

	uts.mockSQSService.On("DeleteMessage", mock.Anything).Return(&sqs.DeleteMessageOutput{}, errors.New("Error"))

	assert.Panics(uts.T(), func() {
		client.ProcessMessage(context.TODO(), message, "https://fake-queue-url")
	})
}

func (uts *UnitTest) TestProcessMessage_Handled() {
	uts.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	message := types.Message{
		Body: aws.String(`{
			"Message": "{\n  \"asda\": \"asdas\"\n}",
			"MessageId": "fake-message-id",
			"ReceiptHandle": "fake-receipt-handle",
			"MessageAttributes": {
				"attribute1": {
					"Type": "String",
					"Value": "value1"
				}
			}
		}`),
		ReceiptHandle: aws.String("fake-receipt-handle"),
		MessageId:     aws.String("fake-message-id"),
	}

	uts.mockSQSService.On("DeleteMessage", mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)

	client.ProcessMessage(context.TODO(), message, "https://fake-queue-url")

	uts.mockSQSService.AssertCalled(uts.T(), "DeleteMessage", &sqs.DeleteMessageInput{
		QueueUrl:      aws.String("https://fake-queue-url"),
		ReceiptHandle: aws.String("fake-receipt-handle"),
	})
}

func (uts *UnitTest) TestProcessMessage_Not_Handled_Error() {
	uts.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return false
		},
	})

	message := types.Message{
		Body:          aws.String(`{"content": "fake-content"}`),
		ReceiptHandle: aws.String("fake-receipt-handle"),
		MessageId:     aws.String("fake-message-id"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"AproximateReceiveCount": {
				DataType:    aws.String("Number"),
				StringValue: aws.String("1"),
			},
		},
	}

	uts.mockSQSService.On("ChangeMessageVisibility", mock.Anything).Return(&sqs.ChangeMessageVisibilityOutput{}, errors.New("Error"))

	assert.Panics(uts.T(), func() {
		client.ProcessMessage(context.TODO(), message, "https://fake-queue-url")
	})
}

func (uts *UnitTest) TestProcessMessage_Not_Handled() {
	uts.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return false
		},
	})

	message := types.Message{
		Body:          aws.String(`{"content": "fake-content"}`),
		ReceiptHandle: aws.String("fake-receipt-handle"),
		MessageId:     aws.String("fake-message-id"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"ApproximateReceiveCount": {
				DataType:    aws.String("String"),
				StringValue: aws.String("2"),
			},
		},
	}

	uts.mockSQSService.On("ChangeMessageVisibility", mock.Anything).Return(&sqs.ChangeMessageVisibilityOutput{}, nil)

	client.ProcessMessage(context.TODO(), message, "https://fake-queue-url")

	uts.mockSQSService.AssertNotCalled(uts.T(), "DeleteMessage", &sqs.DeleteMessageInput{
		QueueUrl:      aws.String("https://fake-queue-url"),
		ReceiptHandle: aws.String("fake-receipt-handle"),
	})
	uts.mockSQSService.AssertCalled(uts.T(), "ChangeMessageVisibility", &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String("https://fake-queue-url"),
		ReceiptHandle:     aws.String("fake-receipt-handle"),
		VisibilityTimeout: 4,
	})
}

func (uts *UnitTest) TestPoll() {
	uts.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	uts.mockSQSService.On("ReceiveMessage", mock.Anything).Return(&sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				Body:          aws.String(`{"content": "fake-content"}`),
				ReceiptHandle: aws.String("fake-receipt-handle"),
				MessageId:     aws.String("fake-message-id"),
			},
		},
	}, nil)

	uts.mockSQSService.On("DeleteMessage", mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)

	go client.Poll(context.TODO())

	time.Sleep(600 * time.Millisecond)

	uts.mockSQSService.AssertCalled(uts.T(), "ReceiveMessage", &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String("https://fake-queue-url"),
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     20,
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	uts.mockSQSService.AssertCalled(uts.T(), "GetQueueUrl", &sqs.GetQueueUrlInput{
		QueueName: aws.String("fake-queue-name"),
	})
}

func (ut *UnitTest) TestGetQueues_Error() {
	client := consumer.New(ut.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	ut.mockSQSService.On("ListQueues", mock.Anything).Return(nil, errors.New("Error"))

	assert.Panics(ut.T(), func() {
		client.GetQueues(context.TODO(), "fake-queue-name")
	})
}

func (uts *UnitTest) TestGetQueues() {
	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	uts.mockSQSService.On("ListQueues", mock.Anything).Return(&sqs.ListQueuesOutput{
		QueueUrls: []string{
			"https://fake-queue-url",
			"https://fake-queue-url-2",
		},
	}, nil)

	queues := client.GetQueues(context.TODO(), "fake-queue-name")

	assert.Equal(uts.T(), 2, len(queues))

	uts.mockSQSService.AssertCalled(uts.T(), "ListQueues", &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String("fake-queue-name"),
	})
}

func (uts *UnitTest) TestPollPrefixBased() {
	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
		PrefixBased: true,
	})

	uts.mockSQSService.On("ListQueues", mock.Anything).Return(&sqs.ListQueuesOutput{
		QueueUrls: []string{
			"https://fake-queue-url",
			"https://fake-queue-url-2",
		},
	}, nil)

	uts.mockSQSService.On("ReceiveMessage", mock.Anything).Return(&sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				Body:          aws.String(`{"content": "fake-content"}`),
				ReceiptHandle: aws.String("fake-receipt-handle"),
				MessageId:     aws.String("fake-message-id"),
			},
		},
	}, nil)

	uts.mockSQSService.On("DeleteMessage", mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)

	go client.Poll(context.TODO())

	time.Sleep(600 * time.Millisecond)

	uts.mockSQSService.AssertCalled(uts.T(), "ListQueues", &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String("fake-queue-name"),
	})
	uts.mockSQSService.AssertNumberOfCalls(uts.T(), "ReceiveMessage", 2)
}

func (uts *UnitTest) TestStart() {
	uts.mockSQSService.On("GetQueueUrl", mock.Anything).Return(&sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("https://fake-queue-url"),
	}, nil)

	client := consumer.New(uts.mockSQSService, consumer.SQSClientOptions{
		QueueName: "fake-queue-name",
		Handle: func(message *message.Message) bool {
			return true
		},
	})

	uts.mockSQSService.On("ReceiveMessage", mock.Anything).Return(&sqs.ReceiveMessageOutput{}, nil)

	uts.mockSQSService.On("DeleteMessage", mock.Anything).Return(&sqs.DeleteMessageOutput{}, nil)

	go client.Start(context.TODO())

	time.Sleep(600 * time.Millisecond)

	uts.mockSQSService.AssertCalled(uts.T(), "ReceiveMessage", &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String("https://fake-queue-url"),
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     20,
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
}
