package gosqs

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"time"
)

type Sqs interface {
	Ping() error
	Wait(numRetries int, minBackOff int, maxBackOff int) (Sqs, error)
	SendMessage(message string, queue string) error
	PollQueue(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int) error
	PollQueueWithRetry(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int, numRetries int, minBackOff int, maxBackOff int) error
	CreateQueue(queue string, retentionPeriod int, visibilityTimeout int, fifo bool, encrypted bool) (string, error)
	CreateDefaultQueue(queue string) (string, error)
	CreateDefaultFifoQueue(queue string) (string, error)
	DeleteQueue(queue string) error
	ListQueues() ([]string, error)
}

var _ Sqs = &Service{}

func SetEndPoint(endpoint string, region string) Sqs {
	if endpoint == "default" {
		endpoint = ""
	}

	// Errors only possible when using custom CA bundles.
	sess, _ := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:   aws.String(region),
			Endpoint: aws.String(endpoint),
		},
	})

	var sqsService Sqs = &Service{
		service: sqs.New(sess),
	}

	return sqsService
}

type Service struct {
	service *sqs.SQS
}

func (mq *Service) Ping() error {
	// Call list queues to test the service configuration
	_, err := mq.ListQueues()
	return err
}

func (mq *Service) Wait(numRetries int, minBackOff int, maxBackOff int) (Sqs, error) {
	for i := 1; i <= numRetries; i++ {
		err := mq.Ping()
		if err == nil {
			return mq, nil
		}
		sleepTime := time.Duration(rand.Intn(maxBackOff-minBackOff) + minBackOff)
		log.Print("Error connecting to SQS, retrying in: " + fmt.Sprintf("%d", sleepTime) + " seconds. Error: " + err.Error())
		time.Sleep(sleepTime * time.Second)
	}
	return mq, errors.New("connecting to SQS timed out")
}

func (mq *Service) SendMessage(message string, queue string) error {
	queueUrl, err := mq.getQueueUrl(queue)
	if err != nil {
		return err
	}

	return mq.sendMessage(message, queueUrl)
}

// Polls the queue for new messages
//
// - queue: the name of the queue to poll
// - callback: the function that will be called to process each message. If the
//   function returns true the message will be deleted from the queue
// - pollWaitTime: the amount of time in seconds sqs will wait for a message on a queue. As soon as a message is received
// 	 the function will return (regardless of the wait time) and a new request started.
// - maxNumberOfMessagesPerPoll: the max number of messages (between 1 and 10) that can be returned from a single poll.
func (mq *Service) PollQueue(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int) error {
	if maxNumberOfMessagesPerPoll > 10 || maxNumberOfMessagesPerPoll < 1 {
		return errors.New("max number of messages per poll must be between 1 and 10")
	}

	queueUrl, err := mq.getQueueUrl(queue)
	if err != nil {
		return err
	}

	go func() {
		for {
			messages, err := mq.receiveMessages(queueUrl, pollWaitTime, maxNumberOfMessagesPerPoll)
			if err != nil {
				log.Println(err)
			}

			for _, message := range messages {
				processedSuccessfully := callback(*message.Body)
				if processedSuccessfully {
					err = mq.deleteMessage(queueUrl, message.ReceiptHandle)
					if err != nil {
						log.Println("Error deleting message: `" + *message.ReceiptHandle + "` from queue: `" + queue + "` Error: " + err.Error())
					}
				}
			}
		}
	}()

	return nil
}

func (mq *Service) PollQueueWithRetry(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int, numRetries int, minBackOff int, maxBackOff int) error {
	err := mq.waitForQueue(queue, numRetries, minBackOff, maxBackOff)
	if err != nil {
		return err
	}

	return mq.PollQueue(queue, callback, pollWaitTime, maxNumberOfMessagesPerPoll)
}

// - Retention period:  time the messages will remain in the queue if not deleted
// - VisibilityTimeout: time the message will be hidden from other consumers after it is retried from the queue. If this time
//                      expires it will be assumed that the message was not processed successfully and will be available to other consumers for retry.
// - Fifo: 				whether the queue should be first in first out with deliver once guarantee
// - encrypted:			whether the queue should use server side encryption using the default kms
func (mq *Service) CreateQueue(queue string, retentionPeriod int, visibilityTimeout int, fifo bool, encrypted bool) (string, error) {
	attributes := make(map[string]*string)

	attributes["MessageRetentionPeriod"] = aws.String(strconv.Itoa(retentionPeriod))
	attributes["VisibilityTimeout"] = aws.String(strconv.Itoa(visibilityTimeout))

	if encrypted {
		attributes["KmsMasterKeyId"] = aws.String("alias/aws/sqs")
		attributes["KmsDataKeyReusePeriodSeconds"] = aws.String("300")
	}

	if fifo {
		if !strings.HasSuffix(queue, ".fifo") {
			return "", errors.New("fifo queue names must end in `.fifo`")
		}
		attributes["FifoQueue"] = aws.String("true")
		attributes["ContentBasedDeduplication"] = aws.String("true")
	}

	result, err := mq.service.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  aws.String(queue),
		Attributes: attributes,
	})
	if err != nil {
		return "", err
	}

	return *result.QueueUrl, nil
}

func (mq *Service) CreateDefaultQueue(queue string) (string, error) {
	return mq.CreateQueue(queue, 345600, 30, false, true)
}

func (mq *Service) CreateDefaultFifoQueue(queue string) (string, error) {
	return mq.CreateQueue(queue, 345600, 30, true, true)
}

func (mq *Service) DeleteQueue(queue string) error {
	queueUrl, err := mq.getQueueUrl(queue)
	if err != nil {
		return err
	}

	_, err = mq.service.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueUrl),
	})

	return err
}

func (mq *Service) ListQueues() ([]string, error) {
	queueData, err := mq.service.ListQueues(nil)
	if err != nil {
		return nil, err
	}

	var queueUrls []string
	for _, queueUrl := range queueData.QueueUrls {
		queueUrls = append(queueUrls, path.Base(*queueUrl))
	}

	return queueUrls, nil
}

func (mq *Service) waitForQueue(queue string, numRetries int, minBackOff int, maxBackOff int) error {
	for i := 1; i <= numRetries; i++ {
		_, err := mq.getQueueUrl(queue)
		if err == nil {
			return nil
		}
		sleepTime := time.Duration(rand.Intn(maxBackOff-minBackOff) + minBackOff)
		log.Print("Error connecting to queue `" + queue + "` retrying in: " + fmt.Sprintf("%d", sleepTime) + " seconds. Error message: " + err.Error())
		time.Sleep(sleepTime * time.Second)
	}
	return errors.New("connecting to SQS queue `" + queue + "` timed out")
}

func (mq *Service) sendMessage(message string, queueUrl string) error {
	var messageGroupId *string
	messageDelay := aws.Int64(0)
	if strings.HasSuffix(queueUrl, ".fifo") {
		messageGroupId = aws.String(queueUrl)
		messageDelay = nil
	}

	_, err := mq.service.SendMessage(&sqs.SendMessageInput{
		MessageGroupId: messageGroupId,
		DelaySeconds:   messageDelay,
		MessageBody:    aws.String(message),
		QueueUrl:       &queueUrl,
	})

	if err != nil {
		return errors.Wrap(err, "Unable to send message to queue: "+queueUrl)
	}

	return nil
}

func (mq *Service) receiveMessages(queueUrl string, pollWaitTime int, maxNumberOfMessagesPerPoll int) ([]*sqs.Message, error) {
	result, err := mq.service.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameCreatedTimestamp),
		},
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: aws.Int64(int64(maxNumberOfMessagesPerPoll)),
		WaitTimeSeconds:     aws.Int64(int64(pollWaitTime)),
	})
	if err != nil {
		return nil, errors.Wrap(err, "Unable to receive message from queue: "+queueUrl)
	}

	return result.Messages, nil
}

func (mq *Service) deleteMessage(queue string, receiptHandle *string) error {
	_, err := mq.service.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queue,
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		return errors.Wrap(err, "Unable to delete message:"+*receiptHandle+" from queue "+queue)
	}

	return nil
}

func (mq *Service) getQueueUrl(queue string) (string, error) {
	resultURL, err := mq.service.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queue),
	})
	if err != nil {
		awsErr, ok := err.(awserr.Error)
		if ok && awsErr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return "", errors.Wrap(err, "unable to find queue: "+queue)
		}
		return "", errors.Wrap(err, "unable to get queue URL: "+queue)
	}

	return *resultURL.QueueUrl, nil
}
