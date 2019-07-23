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
	"time"
)

type Sqs interface {
	Ping() error
	Wait(numRetries int, minBackOff int, maxBackOff int) (Sqs, error)
	SendMessage(message string, queue string) error
	PollQueue(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int) error
	PollQueueWithRetry(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int, numRetries int, minBackOff int, maxBackOff int) error
	CreateQueue(queue string, retentionPeriod int, visibilityTimeout int) (string, error)
	DeleteQueue(queue string) error
	ListQueues() ([]string, error)
}

var _ Sqs = &Service{}

func SetEndPoint(endpoint string, region string) Sqs {
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

func (mq *Service) PollQueue(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int) error {
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
						log.Println("Error deleting message: " + *message.ReceiptHandle + "from queue: " + queue)
						// Todo What to do if we fail to ack a message?
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

// Retention period  - time the messages will remain in the queue if not deleted
// VisibilityTimeout - time the message will be hidden from other consumers after it is retried from the queue. If this time
//                     expires it will be assumed that the message was not processed successfully and will be available to other consumers for retry.
func (mq *Service) CreateQueue(queue string, retentionPeriod int, visibilityTimeout int) (string, error) {
	if retentionPeriod == 0 {
		retentionPeriod = 345600 // 4 days
	}
	
	if visibilityTimeout == 0 {
		visibilityTimeout = 30 // 30 seconds
	}

	result, err := mq.service.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queue),
		Attributes: map[string]*string{
			"MessageRetentionPeriod": aws.String(strconv.Itoa(retentionPeriod)),
			"VisibilityTimeout" : aws.String(strconv.Itoa(visibilityTimeout)),
		},
	})
	if err != nil {
		return "", err
	}

	return *result.QueueUrl, nil
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
	_, err := mq.service.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(int64(0)),
		MessageBody:  aws.String(message),
		QueueUrl:     &queueUrl,
	})

	if err != nil {
		return errors.Wrap(err, "Unable to send message to queue: "+queueUrl)
	}

	return nil
}

func (mq *Service) receiveMessages(queueUrl string, timeBetweenPolls int, maxNumberOfMessagesPerPoll int) ([]*sqs.Message, error) {
	result, err := mq.service.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameCreatedTimestamp),
		},
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: aws.Int64(int64(maxNumberOfMessagesPerPoll)),
		WaitTimeSeconds:     aws.Int64(int64(timeBetweenPolls)),
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
