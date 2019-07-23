package gosqs

import "github.com/pkg/errors"

type Mock struct {
	queues map[string]*MockQueue
}

type MockQueue struct {
	Queue    string
	QueueUrl string
	Messages []string
}

func (Mock) Ping() error {
	return nil
}

func (sqs *Mock) Wait(numRetries int, minBackOff int, maxBackOff int) (Sqs, error) {
	return sqs, nil
}

func (sqs *Mock) SendMessage(message string, queue string) error {
	if len(sqs.queues[queue].Messages) == 0 {
		sqs.queues[queue].Messages = make([]string, 0)
	}
	sqs.queues[queue].Messages = append(sqs.queues[queue].Messages, message)
	return nil
}

func (sqs *Mock) PollQueue(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int) error {
	messages := sqs.queues[queue].Messages
	for _, message := range messages {
		_ = callback(message)
	}
	return nil
}

func (sqs *Mock) PollQueueWithRetry(queue string, callback func(string) bool, pollWaitTime int, maxNumberOfMessagesPerPoll int, numRetries int, minBackOff int, maxBackOff int) error {
	messages := sqs.queues[queue].Messages
	for _, message := range messages {
		_ = callback(message)
	}
	return nil
}

func (sqs *Mock) CreateQueue(queue string, retentionPeriod int, visibilityTimeout int) (string, error) {
	queueUrl := "http://localhost/queues/" + queue

	if len(sqs.queues) == 0 {
		sqs.queues = make(map[string]*MockQueue)
	}

	_, exists := sqs.queues[queue]
	if exists {
		return queueUrl, nil
	}

	newQueue := MockQueue{
		Queue:    queue,
		QueueUrl: queueUrl,
	}

	sqs.queues[queue] = &newQueue

	return queueUrl, nil
}

func (sqs *Mock) DeleteQueue(queue string) error {
	_, exists := sqs.queues[queue]
	if exists {
		delete(sqs.queues, queue)
	} else {
		return errors.New("queue does not exist")
	}

	return nil
}

func (sqs *Mock) ListQueues() ([]string, error) {
	queues := sqs.queues
	keys := make([]string, 0, len(queues))
	for key := range queues {
		keys = append(keys, key)
	}
	return keys, nil
}

func (sqs *Mock) GetQueue(queue string) *MockQueue {
	return sqs.queues[queue]
}
