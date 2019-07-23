# GOSQS

This is a thin wrapper library around the GO AWS SDK's SQS functions. This wrapper seeks to simplify the the usage and unit testing of SQS queues. 

# Usage

### Setting an endpoint
This method will pass the configuration parameters necessary to connect to SQS. Please note that this method does not confirm these settings.

```
endPoint, err := gosqs.SetEndPoint("Endpoint", "Region").Wait(20, 3, 8)
if err != nil {
    return err
}
```

If you would like to attempt to connect to this endpoint and pause execution until it becomes available you can append `.Wait` this will re-try the connection for the selected number of times and jitter the back off time.
```
endPoint, err := gosqs.SetEndPoint("Endpoint", "Region").
    Wait(20, 3, 8) //num retries 20, min backoff 3, max backoff 8
```

### Checking if an endpoint is available
The `Ping` method will return an error if the endpoint is not available. This is an alternative to the `Wait` method
```
err := endpoint.Ping()
```

### Creating a queue
Before we can send messages to a queue we will need to create one
```
queueUrl, err := endPoint.CreateQueue("queue name", 1, 86400) // queue name, default polling delay in seconds, default message retention period in seconds
```

### Delete a queue
```
err := endpoint.DeleteQueue("queue name")
```

### Send message to a queue
```
err := endpoint.SendMessage("message string", "queue name")
```

### Retrieve messages from a queue
Sqs needs to be polled for messages. This is done by setting a callback method which will be triggered each time a message is received. If the method returns true the message will be deleted from the queue. 
```
callback := func(message string) bool {
    log.Println("received message from queue: " + message)
    err := processMessage(message)
    if err != nil {
        log.Println("Failed to process message")
        return false
    }
    return true
}

err := endpoint.PollQueue("queue name", callback, 1) // queue name, callback function, the time between polls in seconds
```

If you want to consume a queue which might not initially be created or on an endpoint which might not be available you can poll with a retry.

```
callback := func(message string) bool {
    log.Println("received message from queue: " + message)
    err := processMessage(message)
    if err != nil {
        log.Println("Failed to process message")
        return false
    }
    return true
}

err := endpoint.PollQueue("queue name", callback, 1, 20, 3, 8) // queue name, callback function, the time between polls in seconds, num retries, min backoof in seconds, max mackoff in seconds
```

If your callback needs to access pre-initialized structs you can use a wrapper function:
```
func createCallback(dependency dependencyType) func(string) bool {
    return func(message string) bool {
        log.Println("received message from queue: " + message)
        err := dependency.processMessage(message)
        if err != nil {
            log.Println("Failed to process message")
            return false
        }
        return true
    }
}

callback := createCallback(dependency)
err := endpoint.PollQueue("queue name", callback, 1)
```
