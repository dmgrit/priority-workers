# priority-workers
Process Go channels by frequency ratio to multiple levels of hierarchy using goroutines

This is a companion project to https://github.com/dmgrit/priority-channels

## Concepts

This package allows you to process channels by frequency ratio, either from a flat list of channels or from a hierarchy of channels.  
Processing is handled internally by initializing a number of goroutines proportional to the given frequency ratio.   
For example, if you have three channels with a frequency ratio of 1:2:3, the package will create six goroutines (= 1+2+3) to process the channels.  
One goroutine will read from the first channel, two will read from the second, and three will read from the third.  
These goroutines will read from the channels and either invoke a callback function or forward the received message to the next channel in the hierarchy.
Processing continues until the provided context is canceled, the processing channel is shut down, or all channels are closed.
  
Channels used in a hierarchy must be of type `priority_workers.Channel`.  
It is defined as follows:  
```go
type Channel[T any] struct {
    Output   <-chan Delivery[T]
    Shutdown ShutdownFunc[T]
}
```
Every channel has a `Shutdown(mode ShutdownMode)` function, which can be used to stop all goroutines in its subtree and close the channel.  
Shutdown can be triggered either indirectly, by canceling the context, or directly by calling the `Shutdown` function.  

Shutdown can be called in either `Graceful` or `Force` mode: 
- `Graceful` shutdown waits for all goroutines to finish processing messages before closing the channel.  
- `Force` shutdown immediately stops all goroutines and closes the channel, dropping any unprocessed messages without forwarding them to the next channel.  
Optionally, an `OnMessageDrop` callback can be provided to handle messages that are dropped during a `Force` shutdown.  

## Installation

```shell
go get github.com/dmgrit/priority-workers
```

## Usage

In the following scenario, we have the following hierarchy of channels:
- Urgent messages are always processed first.
- Two groups of channels: paying customers and free users.
- Paying customers are processed 5 times for every 1 time free users are processed.
- Within each group, high priority messages are processed 3 times for every 1 time low priority messages are processed.

For a full demonstration, run the [corresponding example](examples/multi-hierarchy/main.go).

```go
urgentMessagesC := make(chan string)
payingCustomerHighPriorityC := make(chan string)
payingCustomerLowPriorityC := make(chan string)
freeUserHighPriorityC := make(chan string)
freeUserLowPriorityC := make(chan string)

urgentMessagesChannel, err := priority_workers.ProcessChannel(ctx,
    "Urgent Messages", urgentMessagesC)
if err != nil {
    // handle error
}

payingCustomerChannel, err := priority_workers.ProcessByFrequencyRatio(ctx, []channels.ChannelWithFreqRatio[string]{
    channels.NewChannelWithFreqRatio(
        "Paying Customer - High Priority",
        payingCustomerHighPriorityC,
        3),
    channels.NewChannelWithFreqRatio(
        "Paying Customer - Low Priority",
        payingCustomerLowPriorityC,
        1),
})
if err != nil {
    // handle error
}

freeUserChannel, err := priority_workers.ProcessByFrequencyRatio(ctx, []channels.ChannelWithFreqRatio[string]{
    channels.NewChannelWithFreqRatio(
        "Free User - High Priority",
        freeUserHighPriorityC,
        3),
    channels.NewChannelWithFreqRatio(
        "Free User - Low Priority",
        freeUserLowPriorityC,
        1),
})
if err != nil {
    // handle error
}

combinedUsersChannel, err := priority_workers.CombineByFrequencyRatio(ctx, []priority_workers.ChannelWithFreqRatio[string]{
    priority_workers.NewChannelWithFreqRatio(
        "Paying Customer",
        payingCustomerChannel,
        5),
    priority_workers.NewChannelWithFreqRatio(
        "Free User",
        freeUserChannel,
        1),
})
if err != nil {
    // handle error
}

ch, err := priority_workers.CombineByHighestAlwaysFirst(ctx, []priority_workers.ChannelWithPriority[string]{
    priority_workers.NewChannelWithPriority(
        "Urgent Messages",
        urgentMessagesChannel,
        10),
    priority_workers.NewChannelWithPriority(
        "Combined Users",
        combinedUsersChannel,
        1),
})
if err != nil {
    // handle error
}

for msg := range ch.Output {
    if msg.Status == priority_channels.ReceiveChannelClosed {
        fmt.Printf("Channel %s closed\n", msg.ChannelName())
        continue
    }
    if msg.Status != priority_channels.ReceiveSuccess  {
        break
    }
    fmt.Printf("%s: %s\n", msg.ChannelName(), msg.Msg)
}
```