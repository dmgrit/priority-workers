# priority-workers
Process Go channels by frequency ratio to multiple levels of hierarchy using goroutines

This project is companion to https://github.com/dmgrit/priority-channels.  
The two projects differ mainly in **how** they process channels:
- `priority-channels` focuses on **synchronous** processing.
  It preserves the atomic semantics of Go’s `select` statement by collapsing the entire channel hierarchy into a single `select`.  
  This ensures that each message is either **fully processed or not processed at all** - no partial work happens.
- `priority-workers` (this package) takes an **asynchronous** approach.
   It uses **goroutines** to process channels concurrently, allowing for messages to be in an intermediate state - 
already read from one channel, but still waiting to be processed or forwarded to the next.

## Concepts

This package allows you to process channels based on a **frequency ratio**, either from: 
- A flat list of channels 
- A hierarchy of channels

### How It Works

Processing is handled internally by starting a number of goroutines proportional to the given frequency ratio.

**Example**:   
If you have three channels with a frequency ratio of `1:2:3`, the package will create **6 goroutines** (= 1+2+3) to process the channels:  
- 1 goroutine will read from the first channel
- 2 goroutines will read from the second channel
- 3 goroutines will read from the third channel  

These goroutines will: 
- Read from the channels 
- Either invoke a **callback function** or 
- Forward the received message to the **next channel in the hierarchy**

Processing continues until one of the following conditions is met:
- The provided context is canceled 
- The processing channel is shut down
- All input channels are closed
  
### Channel Type
Channels used in a hierarchy must be of type `priority_workers.Channel`.  
It is defined as follows:  
```go
type Channel[T any] struct {
    Output   <-chan Delivery[T]
    Shutdown ShutdownFunc[T]
}
```
Every channel has a `Shutdown(mode ShutdownMode)` function that:
- Stop all goroutines in its subtree
- Sends a `ReceiveContextCanceled` message to the output channel
- Closes the channel  

Shutdown can be triggered either indirectly, by canceling the context, or directly by calling the `Shutdown` function.  

### Shutdown Modes
You can shut down channels in one of two modes: 
- `Graceful`:   
   Waits for all goroutines to finish processing messages before closing the channel.
- `Force`:   
   Immediately stops all goroutines and closes the channel, dropping any unprocessed messages without forwarding them to the next channel.  
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