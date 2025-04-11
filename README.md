# priority-workers
Process Go channels by frequency ratio to multiple levels of hierarchy using goroutines

This is a companion project to https://github.com/dmgrit/priority-channels

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