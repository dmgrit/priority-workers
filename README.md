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

urgentMessagesPriorityChannel, urgentMessagesShutdownFunc, err := priority_workers.ProcessChannelEx(ctx,
    "Urgent Messages", urgentMessagesC)
if err != nil {
    // handle error
}

payingCustomerPriorityChannel, payingCustomerShutdownFunc, err := priority_workers.ProcessByFrequencyRatioEx(ctx, []channels.ChannelWithFreqRatio[string]{
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

freeUserPriorityChannel, freeUserShutdownFunc, err := priority_workers.ProcessByFrequencyRatioEx(ctx, []channels.ChannelWithFreqRatio[string]{
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

combinedUsersPriorityChannel, combinedUsersShutdownFunc, err := priority_workers.CombineByFrequencyRatioEx(ctx, []priority_workers.ResultChannelWithFreqRatioEx[string]{
    priority_workers.NewResultChannelWithFreqRatioEx(
        "Paying Customer",
        payingCustomerPriorityChannel,
        payingCustomerShutdownFunc,
        5),
    priority_workers.NewResultChannelWithFreqRatioEx(
        "Free User",
        freeUserPriorityChannel,
        freeUserShutdownFunc,
        1),
})
if err != nil {
    // handle error
}

ch, shutdownFunc, err := priority_workers.CombineByHighestAlwaysFirstEx(ctx, []priority_workers.ResultChannelWithPriorityEx[string]{
    priority_workers.NewResultChannelWithPriorityEx(
        "Urgent Messages",
        urgentMessagesPriorityChannel,
        urgentMessagesShutdownFunc,
        10),
    priority_workers.NewResultChannelWithPriorityEx(
        "Combined Users",
        combinedUsersPriorityChannel,
        combinedUsersShutdownFunc,
        1),
})
if err != nil {
    // handle error
}

for msg := range ch {
    if msg.Status == priority_channels.ReceiveChannelClosed {
        fmt.Printf("Channel %s closed\n", msg.ReceiveDetails.ChannelName)
        continue
    }
    if msg.Status != priority_channels.ReceiveSuccess  {
        break
    }
    fmt.Printf("%s: %s\n", msg.ReceiveDetails.ChannelName, msg.Msg)
}
```