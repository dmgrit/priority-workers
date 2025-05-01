package priority_workers

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dmgrit/priority-channels"
)

type Consumer[T any] struct {
	ctx                             context.Context
	channel                         Channel[T]
	channelNameToChannel            map[string]<-chan T
	channelNameToReconfigureChannel map[string]chan T
	priorityConfig                  Configuration
	priorityConfigUpdatesC          chan Configuration
	priorityWorkersUpdatesMtx       sync.Mutex
	priorityWorkersClosedC          chan struct{}
	forceShutdownChannel            chan struct{}
	onMessageDrop                   func(msg T, details priority_channels.ReceiveDetails)
	isStopping                      bool
	isStopped                       bool
	exitReason                      priority_channels.ExitReason
	exitReasonChannelName           string
}

func NewConsumer[T any](
	ctx context.Context,
	channelNameToChannel map[string]<-chan T,
	priorityConfiguration Configuration,
) (*Consumer[T], error) {
	channel, err := NewFromConfiguration(ctx, priorityConfiguration, channelNameToChannel)
	if err != nil {
		return nil, fmt.Errorf("failed to create priority channel from configuration: %w", err)
	}
	return &Consumer[T]{
		ctx:                    ctx,
		priorityConfig:         priorityConfiguration,
		channel:                channel,
		channelNameToChannel:   channelNameToChannel,
		priorityWorkersClosedC: make(chan struct{}),
		forceShutdownChannel:   make(chan struct{}),
	}, nil
}

func (c *Consumer[T]) Consume() (<-chan Delivery[T], error) {
	fnGetResult := func(msg T, details priority_channels.ReceiveDetails) Delivery[T] {
		return Delivery[T]{Msg: msg, ReceiveDetails: details}
	}
	return doConsume(c, fnGetResult)
}

// ConsumeMessages returns a stream of just the message payloads (T only)
// while Consume returns a stream of Delivery[T] which includes the message payload and the receive details.
// This is useful when either you don't care about the receive details or they are already included in the message payload.
func (c *Consumer[T]) ConsumeMessages() (<-chan T, error) {
	fnGetResult := func(msg T, details priority_channels.ReceiveDetails) T {
		return msg
	}
	return doConsume(c, fnGetResult)
}

func doConsume[T any, R any](c *Consumer[T], fnGetResult func(msg T, details priority_channels.ReceiveDetails) R) (<-chan R, error) {
	c.priorityWorkersUpdatesMtx.Lock()
	defer c.priorityWorkersUpdatesMtx.Unlock()

	if c.priorityConfigUpdatesC != nil {
		return nil, errors.New("consume already called")
	} else if c.isStopping {
		return nil, errors.New("cannot consume after stopping")
	}

	deliveries := make(chan R)
	c.priorityConfigUpdatesC = make(chan Configuration, 1)
	go func() {
		defer close(deliveries)
		for {
			select {
			case priorityConfig := <-c.priorityConfigUpdatesC:
				err := c.doUpdatePriorityConfiguration(priorityConfig)
				if err != nil {
					panic(fmt.Sprintf("failed to update priority configuration: %v", err))
				}
			case d, ok := <-c.channel.Output:
				if !ok {
					c.setClosed(priority_channels.PriorityChannelClosed, "")
					return
				}
				if d.Status != priority_channels.ReceiveSuccess && d.Status != priority_channels.ReceiveChannelClosed {
					c.setClosed(d.Status.ExitReason(), d.ChannelName())
					return
				}
				select {
				case deliveries <- fnGetResult(d.Msg, d.ReceiveDetails):
				case <-c.forceShutdownChannel:
					if c.onMessageDrop != nil {
						c.onMessageDrop(d.Msg, d.ReceiveDetails)
					}
				}
			}
		}
	}()

	return deliveries, nil
}

func (c *Consumer[T]) UpdatePriorityConfiguration(priorityConfiguration Configuration) error {
	select {
	case c.priorityConfigUpdatesC <- priorityConfiguration:
	default:
		return errors.New("priority configuration update is already in progress, please retry later")
	}
	return nil
}

func (c *Consumer[T]) doUpdatePriorityConfiguration(priorityConfiguration Configuration) error {
	if priorityConfiguration.PriorityWorkers == nil {
		return fmt.Errorf("no priority workers config found")
	}

	droppedMessages := make(map[string][]T)
	var droppedMessagesMtx sync.Mutex
	onMessageDrop := func(msg T, details priority_channels.ReceiveDetails) {
		droppedMessagesMtx.Lock()
		droppedMessages[details.ChannelName] = append(droppedMessages[details.ChannelName], msg)
		droppedMessagesMtx.Unlock()
	}
	c.channel.Shutdown(Force, OnMessageDrop[T](onMessageDrop))

	// read all messages from Output channel until it is closed
	// because we need to recreate all the channels, including the Output channel
	for d := range c.channel.Output {
		channelName := d.ChannelName()
		if channelName == "" {
			continue
		}
		droppedMessagesMtx.Lock()
		droppedMessages[channelName] = append(droppedMessages[channelName], d.Msg)
		droppedMessagesMtx.Unlock()
	}

	recreateConfigChannels := make(map[string]chan T)
	for channelName, msgs := range droppedMessages {
		prevRecreateMessagesCh := c.channelNameToReconfigureChannel[channelName]
		recreateCh := make(chan T, len(msgs)+len(prevRecreateMessagesCh))
		recreateConfigChannels[channelName] = recreateCh
		for _, msg := range msgs {
			recreateCh <- msg
		}
		if prevRecreateMessagesCh != nil && len(prevRecreateMessagesCh) > 0 {
			fmt.Printf("Recreating channel %s - %d messages still in old recreate channel, passing to new recreate channel\n", channelName, len(prevRecreateMessagesCh))
			for msg := range prevRecreateMessagesCh {
				recreateCh <- msg
			}
		}
		close(recreateCh)
	}
	c.channelNameToReconfigureChannel = recreateConfigChannels

	newChannel, err := newFromPriorityWorkersConfig(c.ctx, *priorityConfiguration.PriorityWorkers, c.channelNameToChannel, recreateConfigChannels)
	if err != nil {
		return fmt.Errorf("failed to create priority channel from configuration: %w", err)
	}
	c.channel = newChannel
	c.priorityConfig = priorityConfiguration
	return nil
}

// StopGracefully stops the consumer with a graceful shutdown, draining the unprocessed messages before stopping.
func (c *Consumer[T]) StopGracefully() {
	c.stop(Graceful, nil)
}

// StopImmediately stops the consumer in a forced manner.
// onMessageDrop is called when a message is dropped. It is optional and can be nil, in this case the message will be silently dropped.
func (c *Consumer[T]) StopImmediately(onMessageDrop func(msg T, details priority_channels.ReceiveDetails)) {
	c.stop(Force, onMessageDrop)
}

// Done returns a channel that is closed when the consumer is stopped.
func (c *Consumer[T]) Done() <-chan struct{} {
	return c.priorityWorkersClosedC
}

func (c *Consumer[T]) stop(mode ShutdownMode, onMessageDrop func(msg T, details priority_channels.ReceiveDetails)) {
	c.priorityWorkersUpdatesMtx.Lock()
	if !c.isStopping {
		c.isStopping = true
		c.onMessageDrop = onMessageDrop
		if onMessageDrop != nil {
			c.channel.Shutdown(mode, OnMessageDrop(onMessageDrop))
		} else {
			c.channel.Shutdown(mode)
		}
	}
	c.priorityWorkersUpdatesMtx.Unlock()

	if mode == Force {
		c.forceShutdownChannel <- struct{}{}
	}
	<-c.priorityWorkersClosedC
}

// Status returns whether the consumer is stopped, and if so, the reason for stopping and,
// in case the reason is a closed channel, the name of the channel that was closed.
func (c *Consumer[T]) Status() (stopped bool, reason priority_channels.ExitReason, channelName string) {
	c.priorityWorkersUpdatesMtx.Lock()
	defer c.priorityWorkersUpdatesMtx.Unlock()

	if c.isStopped {
		return true, c.exitReason, c.exitReasonChannelName
	}
	return false, priority_channels.UnknownExitReason, ""
}

func (c *Consumer[T]) setClosed(exitReason priority_channels.ExitReason, exitReasonChannelName string) {
	c.priorityWorkersUpdatesMtx.Lock()
	defer c.priorityWorkersUpdatesMtx.Unlock()

	c.isStopped = true
	c.exitReason = exitReason
	c.exitReasonChannelName = exitReasonChannelName
	close(c.priorityWorkersClosedC)
}
