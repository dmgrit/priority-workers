package priority_workers

import (
	"github.com/dmgrit/priority-channels"
)

type Channel[T any] struct {
	Output   <-chan Delivery[T]
	Shutdown ShutdownFunc[T]
}

func NewChannel[T any](output <-chan Delivery[T], shutdownFunc ShutdownFunc[T]) Channel[T] {
	return Channel[T]{
		Output:   output,
		Shutdown: shutdownFunc,
	}
}

type Delivery[T any] struct {
	Msg            T
	ReceiveDetails priority_channels.ReceiveDetails
	Status         priority_channels.ReceiveStatus
}

func (r Delivery[T]) ChannelName() string {
	return r.ReceiveDetails.ChannelName
}

type ShutdownMode int

const (
	Graceful ShutdownMode = iota
	Force
)

type ShutdownOptions[T any] struct {
	onMessageDrop func(msg T, details priority_channels.ReceiveDetails)
}

func OnMessageDrop[T any](onMessageDrop func(msg T, details priority_channels.ReceiveDetails)) func(*ShutdownOptions[T]) {
	return func(options *ShutdownOptions[T]) {
		options.onMessageDrop = onMessageDrop
	}
}

type ShutdownFunc[T any] func(mode ShutdownMode, options ...func(*ShutdownOptions[T]))

type ChannelWithFreqRatio[T any] struct {
	channel   Channel[T]
	name      string
	freqRatio int
}

func (c *ChannelWithFreqRatio[T]) Name() string {
	return c.name
}

func (c *ChannelWithFreqRatio[T]) Output() <-chan Delivery[T] {
	return c.channel.Output
}

func (c *ChannelWithFreqRatio[T]) Shutdown(mode ShutdownMode, options ...func(*ShutdownOptions[T])) {
	c.channel.Shutdown(mode, options...)
}

func (c *ChannelWithFreqRatio[T]) FreqRatio() int {
	return c.freqRatio
}

func NewChannelWithFreqRatio[T any](name string, channel Channel[T], freqRatio int) ChannelWithFreqRatio[T] {
	return ChannelWithFreqRatio[T]{
		name:      name,
		channel:   channel,
		freqRatio: freqRatio,
	}
}

type ChannelWithPriority[T any] struct {
	channel  Channel[T]
	name     string
	priority int
}

func (c *ChannelWithPriority[T]) Name() string {
	return c.name
}

func (c *ChannelWithPriority[T]) Output() <-chan Delivery[T] {
	return c.channel.Output
}

func (c *ChannelWithPriority[T]) Shutdown(mode ShutdownMode, options ...func(*ShutdownOptions[T])) {
	c.channel.Shutdown(mode, options...)
}

func (c *ChannelWithPriority[T]) Priority() int {
	return c.priority
}

func NewChannelWithPriority[T any](name string, channel Channel[T], priority int) ChannelWithPriority[T] {
	return ChannelWithPriority[T]{
		name:     name,
		channel:  channel,
		priority: priority,
	}
}
