package priority_workers

import (
	"github.com/dmgrit/priority-channels"
)

type ChannelWithShutdownFunc[T any] struct {
	Channel      <-chan T
	ShutdownFunc ShutdownFunc[T]
}

type ReceiveResult[T any] struct {
	Msg            T
	ReceiveDetails priority_channels.ReceiveDetails
	Status         priority_channels.ReceiveStatus
}

func (r ReceiveResult[T]) ChannelName() string {
	return r.ReceiveDetails.ChannelName
}

type ResultChannelWithFreqRatio[T any] struct {
	channel      <-chan ReceiveResult[T]
	shutdownFunc ShutdownFunc[T]
	name         string
	freqRatio    int
}

func (c *ResultChannelWithFreqRatio[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithFreqRatio[T]) ResultChannel() <-chan ReceiveResult[T] {
	return c.channel
}

func (c *ResultChannelWithFreqRatio[T]) Shutdown(mode ShutdownMode, options ...func(*ShutdownOptions[T])) {
	c.shutdownFunc(mode, options...)
}

func (c *ResultChannelWithFreqRatio[T]) FreqRatio() int {
	return c.freqRatio
}

func NewResultChannelWithFreqRatio[T any](name string, channel <-chan ReceiveResult[T], shutdownFunc ShutdownFunc[T], freqRatio int) ResultChannelWithFreqRatio[T] {
	return ResultChannelWithFreqRatio[T]{
		name:         name,
		channel:      channel,
		shutdownFunc: shutdownFunc,
		freqRatio:    freqRatio,
	}
}

type ResultChannelWithPriority[T any] struct {
	channel      <-chan ReceiveResult[T]
	shutdownFunc ShutdownFunc[T]
	name         string
	priority     int
}

func (c *ResultChannelWithPriority[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithPriority[T]) ResultChannel() <-chan ReceiveResult[T] {
	return c.channel
}

func (c *ResultChannelWithPriority[T]) Shutdown(mode ShutdownMode, options ...func(*ShutdownOptions[T])) {
	c.shutdownFunc(mode, options...)
}

func (c *ResultChannelWithPriority[T]) Priority() int {
	return c.priority
}

func NewResultChannelWithPriority[T any](name string, channel <-chan ReceiveResult[T], shutdownFunc ShutdownFunc[T], priority int) ResultChannelWithPriority[T] {
	return ResultChannelWithPriority[T]{
		name:         name,
		channel:      channel,
		shutdownFunc: shutdownFunc,
		priority:     priority,
	}
}
