package priority_workers

import (
	"github.com/dmgrit/priority-channels"
)

type ChannelWithShutdownFunc[T any] struct {
	Channel      <-chan T
	ShutdownFunc ShutdownFunc
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
	shutdownFunc ShutdownFunc
	name         string
	freqRatio    int
}

func (c *ResultChannelWithFreqRatio[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithFreqRatio[T]) ResultChannel() <-chan ReceiveResult[T] {
	return c.channel
}

func (c *ResultChannelWithFreqRatio[T]) Shutdown(mode ShutdownMode) {
	c.shutdownFunc(mode)
}

func (c *ResultChannelWithFreqRatio[T]) FreqRatio() int {
	return c.freqRatio
}

func NewResultChannelWithFreqRatio[T any](name string, channel <-chan ReceiveResult[T], shutdownFunc ShutdownFunc, freqRatio int) ResultChannelWithFreqRatio[T] {
	return ResultChannelWithFreqRatio[T]{
		name:         name,
		channel:      channel,
		shutdownFunc: shutdownFunc,
		freqRatio:    freqRatio,
	}
}

type ResultChannelWithPriority[T any] struct {
	channel      <-chan ReceiveResult[T]
	shutdownFunc ShutdownFunc
	name         string
	priority     int
}

func (c *ResultChannelWithPriority[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithPriority[T]) ResultChannel() <-chan ReceiveResult[T] {
	return c.channel
}

func (c *ResultChannelWithPriority[T]) Shutdown(mode ShutdownMode) {
	c.shutdownFunc(mode)
}

func (c *ResultChannelWithPriority[T]) Priority() int {
	return c.priority
}

func NewResultChannelWithPriority[T any](name string, channel <-chan ReceiveResult[T], shutdownFunc ShutdownFunc, priority int) ResultChannelWithPriority[T] {
	return ResultChannelWithPriority[T]{
		name:         name,
		channel:      channel,
		shutdownFunc: shutdownFunc,
		priority:     priority,
	}
}
