package priority_workers

import (
	"github.com/dmgrit/priority-channels"
)

type ChannelWithShutdownFunc[T any] struct {
	Channel      <-chan T
	ShutdownFunc ShutdownFunc
}

type ReceiveResult[T any] struct {
	Msg         T
	ChannelName string
	Status      priority_channels.ReceiveStatus
}

func (r ReceiveResult[T]) GetMsg() T {
	return r.Msg
}

func (r ReceiveResult[T]) GetChannelName() string {
	return r.ChannelName
}

func (r ReceiveResult[T]) GetStatus() priority_channels.ReceiveStatus {
	return r.Status
}

type ReceiveResulter[T any] interface {
	GetMsg() T
	GetChannelName() string
	GetStatus() priority_channels.ReceiveStatus
}

type ReceiveResultEx[T any] struct {
	Msg            T
	ReceiveDetails priority_channels.ReceiveDetails
	Status         priority_channels.ReceiveStatus
}

func (r ReceiveResultEx[T]) GetMsg() T {
	return r.Msg
}

func (r ReceiveResultEx[T]) GetReceiveDetails() priority_channels.ReceiveDetails {
	return r.ReceiveDetails
}

func (r ReceiveResultEx[T]) GetStatus() priority_channels.ReceiveStatus {
	return r.Status
}

type ReceiveResulterEx[T any] interface {
	GetMsg() T
	GetReceiveDetails() priority_channels.ReceiveDetails
	GetStatus() priority_channels.ReceiveStatus
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

type ResultChannelWithFreqRatioEx[T any] struct {
	channel      <-chan ReceiveResultEx[T]
	shutdownFunc ShutdownFunc
	name         string
	freqRatio    int
}

func (c *ResultChannelWithFreqRatioEx[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithFreqRatioEx[T]) ResultChannel() <-chan ReceiveResultEx[T] {
	return c.channel
}

func (c *ResultChannelWithFreqRatioEx[T]) Shutdown(mode ShutdownMode) {
	c.shutdownFunc(mode)
}

func (c *ResultChannelWithFreqRatioEx[T]) FreqRatio() int {
	return c.freqRatio
}

func NewResultChannelWithFreqRatioEx[T any](name string, channel <-chan ReceiveResultEx[T], shutdownFunc ShutdownFunc, freqRatio int) ResultChannelWithFreqRatioEx[T] {
	return ResultChannelWithFreqRatioEx[T]{
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
		priority:     priority,
		shutdownFunc: shutdownFunc,
	}
}

type ResultChannelWithPriorityEx[T any] struct {
	channel      <-chan ReceiveResultEx[T]
	shutdownFunc ShutdownFunc
	name         string
	priority     int
}

func (c *ResultChannelWithPriorityEx[T]) Name() string {
	return c.name
}

func (c *ResultChannelWithPriorityEx[T]) ResultChannel() <-chan ReceiveResultEx[T] {
	return c.channel
}

func (c *ResultChannelWithPriorityEx[T]) Shutdown(mode ShutdownMode) {
	c.shutdownFunc(mode)
}

func (c *ResultChannelWithPriorityEx[T]) Priority() int {
	return c.priority
}

func NewResultChannelWithPriorityEx[T any](name string, channel <-chan ReceiveResultEx[T], shutdownFunc ShutdownFunc, priority int) ResultChannelWithPriorityEx[T] {
	return ResultChannelWithPriorityEx[T]{
		name:         name,
		channel:      channel,
		shutdownFunc: shutdownFunc,
		priority:     priority,
	}
}
