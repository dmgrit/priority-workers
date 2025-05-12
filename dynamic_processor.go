package priority_workers

import (
	"context"

	"github.com/dmgrit/priority-channels"
	"github.com/dmgrit/workerpool"
)

type DynamicPriorityProcessor[T any] struct {
	consumer   *Consumer[T]
	workerPool *workerpool.DynamicWorkerPool[Delivery[T]]
}

func NewDynamicPriorityProcessor[T any](
	ctx context.Context,
	processFn func(Delivery[T]),
	channelNameToChannel map[string]<-chan T,
	priorityConfiguration Configuration,
	workersNum int) (*DynamicPriorityProcessor[T], error) {
	consumer, err := NewConsumer(ctx, channelNameToChannel, priorityConfiguration)
	if err != nil {
		return nil, err
	}
	// We're closing the worker pool by having the consumer close its delivery channel.
	// We want the pool to continue processing messages until the consumer explicitly signals that it has stopped.
	// That's why we don't pass the same context used to cancel the consumer to the pool.
	workerPool, err := workerpool.NewDynamicWorkerPool(context.Background(), processFn, workersNum)
	if err != nil {
		return nil, err
	}

	processor := &DynamicPriorityProcessor[T]{
		consumer:   consumer,
		workerPool: workerPool,
	}
	return processor, nil
}

func (p *DynamicPriorityProcessor[T]) Start() error {
	msgs, err := p.consumer.Consume()
	if err != nil {
		return err
	}
	err = p.workerPool.Process(msgs)
	if err != nil {
		return err
	}
	return nil
}

func (p *DynamicPriorityProcessor[T]) StopGracefully() {
	p.consumer.StopGracefully()
}

func (p *DynamicPriorityProcessor[T]) StopImmediately(onMessageDrop func(msg T, details priority_channels.ReceiveDetails)) {
	p.consumer.StopImmediately(onMessageDrop)
}

func (p *DynamicPriorityProcessor[T]) Done() <-chan struct{} {
	return p.workerPool.Done()
}

func (p *DynamicPriorityProcessor[T]) UpdatePriorityConfiguration(priorityConfiguration Configuration) error {
	return p.consumer.UpdatePriorityConfiguration(priorityConfiguration)
}

func (p *DynamicPriorityProcessor[T]) WorkersNum() int {
	return p.workerPool.WorkersNum()
}

func (p *DynamicPriorityProcessor[T]) UpdateWorkersNum(newWorkersNum int) error {
	return p.workerPool.UpdateWorkersNum(newWorkersNum)
}

func (p *DynamicPriorityProcessor[T]) ActiveWorkersNum() int {
	return p.workerPool.ActiveWorkersNum()
}

func (p *DynamicPriorityProcessor[T]) Status() (stopped bool, reason priority_channels.ExitReason, channelName string) {
	select {
	case <-p.workerPool.Done():
		return p.consumer.Status()
	default:
		return false, priority_channels.UnknownExitReason, ""
	}
}
