package priority_workers

import (
	"context"
	"sync"

	"github.com/dmgrit/priority-channels"
	"github.com/dmgrit/priority-channels/channels"
)

func getReceiveResult[T any](
	msg T,
	channelName string, channelIndex int,
	receiveDetails priority_channels.ReceiveDetails,
	status priority_channels.ReceiveStatus) ReceiveResult[T] {
	var pathInTree []priority_channels.ChannelNode
	if channelIndex != -1 {
		if receiveDetails.ChannelName == "" && len(receiveDetails.PathInTree) == 0 {
			receiveDetails.ChannelName = channelName
			receiveDetails.ChannelIndex = channelIndex
		} else {
			pathInTree = append([]priority_channels.ChannelNode{{
				ChannelName:  channelName,
				ChannelIndex: channelIndex,
			}}, receiveDetails.PathInTree...)
		}
	} else {
		pathInTree = receiveDetails.PathInTree
	}
	return ReceiveResult[T]{
		Msg: msg,
		ReceiveDetails: priority_channels.ReceiveDetails{
			ChannelName:  receiveDetails.ChannelName,
			ChannelIndex: receiveDetails.ChannelIndex,
			PathInTree:   pathInTree,
		},
		Status: status,
	}
}

type ShutdownMode int

const (
	Graceful ShutdownMode = iota
	Force
)

type ShutdownFunc func(mode ShutdownMode)

func processWithCallbackToChannel[R any](fnProcessWithCallback func(func(r R), func()) ShutdownFunc) (<-chan R, ShutdownFunc) {
	resChannel := make(chan R, 1)
	senderChannel := make(chan R)
	closeChannel := make(chan struct{})
	fnCallback := func(r R) {
		senderChannel <- r
	}
	fnClose := func() {
		close(closeChannel)
	}
	shutdownFunc := fnProcessWithCallback(fnCallback, fnClose)
	go func() {
		defer close(resChannel)
		for {
			select {
			case <-closeChannel:
				return
			case msg := <-senderChannel:
				resChannel <- msg
			}
		}
	}()
	return resChannel, shutdownFunc
}

func callCallbacksFunc[T any](
	onMessageReceived func(msg T, channelName string),
	onChannelClosed func(channelName string),
	onProcessingFinished func(reason priority_channels.ExitReason)) func(msg ReceiveResult[T]) {
	return func(msg ReceiveResult[T]) {
		switch msg.Status {
		case priority_channels.ReceiveSuccess:
			onMessageReceived(msg.Msg, msg.ChannelName())
		case priority_channels.ReceiveChannelClosed:
			if onChannelClosed != nil {
				onChannelClosed(msg.ChannelName())
			}
		case priority_channels.ReceiveContextCancelled, priority_channels.ReceiveNoOpenChannels:
			if onProcessingFinished != nil {
				onProcessingFinished(msg.Status.ExitReason())
			}
		default:
			return
		}
	}
}

func callCallbacksFuncEx[T any](
	onMessageReceived func(msg T, details priority_channels.ReceiveDetails),
	onChannelClosed func(details priority_channels.ReceiveDetails),
	onProcessingFinished func(reason priority_channels.ExitReason)) func(msg ReceiveResult[T]) {
	return func(msg ReceiveResult[T]) {
		switch msg.Status {
		case priority_channels.ReceiveSuccess:
			onMessageReceived(msg.Msg, msg.ReceiveDetails)
		case priority_channels.ReceiveChannelClosed:
			if onChannelClosed != nil {
				onChannelClosed(msg.ReceiveDetails)
			}
		case priority_channels.ReceiveContextCancelled, priority_channels.ReceiveNoOpenChannels:
			if onProcessingFinished != nil {
				onProcessingFinished(msg.Status.ExitReason())
			}
		default:
			return
		}
	}
}

func ProcessByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) (<-chan ReceiveResult[T], ShutdownFunc, error) {
	if err := validateChannelsWithFreqRatio(convertChannelsWithFreqRatioToChannels(channelsWithFreqRatios)); err != nil {
		return nil, nil, err
	}
	resChan, shutdownFunc := processByFrequencyRatio(ctx, channelsWithFreqRatios)
	return resChan, shutdownFunc, nil
}

func processByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) (<-chan ReceiveResult[T], ShutdownFunc) {
	return processWithCallbackToChannel(func(fnCallback func(r ReceiveResult[T]), fnClose func()) ShutdownFunc {
		return processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnClose)
	})
}

func ProcessByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T])) error {
	if err := validateChannelsWithFreqRatio(convertChannelsWithFreqRatioToChannels(channelsWithFreqRatios)); err != nil {
		return err
	}
	processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil)
	return nil
}

func ProcessByFrequencyRatioWithCallbacks[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T],
	onMessageReceived func(msg T, channelName string),
	onChannelClosed func(channelName string),
	onProcessingFinished func(reason priority_channels.ExitReason)) {
	processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios,
		callCallbacksFunc(onMessageReceived, onChannelClosed, onProcessingFinished), nil)
}

func ProcessByFrequencyRatioWithCallbacksEx[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T],
	onMessageReceived func(msg T, details priority_channels.ReceiveDetails),
	onChannelClosed func(details priority_channels.ReceiveDetails),
	onProcessingFinished func(reason priority_channels.ExitReason)) {
	processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios,
		callCallbacksFuncEx(onMessageReceived, onChannelClosed, onProcessingFinished), nil)
}

func processByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T],
	fnCallback func(ReceiveResult[T]),
	fnClose func()) ShutdownFunc {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	fnShutdown := func(_ ShutdownMode) {
		cancel()
	}
	var wg sync.WaitGroup
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			wg.Add(1)
			go func(c channels.ChannelWithFreqRatio[T], i int) {
				defer wg.Done()
				for {
					select {
					case <-ctxWithCancel.Done():
						// don't receive any new messages after the context is cancelled
						return
					case msg, ok := <-c.MsgsC():
						if !ok {
							closeChannelOnce.Do(func() {
								fnCallback(getReceiveResult(getZero[T](), "", -1,
									priority_channels.ReceiveDetails{ChannelName: c.ChannelName(), ChannelIndex: i},
									priority_channels.ReceiveChannelClosed))
							})
							return
						}
						fnCallback(getReceiveResult(
							msg, "", -1,
							priority_channels.ReceiveDetails{ChannelName: c.ChannelName(), ChannelIndex: i},
							priority_channels.ReceiveSuccess))
					}
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	go func() {
		wg.Wait()
		select {
		case <-ctxWithCancel.Done():
			fnCallback(getReceiveResult(
				getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveContextCancelled))
		default:
			fnCallback(getReceiveResult(getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveNoOpenChannels))
		}
		if fnClose != nil {
			fnClose()
		}
	}()
	return fnShutdown
}

func CombineByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T]) (<-chan ReceiveResult[T], ShutdownFunc, error) {
	if err := validateChannelsWithFreqRatio(convertResultChannelsWithFreqRatioToChannels(channelsWithFreqRatios)); err != nil {
		return nil, nil, err
	}
	resChan, shutdownFunc := processWithCallbackToChannel(func(fnCallback func(r ReceiveResult[T]), fnClose func()) ShutdownFunc {
		return combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnClose)
	})
	return resChan, shutdownFunc, nil
}

func CombineByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T])) (ShutdownFunc, error) {
	if err := validateChannelsWithFreqRatio(convertResultChannelsWithFreqRatioToChannels(channelsWithFreqRatios)); err != nil {
		return nil, err
	}
	shutdownFunc := combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil)
	return shutdownFunc, nil
}

func CombineByFrequencyRatioWithCallbacks[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T],
	onMessageReceived func(msg T, channelName string),
	onChannelClosed func(channelName string),
	onProcessingFinished func(reason priority_channels.ExitReason)) {
	combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios,
		callCallbacksFunc(onMessageReceived, onChannelClosed, onProcessingFinished), nil)
}

func CombineByFrequencyRatioWithCallbacksEx[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T],
	onMessageReceived func(msg T, details priority_channels.ReceiveDetails),
	onChannelClosed func(details priority_channels.ReceiveDetails),
	onProcessingFinished func(reason priority_channels.ExitReason)) {
	combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios,
		callCallbacksFuncEx(onMessageReceived, onChannelClosed, onProcessingFinished), nil)
}

func combineByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []ResultChannelWithFreqRatio[T], fnCallback func(ReceiveResult[T]), fnClose func()) ShutdownFunc {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	forceShutdownChannel := make(chan struct{})
	var shutdownOnce sync.Once
	fnShutdown := func(mode ShutdownMode) {
		if mode == Force {
			shutdownOnce.Do(func() {
				close(forceShutdownChannel)
			})
		}
		cancel()
	}
	var wg sync.WaitGroup
	var closeUnderlyingChannelsOnce sync.Once
	for i := range channelsWithFreqRatios {
		var closeChannelOnce sync.Once
		for j := 0; j < channelsWithFreqRatios[i].FreqRatio(); j++ {
			wg.Add(1)
			go func(c ResultChannelWithFreqRatio[T], i int) {
				defer wg.Done()
				for {
					select {
					case <-ctxWithCancel.Done():
						var shutdownMode ShutdownMode
						select {
						case <-forceShutdownChannel:
							shutdownMode = Force
						default:
							shutdownMode = Graceful
						}
						closeUnderlyingChannelsOnce.Do(func() {
							for _, c := range channelsWithFreqRatios {
								c.Shutdown(shutdownMode)
							}
						})
						// read all remaining messages from the channel
						for msg := range c.ResultChannel() {
							if msg.Status != priority_channels.ReceiveSuccess || shutdownMode == Force {
								// after channel cancellation - call callback only for successful messages of underlying channels
								continue
							}
							fnCallback(getReceiveResult(
								msg.Msg, c.Name(), i,
								msg.ReceiveDetails,
								msg.Status))
						}
						return

					case msg, ok := <-c.ResultChannel():
						if !ok {
							select {
							case <-ctxWithCancel.Done():
								continue
							default:
								closeChannelOnce.Do(func() {
									fnCallback(ReceiveResult[T]{
										Msg: getZero[T](),
										ReceiveDetails: priority_channels.ReceiveDetails{
											ChannelName:  c.Name(),
											ChannelIndex: i,
										},
										Status: priority_channels.ReceiveChannelClosed,
									})
								})
							}
							return
						}
						fnCallback(getReceiveResult(
							msg.Msg, c.Name(), i,
							msg.ReceiveDetails,
							msg.Status))
					}
				}
			}(channelsWithFreqRatios[i], i)
		}
	}
	go func() {
		wg.Wait()
		select {
		case <-ctxWithCancel.Done():
			fnCallback(ReceiveResult[T]{
				Msg:    getZero[T](),
				Status: priority_channels.ReceiveContextCancelled,
			})
		default:
			fnCallback(ReceiveResult[T]{
				Msg:            getZero[T](),
				ReceiveDetails: priority_channels.ReceiveDetails{},
				Status:         priority_channels.ReceiveNoOpenChannels,
			})
		}
		if fnClose != nil {
			fnClose()
		}
	}()
	return fnShutdown
}

func CombineByHighestAlwaysFirst[T any](ctx context.Context,
	resultChannelsWithPriority []ResultChannelWithPriority[T]) (<-chan ReceiveResult[T], ShutdownFunc, error) {
	channelsWithPriority := make([]channels.ChannelWithPriority[ReceiveResult[T]], 0, len(resultChannelsWithPriority))
	for _, resultChannelWithPriority := range resultChannelsWithPriority {
		channelsWithPriority = append(channelsWithPriority, channels.NewChannelWithPriority(
			resultChannelWithPriority.Name(),
			resultChannelWithPriority.ResultChannel(),
			resultChannelWithPriority.Priority()))
	}
	ch, err := priority_channels.NewByHighestAlwaysFirst(context.Background(), channelsWithPriority, priority_channels.AutoDisableClosedChannels())
	if err != nil {
		return nil, nil, err
	}
	shutdownUnderlyingChannelsFunc := func(mode ShutdownMode) {
		for _, resultChannelWithPriority := range resultChannelsWithPriority {
			resultChannelWithPriority.Shutdown(mode)
		}
	}
	wrappedCh, fnShutdown := processPriorityChannelWithUnwrap(ctx, ch, shutdownUnderlyingChannelsFunc)
	return wrappedCh, fnShutdown, nil
}

func ProcessChannel[T any](ctx context.Context, name string, c <-chan T) (<-chan ReceiveResult[T], ShutdownFunc, error) {
	if name == "" {
		return nil, nil, ErrEmptyChannelName
	}
	resChan, shutdownFunc := processChannel(ctx, name, c)
	return resChan, shutdownFunc, nil
}

func processChannel[T any](ctx context.Context, name string, c <-chan T) (<-chan ReceiveResult[T], ShutdownFunc) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	fnShutdown := func(_ ShutdownMode) {
		cancel()
	}

	var resChannel = make(chan ReceiveResult[T], 1)
	go func() {
		defer close(resChannel)
		for {
			select {
			case <-ctxWithCancel.Done():
				resChannel <- getReceiveResult(
					getZero[T](), "", -1,
					priority_channels.ReceiveDetails{},
					priority_channels.ReceiveContextCancelled)
				return
			case msg, ok := <-c:
				if !ok {
					resChannel <- getReceiveResult(getZero[T](), "", -1,
						priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
						priority_channels.ReceiveChannelClosed)
					return
				}
				resChannel <- getReceiveResult(
					msg, "", -1,
					priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
					priority_channels.ReceiveSuccess)
			}
		}
	}()
	return resChannel, fnShutdown
}

func ProcessPriorityChannel[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) (<-chan ReceiveResult[T], ShutdownFunc) {
	return processPriorityChannel(ctx, c)
}

func processPriorityChannel[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) (<-chan ReceiveResult[T], ShutdownFunc) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	resChannel := make(chan ReceiveResult[T], 1)
	fnShutdown := func(_ ShutdownMode) {
		cancel()
	}
	go func() {
		defer close(resChannel)
		for {
			msg, receiveDetails, status := c.ReceiveWithContextEx(ctxWithCancel)
			if status == priority_channels.ReceiveContextCancelled && receiveDetails.ChannelName == "" {
				return
			}
			resChannel <- getReceiveResult(msg, "", -1, receiveDetails, status)
		}
	}()
	return resChannel, fnShutdown
}

func processPriorityChannelWithUnwrap[T any](ctx context.Context, c *priority_channels.PriorityChannel[ReceiveResult[T]],
	shutdownUnderlyingChannelsFunc ShutdownFunc) (<-chan ReceiveResult[T], ShutdownFunc) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	forceShutdownChannel := make(chan struct{})
	var shutdownOnce sync.Once
	fnShutdown := func(mode ShutdownMode) {
		if mode == Force {
			shutdownOnce.Do(func() {
				close(forceShutdownChannel)
			})
		}
		cancel()
	}
	resChannel := make(chan ReceiveResult[T], 1)
	go func() {
		defer close(resChannel)
		for {
			msg, receiveDetails, status := receiveUnwrapped(ctxWithCancel, c)
			if status == priority_channels.ReceiveContextCancelled && receiveDetails.ChannelName == "" && len(receiveDetails.PathInTree) == 0 {
				var shutdownMode ShutdownMode
				select {
				case <-forceShutdownChannel:
					shutdownMode = Force
				default:
					shutdownMode = Graceful
				}
				shutdownUnderlyingChannelsFunc(shutdownMode)
				// read all remaining messages from the channel until all underlying channels are closed
				for {
					msg, receiveDetails, status = receiveUnwrapped(context.Background(), c)
					if status != priority_channels.ReceiveSuccess &&
						(receiveDetails.ChannelName != "" || len(receiveDetails.PathInTree) != 0) {
						// after channel cancellation - call callback only for successful messages of underlying channels
						continue
					}
					if status != priority_channels.ReceiveNoOpenChannels && shutdownMode == Graceful {
						resChannel <- getReceiveResult(msg, "", -1, receiveDetails, status)
					}
					if status == priority_channels.ReceiveNoOpenChannels {
						resChannel <- getReceiveResult(msg, "", -1, receiveDetails, priority_channels.ReceiveContextCancelled)
						break
					}
				}
				return
			}
			resChannel <- getReceiveResult(msg, "", -1, receiveDetails, status)
		}
	}()
	return resChannel, fnShutdown
}

func receiveUnwrapped[T any](ctx context.Context, pc *priority_channels.PriorityChannel[ReceiveResult[T]]) (msg T, details priority_channels.ReceiveDetails, status priority_channels.ReceiveStatus) {
	result, receiveDetails, status := pc.ReceiveWithContextEx(ctx)
	if status != priority_channels.ReceiveSuccess {
		return getZero[T](), receiveDetails, status
	}
	var combinedReceiveDetails priority_channels.ReceiveDetails
	if result.ReceiveDetails.ChannelName == "" && len(result.ReceiveDetails.PathInTree) == 0 {
		combinedReceiveDetails = priority_channels.ReceiveDetails{
			ChannelName:  receiveDetails.ChannelName,
			ChannelIndex: receiveDetails.ChannelIndex,
		}
	} else {
		combinedReceiveDetails = priority_channels.ReceiveDetails{
			ChannelName:  result.ReceiveDetails.ChannelName,
			ChannelIndex: result.ReceiveDetails.ChannelIndex,
			PathInTree: append(append(receiveDetails.PathInTree, priority_channels.ChannelNode{
				ChannelName:  receiveDetails.ChannelName,
				ChannelIndex: receiveDetails.ChannelIndex,
			}), result.ReceiveDetails.PathInTree...),
		}
	}
	return result.Msg, combinedReceiveDetails, result.Status
}

func getZero[T any]() T {
	var result T
	return result
}
