package priority_workers

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/dmgrit/priority-channels"
	"github.com/dmgrit/priority-channels/channels"
)

func getDelivery[T any](
	msg T,
	channelName string, channelIndex int,
	receiveDetails priority_channels.ReceiveDetails,
	status priority_channels.ReceiveStatus) Delivery[T] {
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
	return Delivery[T]{
		Msg: msg,
		ReceiveDetails: priority_channels.ReceiveDetails{
			ChannelName:  receiveDetails.ChannelName,
			ChannelIndex: receiveDetails.ChannelIndex,
			PathInTree:   pathInTree,
		},
		Status: status,
	}
}

func processWithCallbackToChannel[T any](fnProcessWithCallback func(func(r Delivery[T]), func()) ShutdownFunc[T]) Channel[T] {
	resChannel := make(chan Delivery[T], 1)
	senderChannel := make(chan Delivery[T])
	closeChannel := make(chan struct{})
	fnCallback := func(r Delivery[T]) {
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
	return NewChannel(resChannel, shutdownFunc)
}

func callCallbacksFunc[T any](
	onMessageReceived func(msg T, channelName string),
	onChannelClosed func(channelName string),
	onProcessingFinished func(reason priority_channels.ExitReason)) func(msg Delivery[T]) {
	return func(msg Delivery[T]) {
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
	onProcessingFinished func(reason priority_channels.ExitReason)) func(msg Delivery[T]) {
	return func(msg Delivery[T]) {
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
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) (Channel[T], error) {
	if err := validateChannelsWithFreqRatio(convertChannelsWithFreqRatioForValidation(channelsWithFreqRatios)); err != nil {
		return Channel[T]{}, err
	}
	resChan := processByFrequencyRatio(ctx, channelsWithFreqRatios)
	return resChan, nil
}

func processByFrequencyRatio[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T]) Channel[T] {
	return processWithCallbackToChannel(func(fnCallback func(r Delivery[T]), fnClose func()) ShutdownFunc[T] {
		return processByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnClose)
	})
}

func ProcessByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []channels.ChannelWithFreqRatio[T], fnCallback func(Delivery[T])) error {
	if err := validateChannelsWithFreqRatio(convertChannelsWithFreqRatioForValidation(channelsWithFreqRatios)); err != nil {
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
	fnCallback func(Delivery[T]),
	fnClose func()) ShutdownFunc[T] {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	fnShutdown := func(_ ShutdownMode, options ...func(*ShutdownOptions[T])) {
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
							if strings.HasSuffix(c.ChannelName(), recreateChannelNameSuffix) {
								logDebugMessage("Finished transferring messages for recreated channel %s\n", c.ChannelName())
								return
							}
							closeChannelOnce.Do(func() {
								fnCallback(getDelivery(getZero[T](), "", -1,
									priority_channels.ReceiveDetails{ChannelName: c.ChannelName(), ChannelIndex: i},
									priority_channels.ReceiveChannelClosed))
							})
							return
						}
						channelName := strings.TrimSuffix(c.ChannelName(), recreateChannelNameSuffix)
						// channelName := c.ChannelName()
						fnCallback(getDelivery(
							msg, "", -1,
							priority_channels.ReceiveDetails{ChannelName: channelName, ChannelIndex: i},
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
			fnCallback(getDelivery(
				getZero[T](), "", -1,
				priority_channels.ReceiveDetails{},
				priority_channels.ReceiveContextCancelled))
		default:
			fnCallback(getDelivery(getZero[T](), "", -1,
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
	channelsWithFreqRatios []ChannelWithFreqRatio[T]) (Channel[T], error) {
	if err := validateChannelsWithFreqRatio(convertWorkersChannelsWithFreqRatioForValidation(channelsWithFreqRatios)); err != nil {
		return Channel[T]{}, err
	}
	resChan := processWithCallbackToChannel(func(fnCallback func(r Delivery[T]), fnClose func()) ShutdownFunc[T] {
		return combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, fnClose)
	})
	return resChan, nil
}

func CombineByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []ChannelWithFreqRatio[T], fnCallback func(Delivery[T])) (ShutdownFunc[T], error) {
	if err := validateChannelsWithFreqRatio(convertWorkersChannelsWithFreqRatioForValidation(channelsWithFreqRatios)); err != nil {
		return nil, err
	}
	shutdownFunc := combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios, fnCallback, nil)
	return shutdownFunc, nil
}

func CombineByFrequencyRatioWithCallbacks[T any](ctx context.Context,
	channelsWithFreqRatios []ChannelWithFreqRatio[T],
	onMessageReceived func(msg T, channelName string),
	onChannelClosed func(channelName string),
	onProcessingFinished func(reason priority_channels.ExitReason)) {
	combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios,
		callCallbacksFunc(onMessageReceived, onChannelClosed, onProcessingFinished), nil)
}

func CombineByFrequencyRatioWithCallbacksEx[T any](ctx context.Context,
	channelsWithFreqRatios []ChannelWithFreqRatio[T],
	onMessageReceived func(msg T, details priority_channels.ReceiveDetails),
	onChannelClosed func(details priority_channels.ReceiveDetails),
	onProcessingFinished func(reason priority_channels.ExitReason)) {
	combineByFrequencyRatioWithCallback(ctx, channelsWithFreqRatios,
		callCallbacksFuncEx(onMessageReceived, onChannelClosed, onProcessingFinished), nil)
}

func combineByFrequencyRatioWithCallback[T any](ctx context.Context,
	channelsWithFreqRatios []ChannelWithFreqRatio[T], fnCallback func(Delivery[T]), fnClose func()) ShutdownFunc[T] {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	forceShutdownChannel := make(chan struct{})
	var shutdownOnce sync.Once
	shutdownOptions := &ShutdownOptions[T]{}
	var shutdownOptionsParams []func(*ShutdownOptions[T])
	fnShutdown := func(mode ShutdownMode, options ...func(*ShutdownOptions[T])) {
		shutdownOptionsParams = options
		for _, option := range options {
			option(shutdownOptions)
		}
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
			go func(c ChannelWithFreqRatio[T], i int) {
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
								c.Shutdown(shutdownMode, shutdownOptionsParams...)
							}
						})
						// read all remaining messages from the channel
						for msg := range c.Output() {
							if msg.Status != priority_channels.ReceiveSuccess || shutdownMode == Force {
								if msg.Status == priority_channels.ReceiveSuccess && shutdownOptions.onMessageDrop != nil {
									shutdownOptions.onMessageDrop(msg.Msg, msg.ReceiveDetails)
								}

								// after channel cancellation - call callback only for successful messages of underlying channels
								continue
							}
							fnCallback(getDelivery(
								msg.Msg, c.Name(), i,
								msg.ReceiveDetails,
								msg.Status))
						}
						return

					case msg, ok := <-c.Output():
						if !ok {
							select {
							case <-ctxWithCancel.Done():
								continue
							default:
								closeChannelOnce.Do(func() {
									fnCallback(Delivery[T]{
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
						fnCallback(getDelivery(
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
			fnCallback(Delivery[T]{
				Msg:    getZero[T](),
				Status: priority_channels.ReceiveContextCancelled,
			})
		default:
			fnCallback(Delivery[T]{
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
	resultChannelsWithPriority []ChannelWithPriority[T]) (Channel[T], error) {
	channelsWithPriority := make([]channels.ChannelWithPriority[Delivery[T]], 0, len(resultChannelsWithPriority))
	for _, resultChannelWithPriority := range resultChannelsWithPriority {
		channelsWithPriority = append(channelsWithPriority, channels.NewChannelWithPriority(
			resultChannelWithPriority.Name(),
			resultChannelWithPriority.Output(),
			resultChannelWithPriority.Priority()))
	}
	ch, err := priority_channels.NewByHighestAlwaysFirst(context.Background(), channelsWithPriority, priority_channels.AutoDisableClosedChannels())
	if err != nil {
		return Channel[T]{}, err
	}
	shutdownUnderlyingChannelsFunc := func(mode ShutdownMode, options ...func(*ShutdownOptions[T])) {
		for _, resultChannelWithPriority := range resultChannelsWithPriority {
			resultChannelWithPriority.Shutdown(mode, options...)
		}
	}
	wrappedCh := processPriorityChannelWithUnwrap(ctx, ch, shutdownUnderlyingChannelsFunc)
	return wrappedCh, nil
}

func ProcessChannel[T any](ctx context.Context, name string, c <-chan T) (Channel[T], error) {
	if name == "" {
		return Channel[T]{}, ErrEmptyChannelName
	}
	resChan := processChannel(ctx, name, c)
	return resChan, nil
}

func processChannel[T any](ctx context.Context, name string, c <-chan T) Channel[T] {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	fnShutdown := func(_ ShutdownMode, options ...func(*ShutdownOptions[T])) {
		cancel()
	}

	var resChannel = make(chan Delivery[T], 1)
	go func() {
		defer close(resChannel)
		for {
			select {
			case <-ctxWithCancel.Done():
				resChannel <- getDelivery(
					getZero[T](), "", -1,
					priority_channels.ReceiveDetails{},
					priority_channels.ReceiveContextCancelled)
				return
			case msg, ok := <-c:
				if !ok {
					resChannel <- getDelivery(getZero[T](), "", -1,
						priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
						priority_channels.ReceiveChannelClosed)
					return
				}
				resChannel <- getDelivery(
					msg, "", -1,
					priority_channels.ReceiveDetails{ChannelName: name, ChannelIndex: 0},
					priority_channels.ReceiveSuccess)
			}
		}
	}()
	return NewChannel(resChannel, fnShutdown)
}

func ProcessPriorityChannel[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) (Channel[T], error) {
	if c == nil {
		return Channel[T]{}, errors.New("priority channel is nil")
	}
	resChan := processPriorityChannel(ctx, c)
	return resChan, nil
}

func processPriorityChannel[T any](ctx context.Context, c *priority_channels.PriorityChannel[T]) Channel[T] {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	resChannel := make(chan Delivery[T], 1)
	fnShutdown := func(_ ShutdownMode, options ...func(*ShutdownOptions[T])) {
		cancel()
	}
	go func() {
		defer close(resChannel)
		for {
			msg, receiveDetails, status := c.ReceiveWithContextEx(ctxWithCancel)
			if status == priority_channels.ReceiveContextCancelled && receiveDetails.ChannelName == "" {
				return
			}
			resChannel <- getDelivery(msg, "", -1, receiveDetails, status)
		}
	}()
	return NewChannel(resChannel, fnShutdown)
}

func processPriorityChannelWithUnwrap[T any](ctx context.Context, c *priority_channels.PriorityChannel[Delivery[T]],
	shutdownUnderlyingChannelsFunc ShutdownFunc[T]) Channel[T] {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	forceShutdownChannel := make(chan struct{})
	var shutdownOnce sync.Once
	shutdownOptions := &ShutdownOptions[T]{}
	var shutdownOptionsParams []func(*ShutdownOptions[T])
	fnShutdown := func(mode ShutdownMode, options ...func(*ShutdownOptions[T])) {
		shutdownOptionsParams = options
		for _, option := range options {
			option(shutdownOptions)
		}
		if mode == Force {
			shutdownOnce.Do(func() {
				close(forceShutdownChannel)
			})
		}
		cancel()
	}
	resChannel := make(chan Delivery[T], 1)
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
				shutdownUnderlyingChannelsFunc(shutdownMode, shutdownOptionsParams...)
				// read all remaining messages from the channel until all underlying channels are closed
				for {
					msg, receiveDetails, status = receiveUnwrapped(context.Background(), c)
					if status != priority_channels.ReceiveSuccess &&
						(receiveDetails.ChannelName != "" || len(receiveDetails.PathInTree) != 0) {
						// after channel cancellation - call callback only for successful messages of underlying channels
						continue
					}
					if status != priority_channels.ReceiveNoOpenChannels && shutdownMode == Graceful {
						resChannel <- getDelivery(msg, "", -1, receiveDetails, status)
					} else if status == priority_channels.ReceiveSuccess && shutdownOptions.onMessageDrop != nil {
						shutdownOptions.onMessageDrop(msg, receiveDetails)
					}
					if status == priority_channels.ReceiveNoOpenChannels {
						resChannel <- getDelivery(msg, "", -1, receiveDetails, priority_channels.ReceiveContextCancelled)
						break
					}
				}
				return
			}
			resChannel <- getDelivery(msg, "", -1, receiveDetails, status)
		}
	}()
	return NewChannel(resChannel, fnShutdown)
}

func receiveUnwrapped[T any](ctx context.Context, pc *priority_channels.PriorityChannel[Delivery[T]]) (msg T, details priority_channels.ReceiveDetails, status priority_channels.ReceiveStatus) {
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

func logDebugMessage(msg string, args ...any) {
	// fmt.Printf(msg, args...)
}
