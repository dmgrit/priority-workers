package priority_workers

import (
	"context"
	"fmt"

	"github.com/dmgrit/priority-channels/channels"
)

type Configuration struct {
	PriorityWorkers *PriorityWorkersConfig `json:"priorityWorkers,omitempty"`
}

type PriorityWorkersConfig struct {
	Method   PriorityWorkersMethodConfig `json:"method"`
	Channels []ChannelConfig             `json:"channels"`
}

type PriorityWorkersMethodConfig string

const (
	ByHighestAlwaysFirstMethodConfig PriorityWorkersMethodConfig = "by-highest-always-first"
	ByFrequencyRatioMethodConfig     PriorityWorkersMethodConfig = "by-frequency-ratio"
)

type ChannelConfig struct {
	Name                   string `json:"name"`
	Priority               int    `json:"priority,omitempty"`
	FreqRatio              int    `json:"freqRatio,omitempty"`
	*PriorityWorkersConfig `json:"priorityWorkers,omitempty"`
}

func NewFromConfiguration[T any](ctx context.Context, config Configuration, channelNameToChannel map[string]<-chan T) (Channel[T], error) {
	if config.PriorityWorkers == nil {
		return Channel[T]{}, fmt.Errorf("no priority workers config found")
	}
	return newFromPriorityWorkersConfig(ctx, *config.PriorityWorkers, channelNameToChannel)
}

func newFromPriorityWorkersConfig[T any](ctx context.Context, config PriorityWorkersConfig, channelNameToChannel map[string]<-chan T) (Channel[T], error) {
	var isCombinedChannel bool
	for _, c := range config.Channels {
		if c.PriorityWorkersConfig != nil {
			isCombinedChannel = true
			break
		}
	}
	if !isCombinedChannel {
		if len(config.Channels) == 1 {
			c := config.Channels[0]
			channel, ok := channelNameToChannel[c.Name]
			if !ok {
				return Channel[T]{}, fmt.Errorf("channel %s not found", c.Name)
			}
			return ProcessChannel(ctx, c.Name, channel)
		}

		switch config.Method {
		case ByHighestAlwaysFirstMethodConfig:
			return Channel[T]{}, fmt.Errorf("by-highest-always-first method is not supported for first level channels")
		case ByFrequencyRatioMethodConfig:
			channelsWithFreqRatio := make([]channels.ChannelWithFreqRatio[T], 0, len(config.Channels))
			for _, c := range config.Channels {
				channel, ok := channelNameToChannel[c.Name]
				if !ok {
					return Channel[T]{}, fmt.Errorf("channel %s not found", c.Name)
				}
				channelsWithFreqRatio = append(channelsWithFreqRatio, channels.NewChannelWithFreqRatio(c.Name, channel, c.FreqRatio))
			}
			return ProcessByFrequencyRatio(ctx, channelsWithFreqRatio)
		default:
			return Channel[T]{}, fmt.Errorf("unknown type %s", config.Method)
		}
	}

	switch config.Method {
	case ByHighestAlwaysFirstMethodConfig:
		channelsWithPriority := make([]ChannelWithPriority[T], 0, len(config.Channels))
		for _, c := range config.Channels {
			var priorityChannel Channel[T]
			var err error
			if c.PriorityWorkersConfig == nil {
				priorityChannel, err = ProcessChannel(context.Background(), c.Name, channelNameToChannel[c.Name])
			} else {
				priorityChannel, err = newFromPriorityWorkersConfig[T](context.Background(), *c.PriorityWorkersConfig, channelNameToChannel)
			}
			if err != nil {
				return Channel[T]{}, err
			}
			channelsWithPriority = append(channelsWithPriority, NewChannelWithPriority(c.Name, priorityChannel, c.Priority))
		}
		return CombineByHighestAlwaysFirst(ctx, channelsWithPriority)
	case ByFrequencyRatioMethodConfig:
		channelsWithFreqRatio := make([]ChannelWithFreqRatio[T], 0, len(config.Channels))
		for _, c := range config.Channels {
			var priorityChannel Channel[T]
			var err error
			if c.PriorityWorkersConfig == nil {
				priorityChannel, err = ProcessChannel(context.Background(), c.Name, channelNameToChannel[c.Name])
			} else {
				priorityChannel, err = newFromPriorityWorkersConfig[T](context.Background(), *c.PriorityWorkersConfig, channelNameToChannel)
			}
			if err != nil {
				return Channel[T]{}, err
			}
			channelsWithFreqRatio = append(channelsWithFreqRatio, NewChannelWithFreqRatio(c.Name, priorityChannel, c.FreqRatio))
		}
		return CombineByFrequencyRatio(ctx, channelsWithFreqRatio)
	}

	return Channel[T]{}, fmt.Errorf("unknown type %s", config.Method)
}
