package priority_workers

import (
	"errors"
	"fmt"

	"github.com/dmgrit/priority-channels/channels"
)

var (
	ErrNoChannels                     = errors.New("no channels provided")
	ErrEmptyChannelName               = errors.New("channel name is empty")
	ErrFreqRatioMustBeGreaterThanZero = errors.New("frequency ratio must be greater than 0")
)

type DuplicateChannelError struct {
	ChannelName string
}

func (e *DuplicateChannelError) Error() string {
	return fmt.Sprintf("channel name '%s' is used more than once", e.ChannelName)
}

type ChannelValidationError struct {
	ChannelName string
	Err         error
}

func (e *ChannelValidationError) Error() string {
	return fmt.Sprintf("channel '%s': %v", e.ChannelName, e.Err)
}

type channelWithFreqRatio struct {
	ChannelName string
	FreqRatio   int
}

func convertChannelsWithFreqRatioToChannels[T any](channelsWithFreqRatio []channels.ChannelWithFreqRatio[T]) []channelWithFreqRatio {
	res := make([]channelWithFreqRatio, 0, len(channelsWithFreqRatio))
	for _, c := range channelsWithFreqRatio {
		res = append(res, channelWithFreqRatio{
			ChannelName: c.ChannelName(),
			FreqRatio:   c.FreqRatio(),
		})
	}
	return res
}

func convertResultChannelsWithFreqRatioToChannels[T any](channelsWithFreqRatio []ResultChannelWithFreqRatio[T]) []channelWithFreqRatio {
	res := make([]channelWithFreqRatio, 0, len(channelsWithFreqRatio))
	for _, c := range channelsWithFreqRatio {
		res = append(res, channelWithFreqRatio{
			ChannelName: c.Name(),
			FreqRatio:   c.FreqRatio(),
		})
	}
	return res
}

func convertResultChannelsWithFreqRatioExToChannels[T any](channelsWithFreqRatio []ResultChannelWithFreqRatioEx[T]) []channelWithFreqRatio {
	res := make([]channelWithFreqRatio, 0, len(channelsWithFreqRatio))
	for _, c := range channelsWithFreqRatio {
		res = append(res, channelWithFreqRatio{
			ChannelName: c.Name(),
			FreqRatio:   c.FreqRatio(),
		})
	}
	return res
}

func validateChannelsWithFreqRatio(channels []channelWithFreqRatio) error {
	if len(channels) == 0 {
		return ErrNoChannels
	}
	channelNames := make(map[string]struct{})
	for _, c := range channels {
		if c.ChannelName == "" {
			return ErrEmptyChannelName
		}
		if _, ok := channelNames[c.ChannelName]; ok {
			return &DuplicateChannelError{ChannelName: c.ChannelName}
		}
		if c.FreqRatio <= 0 {
			return &ChannelValidationError{
				ChannelName: c.ChannelName,
				Err:         ErrFreqRatioMustBeGreaterThanZero,
			}
		}
		channelNames[c.ChannelName] = struct{}{}
	}
	return nil
}
