package priority_workers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dmgrit/priority-workers"
)

func TestNewFromConfiguration(t *testing.T) {
	config := priority_workers.Configuration{
		PriorityWorkers: &priority_workers.PriorityWorkersConfig{
			Method: "by-frequency-ratio",
			Channels: []priority_workers.ChannelConfig{
				{
					Name:      "channel-1",
					FreqRatio: 2,
				},
				{
					Name:      "channel-2",
					FreqRatio: 3,
				},
				{
					Name:      "priority-channel-3",
					FreqRatio: 3,
					PriorityWorkersConfig: &priority_workers.PriorityWorkersConfig{
						Method: "by-frequency-ratio",
						Channels: []priority_workers.ChannelConfig{
							{
								Name:      "channel-3",
								FreqRatio: 1,
							},
							{
								Name:      "channel-4",
								FreqRatio: 2,
							},
						},
					},
				},
			},
		},
	}

	var channelNameToChannel = map[string]<-chan string{
		"channel-1": make(chan string),
		"channel-2": make(chan string),
		"channel-3": make(chan string),
		"channel-4": make(chan string),
	}

	channel, err := priority_workers.NewFromConfiguration(context.Background(), config, channelNameToChannel)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	channel.Shutdown(priority_workers.Graceful)

	jsonConfig, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}
	t.Logf(string(jsonConfig))
}
