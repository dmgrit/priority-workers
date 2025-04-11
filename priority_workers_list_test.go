package priority_workers_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dmgrit/priority-channels"
	"github.com/dmgrit/priority-channels/channels"
	"github.com/dmgrit/priority-workers"
)

func TestProcessMessagesByFrequencyRatio_RandomChannelsList(t *testing.T) {
	var testCases = []struct {
		Name string
	}{
		{
			Name: "ByGoroutines",
		},
	}

	channelsWithFreqRatio, channelsWithExpectedRatios := generateRandomFreqRatioList(8, 16)
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			testProcessMessagesByFrequencyRatio_RandomChannelsList(t,
				channelsWithFreqRatio, channelsWithExpectedRatios, nil, false)
		})
	}
}

func TestProcessMessagesByFrequencyRatio_RandomChannelsListSubset(t *testing.T) {
	var testCases = []struct {
		Name          string
		CloseChannels bool
	}{
		{
			Name: "ByGoroutines",
		},
		{
			Name:          "ByGoroutines-CloseChannels",
			CloseChannels: true,
		},
	}

	channelsWithFreqRatio, channelsWithExpectedRatios := generateRandomFreqRatioList(8, 16)
	channelIndexesSubset := getRandomChannelIndexesSubset(len(channelsWithExpectedRatios))
	if channelIndexesSubset != nil {
		recomputeRandomFreqListExpectedRatios(channelsWithFreqRatio, channelsWithExpectedRatios, channelIndexesSubset)
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			testProcessMessagesByFrequencyRatio_RandomChannelsList(t,
				channelsWithFreqRatio, channelsWithExpectedRatios, channelIndexesSubset,
				tc.CloseChannels)
		})
	}
}

func generateRandomFreqRatioList(minListSize, maxListSize int) ([]channels.ChannelWithFreqRatio[string], map[string]*channelWithExpectedRatio) {
	childrenNum := rand.N(maxListSize-minListSize+1) + minListSize
	totalSum := 0.0
	weights := make([]int, 0, childrenNum)
	for i := 0; i < childrenNum; i++ {
		w := rand.N(10) + 1
		weights = append(weights, w)
		totalSum += float64(w)
	}
	expectedRatios := make([]float64, 0, childrenNum)
	accExpectedRatio := 0.0
	for i := 0; i < childrenNum; i++ {
		if i == childrenNum-1 {
			expectedRatios = append(expectedRatios, 1.0-accExpectedRatio)
			break
		}
		expectedRatio := float64(weights[i]) / totalSum
		expectedRatios = append(expectedRatios, expectedRatio)
		accExpectedRatio += expectedRatio
	}
	channelsWithExpectedRatios := make(map[string]*channelWithExpectedRatio)
	channelsWithFreqRatio := make([]channels.ChannelWithFreqRatio[string], 0, childrenNum)
	for i := 0; i < childrenNum; i++ {
		cwr := &channelWithExpectedRatio{
			channel:       make(chan string, 10),
			expectedRatio: expectedRatios[i],
			channelIndex:  i,
		}
		channelName := fmt.Sprintf("channel-%d", i)
		channelsWithExpectedRatios[channelName] = cwr
		childChannel := channels.NewChannelWithFreqRatio(channelName, cwr.channel, weights[i])
		channelsWithFreqRatio = append(channelsWithFreqRatio, childChannel)
	}
	return channelsWithFreqRatio, channelsWithExpectedRatios
}

func recomputeRandomFreqListExpectedRatios(
	channelsWithFreqRatio []channels.ChannelWithFreqRatio[string],
	channelsWithExpectedRatios map[string]*channelWithExpectedRatio,
	channelIndexesSubset map[int]struct{}) {
	totalSum := 0.0
	for i := range channelIndexesSubset {
		totalSum += float64(channelsWithFreqRatio[i].FreqRatio())
	}
	for i := 0; i < len(channelsWithFreqRatio); i++ {
		c := channelsWithFreqRatio[i]
		cwr := channelsWithExpectedRatios[c.ChannelName()]
		if _, ok := channelIndexesSubset[i]; !ok {
			cwr.expectedRatio = 0.0
			continue
		}
		expectedRatio := float64(c.FreqRatio()) / totalSum
		cwr.expectedRatio = expectedRatio
	}
}

func testProcessMessagesByFrequencyRatio_RandomChannelsList(t *testing.T,
	channelsWithFreqRatio []channels.ChannelWithFreqRatio[string],
	channelsWithExpectedRatios map[string]*channelWithExpectedRatio,
	channelIndexesSubset map[int]struct{},
	closeChannels bool,
) {
	ctx, cancel := context.WithCancel(context.Background())

	if len(channelIndexesSubset) > 0 {
		t.Logf("Taking subset of %d channels out of %d\n", len(channelIndexesSubset), len(channelsWithFreqRatio))
		for channelName, cwr := range channelsWithExpectedRatios {
			if _, ok := channelIndexesSubset[cwr.channelIndex]; !ok {
				delete(channelsWithExpectedRatios, channelName)
				if closeChannels {
					close(cwr.channel)
				}
			}
		}
	}

	messagesNum := 100000
	for channelName, cwr := range channelsWithExpectedRatios {
		go func(channelName string, cwr *channelWithExpectedRatio) {
			for j := 1; j <= messagesNum; j++ {
				select {
				case <-ctx.Done():
					return
				case cwr.channel <- fmt.Sprintf("Message %d", j):
				}
			}
		}(channelName, cwr)
	}

	totalCount := 0
	countPerChannel := make(map[string]int)
	var mtx sync.Mutex
	processingDone := make(chan struct{})

	err := priority_workers.ProcessByFrequencyRatioWithCallback(ctx, channelsWithFreqRatio, func(result priority_workers.Delivery[string]) {
		if result.Status != priority_channels.ReceiveSuccess {
			if result.Status == priority_channels.ReceiveContextCancelled {
				processingDone <- struct{}{}
			}
			return
		}
		time.Sleep(1 * time.Millisecond)
		var reachedTotalCount bool
		mtx.Lock()
		totalCount++
		countPerChannel[result.ChannelName()] = countPerChannel[result.ChannelName()] + 1
		reachedTotalCount = totalCount == messagesNum
		mtx.Unlock()
		if reachedTotalCount {
			cancel()
			return
		}
	})
	if err != nil {
		t.Errorf("Unexpected error on initializing ProcessByFrequencyRatioWithCallback")
	}

	<-ctx.Done()
	<-processingDone

	totalDiff := 0.0
	channelNames := make([]string, 0, len(channelsWithExpectedRatios))
	for channelName := range channelsWithExpectedRatios {
		channelNames = append(channelNames, channelName)
	}
	sort.Strings(channelNames)

	for _, channelName := range channelNames {
		cwr := channelsWithExpectedRatios[channelName]
		actualRatio := float64(countPerChannel[channelName]) / float64(totalCount)
		diff := math.Abs(cwr.expectedRatio - actualRatio)
		diffPercentage := (diff / cwr.expectedRatio) * 100
		diffThreshold := 3.0
		if diffPercentage > diffThreshold && diff > 0.001 {
			t.Errorf("Unexpected Ratio: Channel %s: expected messages number by ratio %.5f, got %.5f (%.1f%%)",
				channelName, cwr.expectedRatio, actualRatio, diffPercentage)
		} else {
			t.Logf("Channel %s: expected messages number by ratio %.5f, got %.5f (%.1f%%)",
				channelName, cwr.expectedRatio, actualRatio, diffPercentage)
		}
		totalDiff += diff
	}
	t.Logf("Total diff: %.5f\n", totalDiff)
}
