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

func TestProcessMessagesOfCombinedPriorityChannelsByFrequencyRatio_RandomTree(t *testing.T) {
	var testCases = []struct {
		Name string
	}{
		{
			Name: "ByGoroutines",
		},
	}

	freqRatioTree := generateRandomFreqRatioTree(t, 3, 5, 5)

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			testProcessMessagesOfCombinedChannelsByFrequencyRatio_RandomTree(t,
				freqRatioTree,
				nil,
				nil,
				false)
		})
	}
}

func TestProcessMessagesOfCombinedPriorityChannelsByFrequencyRatio_RandomTreeSubset(t *testing.T) {
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

	freqRatioTree := generateRandomFreqRatioTree(t, 3, 5, 5)

	channelIndexesSubset := getRandomChannelIndexesSubset(freqRatioTree.totalChannelsNum())
	var recomputedChannelsFreqRatios map[int]float64
	if channelIndexesSubset != nil {
		truncatedTree, _ := duplicateFreqRatioTreeExceptNotIncludedChannelIndexes(freqRatioTree, channelIndexesSubset)
		recomputedChannelsFreqRatios = make(map[int]float64)
		recomputeFreqRatioTreeExpectedRatios(truncatedTree, channelIndexesSubset, recomputedChannelsFreqRatios)
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			testProcessMessagesOfCombinedChannelsByFrequencyRatio_RandomTree(t,
				freqRatioTree,
				recomputedChannelsFreqRatios,
				channelIndexesSubset,
				tc.CloseChannels)
		})
	}
}

func getRandomChannelIndexesSubset(channelsNum int) map[int]struct{} {
	allChannelIndexes := make([]int, 0, channelsNum)
	for i := 0; i < channelsNum; i++ {
		allChannelIndexes = append(allChannelIndexes, i)
	}
	rand.Shuffle(channelsNum, func(i, j int) {
		allChannelIndexes[i], allChannelIndexes[j] = allChannelIndexes[j], allChannelIndexes[i]
	})
	subsetSize := rand.IntN(channelsNum) + 1
	if subsetSize == 1 {
		subsetSize = 2
	}
	channelIndexesSubset := make(map[int]struct{})
	for _, i := range allChannelIndexes[:subsetSize] {
		channelIndexesSubset[i] = struct{}{}
	}
	return channelIndexesSubset
}

func testProcessMessagesOfCombinedChannelsByFrequencyRatio_RandomTree(t *testing.T,
	freqRatioTree *freqRatioTreeNode,
	recomputedChannelsFrequencyRatios map[int]float64,
	channelIndexesSubset map[int]struct{},
	closeChannels bool) {
	ctx, cancel := context.WithCancel(context.Background())

	childResultChannels, channelsWithExpectedRatios := getResultChannelsToProcessFromFreqRatioTree(t, ctx, freqRatioTree)
	if recomputedChannelsFrequencyRatios != nil {
		for _, c := range channelsWithExpectedRatios {
			c.expectedRatio = recomputedChannelsFrequencyRatios[c.channelIndex]
		}
	}

	if len(channelIndexesSubset) > 0 {
		t.Logf("Taking subset of %d channels out of %d\n", len(channelIndexesSubset), len(channelsWithExpectedRatios))
		for channelName, cwr := range channelsWithExpectedRatios {
			if _, ok := channelIndexesSubset[cwr.channelIndex]; !ok {
				delete(channelsWithExpectedRatios, channelName)
				if closeChannels {
					close(cwr.channel)
				}
			}
		}
	}

	messagesNum := 10000
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

	_, err := priority_workers.CombineByFrequencyRatioWithCallback(ctx, childResultChannels, func(result priority_workers.ReceiveResult[string]) {
		if result.Status != priority_channels.ReceiveSuccess {
			if result.Status == priority_channels.ReceiveContextCancelled {
				processingDone <- struct{}{}
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
		var reachedTotalCount bool
		mtx.Lock()
		totalCount++
		countPerChannel[result.ChannelName] = countPerChannel[result.ChannelName] + 1
		reachedTotalCount = totalCount == messagesNum
		mtx.Unlock()
		if reachedTotalCount {
			cancel()
			return
		}
	})
	if err != nil {
		t.Fatalf("Unexpected error on initializing processing CombineByFrequencyRatioWithCallback: %v", err)
	}

	<-ctx.Done()
	<-processingDone

	channelNames := make([]string, 0, len(channelsWithExpectedRatios))
	for channelName := range channelsWithExpectedRatios {
		channelNames = append(channelNames, channelName)
	}
	sort.Strings(channelNames)

	totalDiff := 0.0
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

func getResultChannelsToProcessFromFreqRatioTree(t *testing.T, ctx context.Context, root *freqRatioTreeNode) (
	[]priority_workers.ResultChannelWithFreqRatio[string], map[string]*channelWithExpectedRatio) {
	channelsWithExpectedRatios := make(map[string]*channelWithExpectedRatio)
	startTime := time.Now()
	_, _, childPriorityChannels := doGenerateGoRoutinesPriorityChannelTreeFromFreqRatioTree(t, ctx, root, channelsWithExpectedRatios)
	t.Logf("Time to generate priority channel tree: %v\n", time.Since(startTime))
	sumOfAllChannels := 0.0
	for _, cwr := range channelsWithExpectedRatios {
		sumOfAllChannels += cwr.expectedRatio
	}
	if math.Abs(sumOfAllChannels-1.0) > 0.0001 {
		t.Fatalf("Expected sum of all priority channels to be %.4f, got %.4f\n", 1.0, sumOfAllChannels)
	}
	return childPriorityChannels, channelsWithExpectedRatios
}

func doGenerateGoRoutinesPriorityChannelTreeFromFreqRatioTree(t *testing.T, ctx context.Context,
	node *freqRatioTreeNode,
	channelsWithExpectedRatios map[string]*channelWithExpectedRatio) (<-chan priority_workers.ReceiveResult[string], priority_workers.ShutdownFunc, []priority_workers.ResultChannelWithFreqRatio[string]) {
	if len(node.Children) == 0 {
		cwr := &channelWithExpectedRatio{
			channel:       make(chan string, 10),
			expectedRatio: node.ExpectedRatio,
			channelIndex:  node.ChannelIndex,
		}
		channelName := fmt.Sprintf("channel-%d-%s", node.Level, node.Label)
		if _, ok := channelsWithExpectedRatios[channelName]; ok {
			t.Fatalf("Duplicate channel name: %s", channelName)
		}
		channelsWithExpectedRatios[channelName] = cwr
		processingChan, processingChanCancelFunc, err := priority_workers.ProcessChannel(ctx, channelName, cwr.channel)
		if err != nil {
			t.Fatalf("Unexpected error on initializing ProcessChannel: %v", err)
		}
		return processingChan, processingChanCancelFunc, nil
	}
	if node.Level == 1 {
		channelsWithFreqRatio := make([]channels.ChannelWithFreqRatio[string], 0, len(node.Children))
		for _, child := range node.Children {
			cwr := &channelWithExpectedRatio{
				channel:       make(chan string, 10),
				expectedRatio: child.ExpectedRatio,
				channelIndex:  child.ChannelIndex,
			}
			childName := fmt.Sprintf("channel-%d-%s", child.Level, child.Label)
			if _, ok := channelsWithExpectedRatios[childName]; ok {
				t.Fatalf("Duplicate channel name: %s", childName)
			}
			channelsWithExpectedRatios[childName] = cwr
			childChannel := channels.NewChannelWithFreqRatio(childName, cwr.channel, child.Weight)
			channelsWithFreqRatio = append(channelsWithFreqRatio, childChannel)
		}
		processingChan, processingChanShutdownFunc, err := priority_workers.ProcessByFrequencyRatio(ctx, channelsWithFreqRatio)
		if err != nil {
			t.Fatalf("Unexpected error on intializing ProcessByFrequencyRatio: %v", err)
		}
		return processingChan, processingChanShutdownFunc, nil
	}
	priorityChannelsWithFreqRatio := make([]priority_workers.ResultChannelWithFreqRatio[string], 0, len(node.Children))
	for _, child := range node.Children {
		childCh, childChShutdownFunc, _ := doGenerateGoRoutinesPriorityChannelTreeFromFreqRatioTree(t, ctx, child, channelsWithExpectedRatios)
		childName := fmt.Sprintf("priority-channel-%d-%s", child.Level, child.Label)
		priorityChannelsWithFreqRatio = append(priorityChannelsWithFreqRatio, priority_workers.NewResultChannelWithFreqRatio(childName, childCh, childChShutdownFunc, child.Weight))
	}
	processingChan, processingChanShutdownFunc, err := priority_workers.CombineByFrequencyRatio(ctx, priorityChannelsWithFreqRatio)
	if err != nil {
		t.Fatalf("Unexpected error on initializing CombineByFrequencyRatio: %v", err)
	}
	return processingChan, processingChanShutdownFunc, priorityChannelsWithFreqRatio
}

func generateRandomFreqRatioTree(t *testing.T, maxLevelNum int, maxChildrenNum int, maxWeight int) *freqRatioTreeNode {
	levelsNum := rand.N(maxLevelNum) + 1
	if levelsNum == 1 {
		levelsNum = 2
	}
	childrenNum := rand.N(maxChildrenNum) + 1
	if childrenNum == 1 {
		childrenNum = 2
	}
	totalSum := 0.0
	weights := make([]int, 0, childrenNum)
	for i := 0; i < childrenNum; i++ {
		w := rand.N(maxWeight) + 1
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

	children := make([]*freqRatioTreeNode, 0, childrenNum)
	currChannelIndex := 0
	for i := 0; i < childrenNum; i++ {
		childLabel := fmt.Sprintf("%d", i)
		childNode := generateRandomFreqRatioSubtree(levelsNum-1, childLabel, weights[i], expectedRatios[i], maxChildrenNum, maxWeight, &currChannelIndex)
		children = append(children, childNode)
	}
	return &freqRatioTreeNode{
		Level:         levelsNum,
		Label:         "0",
		ExpectedRatio: 1.0,
		Children:      children,
	}
}

func generateRandomFreqRatioSubtree(currLevel int, currLabel string, weight int, currExpectedRatio float64,
	maxChildrenNum int, maxWeight int, currChannelIndex *int) *freqRatioTreeNode {
	childrenNum := rand.N(maxChildrenNum) + 1
	if childrenNum == 1 {
		return generateRandomFreqRatioTreeLeafNode(currLevel,
			fmt.Sprintf("%s-%d", currLabel, 0), weight, currExpectedRatio, currChannelIndex)
	}

	totalSum := 0.0
	weights := make([]int, 0, childrenNum)
	for i := 0; i < childrenNum; i++ {
		w := rand.N(maxWeight) + 1
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

	children := make([]*freqRatioTreeNode, 0, childrenNum)
	for i := 0; i < childrenNum; i++ {
		var childNode *freqRatioTreeNode
		childLabel := fmt.Sprintf("%s-%d", currLabel, i)
		expectedRatio := currExpectedRatio * expectedRatios[i]
		if currLevel == 1 {
			childNode = generateRandomFreqRatioTreeLeafNode(currLevel-1, childLabel, weights[i], expectedRatio, currChannelIndex)
		} else {
			childNode = generateRandomFreqRatioSubtree(currLevel-1, childLabel, weights[i], expectedRatio, maxChildrenNum, maxWeight, currChannelIndex)
		}
		children = append(children, childNode)
	}
	return &freqRatioTreeNode{
		Level:         currLevel,
		Weight:        weight,
		Label:         currLabel,
		ExpectedRatio: currExpectedRatio,
		Children:      children,
	}
}

func generateRandomFreqRatioTreeLeafNode(currLevel int, currLabel string, weight int, currExpectedRatio float64, channelIndex *int) *freqRatioTreeNode {
	currChannelIndex := *channelIndex
	*channelIndex = *channelIndex + 1
	return &freqRatioTreeNode{
		Level:         currLevel,
		Label:         currLabel,
		Weight:        weight,
		ExpectedRatio: currExpectedRatio,
		ChannelIndex:  currChannelIndex,
	}
}

func recomputeFreqRatioTreeExpectedRatios(node *freqRatioTreeNode, channelIndexesSubset map[int]struct{}, recomputedExpectedFreqRatios map[int]float64) {
	node.ExpectedRatio = 1.0
	doRecomputeFreqRatioTreeExpectedRatios(node, channelIndexesSubset, recomputedExpectedFreqRatios)
}

func doRecomputeFreqRatioTreeExpectedRatios(node *freqRatioTreeNode, channelIndexesSubset map[int]struct{}, recomputedExpectedFreqRatios map[int]float64) {
	totalSum := 0.0
	for _, c := range node.Children {
		isLeafNode := len(c.Children) == 0
		_, channelIndexIncluded := channelIndexesSubset[c.ChannelIndex]
		if isLeafNode && !channelIndexIncluded {
			continue
		}
		totalSum += float64(c.Weight)
	}
	for _, c := range node.Children {
		isLeafNode := len(c.Children) == 0
		_, channelIndexIncluded := channelIndexesSubset[c.ChannelIndex]
		if isLeafNode && !channelIndexIncluded {
			recomputedExpectedFreqRatios[c.ChannelIndex] = c.ExpectedRatio
			c.ExpectedRatio = 0
			continue
		}
		c.ExpectedRatio = (float64(c.Weight) / totalSum) * node.ExpectedRatio
		if isLeafNode {
			recomputedExpectedFreqRatios[c.ChannelIndex] = c.ExpectedRatio
		}
		doRecomputeFreqRatioTreeExpectedRatios(c, channelIndexesSubset, recomputedExpectedFreqRatios)
	}
}

func duplicateFreqRatioTreeExceptNotIncludedChannelIndexes(node *freqRatioTreeNode, channelIndexesSubset map[int]struct{}) (*freqRatioTreeNode, bool) {
	newNode := &freqRatioTreeNode{
		Level:        node.Level,
		Label:        node.Label,
		Weight:       node.Weight,
		ChannelIndex: node.ChannelIndex,
	}
	if len(node.Children) == 0 {
		return newNode, true
	}
	for _, c := range node.Children {
		isLeafNode := len(c.Children) == 0
		_, channelIndexIncluded := channelIndexesSubset[c.ChannelIndex]
		if isLeafNode && !channelIndexIncluded {
			continue
		}
		newChild, nonEmptySubtree := duplicateFreqRatioTreeExceptNotIncludedChannelIndexes(c, channelIndexesSubset)
		if nonEmptySubtree {
			newNode.Children = append(newNode.Children, newChild)
		}
	}
	return newNode, len(newNode.Children) > 0
}

func (n *freqRatioTreeNode) totalChannelsNum() int {
	if len(n.Children) == 0 {
		return 1
	}
	total := 0
	for _, c := range n.Children {
		total += c.totalChannelsNum()
	}
	return total
}

type freqRatioTreeNode struct {
	Level         int
	Label         string
	Children      []*freqRatioTreeNode
	Weight        int
	ExpectedRatio float64
	ChannelIndex  int
}

type channelWithExpectedRatio struct {
	channel       chan string
	expectedRatio float64
	channelIndex  int
}
