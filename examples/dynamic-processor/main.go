package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmgrit/priority-channels"
	"github.com/dmgrit/priority-workers"
)

type channelStatsMessage struct {
	ChannelName string
	Message     string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inputChannels []chan string
	var triggerPauseChannels []chan bool
	var triggerCloseChannels []chan bool
	var totalSentMessages atomic.Int32
	var totalReceivedMessages atomic.Int32
	var totalDroppedMessages atomic.Int32

	channelsNum := 5
	for i := 1; i <= channelsNum; i++ {
		inputChannels = append(inputChannels, make(chan string))
		triggerPauseChannels = append(triggerPauseChannels, make(chan bool))
		triggerCloseChannels = append(triggerCloseChannels, make(chan bool))
	}
	channelsOrder := map[string]int{
		"Customer A - High Priority": 1,
		"Customer A - Low Priority":  2,
		"Customer B - High Priority": 3,
		"Customer B - Low Priority":  4,
		"Urgent Messages":            5,
	}

	priorityConfig := priority_workers.Configuration{
		PriorityWorkers: &priority_workers.PriorityWorkersConfig{
			Method: priority_workers.ByHighestAlwaysFirstMethodConfig,
			Channels: []priority_workers.ChannelConfig{
				{
					Name:     "Urgent Messages",
					Priority: 100,
				},
				{
					Name:     "Customer Messages",
					Priority: 1,
					PriorityWorkersConfig: &priority_workers.PriorityWorkersConfig{
						Method: priority_workers.ByFrequencyRatioMethodConfig,
						Channels: []priority_workers.ChannelConfig{
							{
								Name:      "Customer A",
								FreqRatio: 5,
								PriorityWorkersConfig: &priority_workers.PriorityWorkersConfig{
									Method: priority_workers.ByFrequencyRatioMethodConfig,
									Channels: []priority_workers.ChannelConfig{
										{
											Name:      "Customer A - High Priority",
											FreqRatio: 3,
										},
										{
											Name:      "Customer A - Low Priority",
											FreqRatio: 1,
										},
									},
								},
							},
							{
								Name:      "Customer B",
								FreqRatio: 1,
								PriorityWorkersConfig: &priority_workers.PriorityWorkersConfig{
									Method: priority_workers.ByFrequencyRatioMethodConfig,
									Channels: []priority_workers.ChannelConfig{
										{
											Name:      "Customer B - High Priority",
											FreqRatio: 3,
										},
										{
											Name:      "Customer B - Low Priority",
											FreqRatio: 1,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	channelNameToChannel := map[string]<-chan string{
		"Customer A - High Priority": inputChannels[0],
		"Customer A - Low Priority":  inputChannels[1],
		"Customer B - High Priority": inputChannels[2],
		"Customer B - Low Priority":  inputChannels[3],
		"Urgent Messages":            inputChannels[4],
	}

	receivedMsgs := 0
	byChannelName := make(map[string]int)
	var receivedMsgsMutex sync.Mutex
	var presentDetails atomic.Bool

	wp, err := priority_workers.NewDynamicPriorityProcessor(ctx, func(d priority_workers.Delivery[string]) {
		time.Sleep(100 * time.Millisecond)
		status := d.Status
		fullChannelPath := ""
		if presentDetails.Load() {
			for _, channelNode := range d.ReceiveDetails.PathInTree {
				fullChannelPath += fmt.Sprintf("%s [%d] -> ", channelNode.ChannelName, channelNode.ChannelIndex)
			}
			fullChannelPath = fullChannelPath + fmt.Sprintf("%s [%d]", d.ReceiveDetails.ChannelName, d.ReceiveDetails.ChannelIndex)
		} else {
			fullChannelPath = d.ReceiveDetails.ChannelName
		}
		if status == priority_channels.ReceiveSuccess {
			receivedMsgsMutex.Lock()
			receivedMsgs++
			totalReceivedMessages.Add(1)
			byChannelName[fullChannelPath] = byChannelName[fullChannelPath] + 1
			receivedMsgsMutex.Unlock()
		} else if status == priority_channels.ReceiveChannelClosed {
			fmt.Printf("Channel '%s' is closed\n", fullChannelPath)
		} else if status == priority_channels.ReceiveContextCancelled {
			if fullChannelPath == "" {
				fmt.Printf("Context is cancelled\n")
			} else {
				fmt.Printf("Context of channel '%s' is cancelled\n", fullChannelPath)
			}
		} else if status == priority_channels.ReceiveNoOpenChannels {
			if fullChannelPath == "" {
				fmt.Printf("No open channels left\n")
			} else {
				fmt.Printf("No open channels left for channel '%s'\n", fullChannelPath)
			}
		} else {
			fmt.Printf("Unexpected status %d for channel %s\n", status, fullChannelPath)
		}
	}, channelNameToChannel, priorityConfig, 3)
	if err != nil {
		fmt.Printf("failed to initialize dynamic priority processor: %v\n", err)
		os.Exit(1)
	}

	err = wp.Start()
	if err != nil {
		fmt.Printf("failed to start processing messages: %v\n", err)
		os.Exit(1)
	}

	demoFilePath := filepath.Join(os.TempDir(), "priority_channels_demo.txt")
	f, err := os.Create(demoFilePath)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		cancel()
		return
	}
	defer f.Close()

	fmt.Printf("Dynamic-Priority-Processor Multi-Hierarchy Demo:\n")
	fmt.Printf("- Press 'A/NA' to start/stop receiving messages from Customer A\n")
	fmt.Printf("- Press 'B/NB' to start/stop receiving messages from Customer B\n")
	fmt.Printf("- Press 'H/NH' to start/stop receiving high priority messages\n")
	fmt.Printf("- Press 'L/NL' to start/stop receiving low priority messages\n")
	fmt.Printf("- Press 'U/NU' to start/stop receiving urgent messages\n")
	fmt.Printf("- Press 'D/ND' to start/stop presenting receive path in tree\n")
	fmt.Printf("- Press 'w <workers_num>' to update number of workers in the worker pool\n")
	fmt.Printf("- Press 'load <priority_configuration_file>' to load a different priority configuration\n")
	fmt.Printf("- Press 'w' or 'workers' to see the number of worker goroutines configured in the worker pool\n")
	fmt.Printf("- Press 'active' to see the number of goroutines currently processing messages\n")
	fmt.Printf("- Press 'quit' to exit, waiting for worker goroutines to finish processing \n\n")
	fmt.Printf("- Press 'fq/force-quit' to exit, without waiting for worker goroutines to finish processing\n\n")
	fmt.Printf("To see the results live, run in another terminal window:\ntail -f %s\n\n", demoFilePath)

	for i := 1; i <= len(inputChannels); i++ {
		go func(i int) {
			paused := true
			closed := false
			j := 0
			for {
				j++
				select {
				case b := <-triggerPauseChannels[i-1]:
					paused = !b
				case b := <-triggerCloseChannels[i-1]:
					if b && !closed {
						close(inputChannels[i-1])
						closed = true
					}
				default:
					if !paused && !closed {
						select {
						case b := <-triggerPauseChannels[i-1]:
							paused = !b
						case b := <-triggerCloseChannels[i-1]:
							if b && !closed {
								close(inputChannels[i-1])
								closed = true
							}
						case inputChannels[i-1] <- fmt.Sprintf("message-%d", j):
							totalSentMessages.Add(1)
						}
					} else {
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
		}(i)
	}

	tickerCh := time.Tick(5 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tickerCh:
				receivedMsgsMutex.Lock()
				separatorLen := 80
				if presentDetails.Load() {
					separatorLen = 100
				}
				_, _ = f.WriteString(strings.Repeat("=", separatorLen) + "\n")
				if receivedMsgs > 0 {
					_, _ = f.WriteString(fmt.Sprintf("Received %d messages in the last 5 seconds (%.2f messages per second)\n", receivedMsgs, float64(receivedMsgs)/5))
					var messages []channelStatsMessage
					for name, val := range byChannelName {
						messages = append(messages, channelStatsMessage{
							ChannelName: name,
							Message:     fmt.Sprintf("%d messages (%.2f%%)", val, float64(val)/float64(receivedMsgs)*100),
						})
					}
					sort.Slice(messages, func(i, j int) bool {
						return channelsOrder[messages[i].ChannelName] < channelsOrder[messages[j].ChannelName]
					})
					for _, msg := range messages {
						_, _ = f.WriteString(fmt.Sprintf("%s: %s\n", msg.ChannelName, msg.Message))
					}
				} else {
					_, _ = f.WriteString("No messages received in the last 5 seconds\n")
					stopped, reason, channelName := wp.Status()
					if stopped {
						switch reason {
						case priority_channels.UnknownExitReason:
							_, _ = f.WriteString("Worker pool stopped: Unknown reason\n")
						case priority_channels.ChannelClosed:
							_, _ = f.WriteString(fmt.Sprintf("Worker pool stopped: Channel '%s' closed\n", channelName))
						case priority_channels.PriorityChannelClosed:
							_, _ = f.WriteString(fmt.Sprintf("Worker pool stopped: Priority Channel '%s' closed\n", channelName))
						case priority_channels.NoOpenChannels:
							_, _ = f.WriteString("Worker pool stopped: No open channels\n")
						case priority_channels.ContextCancelled:
							_, _ = f.WriteString("Worker pool stopped: Context cancelled\n")
						}
					}
				}
				_, _ = f.WriteString(strings.Repeat("=", separatorLen) + "\n")

				receivedMsgs = 0
				byChannelName = make(map[string]int)
				receivedMsgsMutex.Unlock()
			}
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	for {
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		upperLine := strings.ToUpper(line)
		value := !strings.HasPrefix(upperLine, "N")
		operation := "Started"
		if !value {
			operation = "Stopped"
		}
		if strings.HasPrefix(upperLine, "C") {
			upperLine = strings.TrimPrefix(upperLine, "C")
			number, err := strconv.Atoi(upperLine)
			if err != nil || number <= 0 || number > channelsNum {
				continue
			}
			fmt.Printf("Closing Channel %d\n", number)
			triggerCloseChannels[number-1] <- value
			continue
		}

		words := strings.Split(line, " ")
		if len(words) == 2 {
			switch words[0] {
			case "load":
				contents, err := os.ReadFile(words[1])
				if err != nil {
					fmt.Printf("failed to load file %s: %v\n", words[1], err)
					continue
				}

				var priorityConfig priority_workers.Configuration
				if err := json.Unmarshal(contents, &priorityConfig); err != nil {
					fmt.Printf("failed to unmarshal priority configuration: %v\n", err)
					continue
				}

				if err := wp.UpdatePriorityConfiguration(priorityConfig); err != nil {
					fmt.Printf("failed to update priority consumer configuration: %v\n", err)
					continue
				}
				fmt.Printf("Updated prioritization configuration\n")
			case "workers", "w":
				num, err := strconv.Atoi(words[1])
				if err != nil || num < 0 {
					fmt.Printf("second parameter to workers should be a non-negative number\n")
					continue
				}
				if err := wp.UpdateWorkersNum(num); err != nil {
					fmt.Printf("failed to update workers number: %v\n", err)
					continue
				}
				fmt.Printf("Updated workers number to %d\n", num)
			default:
				continue
			}
		}

		switch upperLine {
		case "A", "NA":
			triggerPauseChannels[0] <- value
			triggerPauseChannels[1] <- value
			fmt.Printf(operation + " receiving messages for Customer A\n")
		case "B", "NB":
			triggerPauseChannels[2] <- value
			triggerPauseChannels[3] <- value
			fmt.Printf(operation + " receiving messages for Customer B\n")
		case "H", "NH":
			triggerPauseChannels[0] <- value
			triggerPauseChannels[2] <- value
			fmt.Printf(operation + " receiving High Priority messages\n")
		case "L", "NL":
			triggerPauseChannels[1] <- value
			triggerPauseChannels[3] <- value
			fmt.Printf(operation + " receiving Low Priority messages\n")
		case "U", "NU":
			triggerPauseChannels[4] <- value
			fmt.Printf(operation + " receiving Urgent messages\n")
		case "D":
			presentDetails.Store(true)
			fmt.Printf("Presenting receive path on\n")
		case "ND":
			presentDetails.Store(false)
			fmt.Printf("Presenting receive path off\n")
		case "QUIT":
			fmt.Printf("Waiting for all workers to finish...\n")
			wp.StopGracefully()
			<-wp.Done()
			fmt.Printf("Processing finished\n")
			fmt.Printf("Total sent messages: %d\nTotal received messages: %d\n",
				totalSentMessages.Load(), totalReceivedMessages.Load())
			return
		case "FORCE-QUIT", "FQ":
			fmt.Printf("Waiting for all workers to finish...\n")
			wp.StopImmediately(func(msg string, details priority_channels.ReceiveDetails) {
				totalDroppedMessages.Add(1)
				fullChannelPath := ""
				for _, channelNode := range details.PathInTree {
					fullChannelPath += fmt.Sprintf("%s [%d] -> ", channelNode.ChannelName, channelNode.ChannelIndex)
				}
				fullChannelPath = fullChannelPath + fmt.Sprintf("%s [%d]", details.ChannelName, details.ChannelIndex)
				fmt.Printf("Message dropped from: %s\n", fullChannelPath)
			})
			<-wp.Done()
			for wp.ActiveWorkersNum() != 0 {
				fmt.Printf("Waiting for %d active workers to finish...\n", wp.ActiveWorkersNum())
				time.Sleep(100 * time.Millisecond)
			}
			fmt.Printf("Processing finished\n")
			fmt.Printf("Total sent messages: %d\n", totalSentMessages.Load())
			receivedMessages := totalReceivedMessages.Load()
			droppedMessages := totalDroppedMessages.Load()
			fmt.Printf("Total received messages: %d, Total dropped messages: %d, Total: %d\n",
				receivedMessages, droppedMessages, receivedMessages+droppedMessages)
			return
		case "WORKERS", "W":
			fmt.Printf("Workers number: %d\n", wp.WorkersNum())
		case "ACTIVE":
			fmt.Printf("Active workers number: %d\n", wp.ActiveWorkersNum())
		}
	}
}
