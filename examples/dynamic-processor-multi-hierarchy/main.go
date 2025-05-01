package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dmgrit/priority-channels"
	"github.com/dmgrit/priority-workers"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	var inputChannels []chan string
	var triggerPauseChannels []chan bool
	var triggerCloseChannels []chan bool

	channelsNum := 5
	for i := 1; i <= channelsNum; i++ {
		inputChannels = append(inputChannels, make(chan string))
		triggerPauseChannels = append(triggerPauseChannels, make(chan bool))
		triggerCloseChannels = append(triggerCloseChannels, make(chan bool))
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

	consumer, err := priority_workers.NewConsumer(ctx, channelNameToChannel, priorityConfig)
	//resCh, err := priority_workers.NewFromConfiguration[string](ctx, priorityConfig, channelNameToChannel)
	if err != nil {
		fmt.Printf("Unexpected error on consumer initialization: %v\n", err)
		return
	}
	resCh, err := consumer.Consume()
	if err != nil {
		fmt.Printf("Unexpected error on calling consume: %v\n", err)
		return
	}

	demoFilePath := filepath.Join(os.TempDir(), "priority_workers_demo.txt")

	fmt.Printf("Multi-Hierarchy Demo:\n")
	fmt.Printf("- Press 'A/NA' to start/stop receiving messages from Customer A\n")
	fmt.Printf("- Press 'B/NB' to start/stop receiving messages from Customer B\n")
	fmt.Printf("- Press 'H/NH' to start/stop receiving high priority messages\n")
	fmt.Printf("- Press 'L/NL' to start/stop receiving low priority messages\n")
	fmt.Printf("- Press 'U/NU' to start/stop receiving urgent messages\n")
	fmt.Printf("- Press 'D/ND' to start/stop presenting receive path in tree\n")
	fmt.Printf("- Press 0 to exit\n\n")
	fmt.Printf("To see the results live, run in another terminal window:\ntail -f %s\n\n", demoFilePath)

	for i := 1; i <= len(inputChannels); i++ {
		go func(i int) {
			paused := true
			closed := false
			for {
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
						case inputChannels[i-1] <- fmt.Sprintf("Channel %d", i):
						}
					} else {
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
		}(i)
	}

	var presentDetails atomic.Bool

	go func() {
		f, err := os.Create(demoFilePath)
		if err != nil {
			fmt.Printf("Failed to open file: %v\n", err)
			cancel()
			return
		}
		defer f.Close()
		prevFullChannelPath := ""
		streakLength := 0

		for msg := range resCh {
			details, status := msg.ReceiveDetails, msg.Status
			fullChannelPath := ""
			if presentDetails.Load() {
				for _, channelNode := range details.PathInTree {
					fullChannelPath += fmt.Sprintf("%s [%d] -> ", channelNode.ChannelName, channelNode.ChannelIndex)
				}
				fullChannelPath = fullChannelPath + fmt.Sprintf("%s [%d]", details.ChannelName, details.ChannelIndex)
			} else {
				fullChannelPath = details.ChannelName
			}
			if status == priority_channels.ReceiveSuccess {
				if fullChannelPath == prevFullChannelPath {
					streakLength++
				} else {
					streakLength = 1
				}
				prevFullChannelPath = fullChannelPath
				logMessage := fmt.Sprintf("%s (%d)\n", fullChannelPath, streakLength)

				_, err := f.WriteString(logMessage)
				if err != nil {
					fmt.Printf("Failed to write to file: %v\n", err)
					cancel()
					break
				}
			} else if status == priority_channels.ReceiveChannelClosed {
				_, err := f.WriteString(fmt.Sprintf("Channel '%s' is closed\n", fullChannelPath))
				if err != nil {
					fmt.Printf("Failed to write to file: %v\n", err)
					cancel()
					break
				}
			} else if status == priority_channels.ReceiveContextCancelled {
				var err error
				if fullChannelPath == "" {
					_, err = f.WriteString(fmt.Sprintf("Context is cancelled\n"))
				} else {
					_, err = f.WriteString(fmt.Sprintf("Context of channel '%s' is cancelled\n", fullChannelPath))
				}
				if err != nil {
					fmt.Printf("Failed to write to file: %v\n", err)
					cancel()
					break
				}
			} else if status == priority_channels.ReceiveNoOpenChannels {
				if fullChannelPath == "" {
					_, err = f.WriteString("No open channels left\n")
				} else {
					_, err = f.WriteString(fmt.Sprintf("No open channels left for channel '%s'\n", fullChannelPath))
				}
				if err != nil {
					fmt.Printf("Failed to write to file: %v\n", err)
					cancel()
					break
				}
			} else {
				_, err := f.WriteString(fmt.Sprintf("Unexpected status %s\n", fullChannelPath))
				if err != nil {
					fmt.Printf("Failed to write to file: %v\n", err)
					cancel()
					break
				}
			}

			if status != priority_channels.ReceiveSuccess &&
				status != priority_channels.ReceiveChannelClosed &&
				(status != priority_channels.ReceiveNoOpenChannels || fullChannelPath == "") &&
				(status != priority_channels.ReceiveContextCancelled || fullChannelPath == "") {
				_, err := f.WriteString("Exiting\n")
				if err != nil {
					fmt.Printf("Failed to write to file: %v\n", err)
					cancel()
					break
				}
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	for {
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		upperLine := strings.ToUpper(line)
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

				if err := consumer.UpdatePriorityConfiguration(priorityConfig); err != nil {
					fmt.Printf("failed to update priority consumer configuration: %v\n", err)
					continue
				}
				fmt.Printf("Updated prioritization configuration\n")
			}
			continue
		}

		value := !strings.HasPrefix(upperLine, "N")
		operation := "Started"
		if !value {
			operation = "Stopped"
		}
		if strings.HasPrefix(upperLine, "C") || strings.HasPrefix(upperLine, "FC") {
			shutdownMode := priority_workers.Graceful
			if strings.HasPrefix(upperLine, "FC") {
				shutdownMode = priority_workers.Force
			}
			switch strings.TrimPrefix(upperLine, "F") {
			//case "CA":
			//	fmt.Printf("Closing Priority Channel of Customer A\n")
			//	customerAChannel.Shutdown(shutdownMode, shutdownOptions...)
			//	continue
			//case "CB":
			//	fmt.Printf("Closing Priority Channel of Customer B\n")
			//	customerBChannel.Shutdown(shutdownMode, shutdownOptions...)
			//	continue
			//case "CU":
			//	fmt.Printf("Closing Priority Channel of Urgent Messages\n")
			//	urgentMessagesChannel.Shutdown(shutdownMode, shutdownOptions...)
			//	continue
			//case "CC":
			//	fmt.Printf("Closing Combined Priority Channel of Both Customers\n")
			//	combinedUsersAndMessageTypesChannel.Shutdown(shutdownMode, shutdownOptions...)
			//	continue
			case "CG":
				fmt.Printf("Closing Priority Channel\n")
				if shutdownMode == priority_workers.Force {
					consumer.StopImmediately(func(msg string, details priority_channels.ReceiveDetails) {
						fullChannelPath := ""
						for _, channelNode := range details.PathInTree {
							fullChannelPath += fmt.Sprintf("%s [%d] -> ", channelNode.ChannelName, channelNode.ChannelIndex)
						}
						fullChannelPath = fullChannelPath + fmt.Sprintf("%s [%d]", details.ChannelName, details.ChannelIndex)
						fmt.Printf("Message dropped from: %s\n", fullChannelPath)
					})
				} else {
					consumer.StopGracefully()
				}
				fmt.Printf("Waiting for processing to finished\n")
				<-consumer.Done()
				fmt.Printf("Processing is done\n")
				continue
			}
			upperLine = strings.TrimPrefix(upperLine, "C")
			number, err := strconv.Atoi(upperLine)
			if err != nil || number < 0 || number > channelsNum {
				continue
			}
			fmt.Printf("Closing Channel %d\n", number)
			triggerCloseChannels[number-1] <- value
			continue
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
		case "ND":
			presentDetails.Store(false)
		case "QUIT":
			fmt.Printf("Exiting\n")
			cancel()
			return
		}
	}
}
