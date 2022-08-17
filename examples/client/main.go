// Copyright 2019 - 2021 Barchart.com, Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Client example program

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/openfeed-org/sdk-go/openfeed"
	"google.golang.org/protobuf/proto"
)

type MessageHandlerStats struct {
	HeartBeat            uint64
	InstrumentDefinition uint64
	MarketSnapshot       uint64
	MarketUpdate         uint64
	OHLC                 uint64
	SubscriptionResponse uint64
	Unknown              uint64
}

type ExampleMessageHandler struct {
	li, ls, lu, lo bool
	stats          MessageHandlerStats
}

func (hnd *ExampleMessageHandler) NewHeartbeat(msg *openfeed.HeartBeat) {
	hnd.stats.HeartBeat++
	t := time.Unix(0, msg.GetTransactionTime())
	log.Printf("HB\t%v msg: %v", t, msg)
}

func (hnd *ExampleMessageHandler) NewMessage(msg *openfeed.Message) {
	switch msg.MessageType {
	case openfeed.MessageType_HEARTBEAT:
		// Logged in HB handler
	case openfeed.MessageType_INSTRUMENT_DEFINITION:
		hnd.stats.InstrumentDefinition++
		if hnd.li {
			log.Printf("INST DEF: %v", msg.Message)
		}
	case openfeed.MessageType_OHLC:
		hnd.stats.OHLC++
		if hnd.lo {
			log.Printf("OHLC: %v", msg.Message)
		}
	case openfeed.MessageType_MARKET_SNAPSHOT:
		hnd.stats.MarketSnapshot++
		if hnd.ls {
			log.Printf("MKT SNAP: %v", msg.Message)
		}
	case openfeed.MessageType_MARKET_UPDATE:
		hnd.stats.MarketUpdate++
		if hnd.lu {
			log.Printf("MKT UPD: %v", msg.Message)
		}
	case openfeed.MessageType_INSTRUMENT_RESPONSE:
		log.Printf("INST RSP: %v", msg.Message)
	case openfeed.MessageType_SUBSCRIPTION_RESPONSE:
		hnd.stats.SubscriptionResponse++
		log.Print("SUB RESP: %v", msg.Message)
	default:
		hnd.stats.Unknown++
		log.Printf("Unhandled message type: %d msg: %v", msg.MessageType, msg.Message)
	}
}

func (hnd ExampleMessageHandler) Stats() MessageHandlerStats {
	return hnd.stats
}

type AllMessagesHandler struct {
}

func (hnd *AllMessagesHandler) NewMessage(msg *openfeed.Message) {
	log.Printf("MSG\t%v", msg)
}

const usage = `Usage:
ofclient [flags] symbols|exchanges

Flags:
-u	Required. Username
-p	Required. Password
-s	Optional. Default is openfeed.aws.barchart.com
-e	Optional. Change to exchanges mode
-t	Optional. Default is q. Subscription type. Values include:
		o OHLC
		q Quotes (includes trades)
-li  Optional. Log Instruments
-ls  Optional. Log Snapshots
-lu  Optional. Log Market Updates
-lo  Optional. Log OHLC
`

func main() {
	log.SetOutput(os.Stdout)

	username := flag.String("u", "", "The username")
	password := flag.String("p", "", "The password")
	server := flag.String("s", "openfeed.aws.barchart.com", "The server")
	li := flag.Bool("li", false, "Log Instruments")
	ls := flag.Bool("ls", false, "Log Snapshots")
	lu := flag.Bool("lu", false, "Log Market Updates")
	lo := flag.Bool("lo", false, "Log OHLC")
	exchange := flag.Bool("e", false, "Exchanges mode.")
	subscriptions := flag.String("t", "q", "Quotes.")
	mode := flag.String("m", "quotes", "Mode: [quotes | def]")

	flag.Parse()

	if *username == "" || *password == "" || flag.Arg(0) == "" {
		log.Fatalln(usage)
	}

	log.Printf("Using %s/%s connecting to %s\n", *username, *password, *server)
	conn := openfeed.NewConnection(openfeed.Credentials{
		Username: *username,
		Password: *password,
	}, *server)

	var messageHandler = ExampleMessageHandler{}
	messageHandler.li = *li
	messageHandler.ls = *ls
	messageHandler.lu = *lu
	messageHandler.lo = *lo

	go func() {
		for {
			log.Printf("Stats: Total: %d, hb: %d id: %d, ms: %d, mu: %d, ohlc: %d, sr: %d, unk: %d", (messageHandler.Stats().HeartBeat + messageHandler.Stats().InstrumentDefinition + messageHandler.Stats().MarketSnapshot + messageHandler.Stats().MarketUpdate + messageHandler.Stats().OHLC + messageHandler.Stats().SubscriptionResponse + messageHandler.Stats().Unknown),
				messageHandler.Stats().HeartBeat, messageHandler.Stats().InstrumentDefinition, messageHandler.Stats().MarketSnapshot, messageHandler.Stats().MarketUpdate, messageHandler.Stats().OHLC, messageHandler.Stats().SubscriptionResponse, messageHandler.Stats().Unknown)
			time.Sleep(5 * time.Second)
		}
	}()

	hnd := openfeed.MessageHandler(&messageHandler)

	switch *mode {
	case "def":
		err := conn.Connect()
		if err != nil {
			log.Printf("error connecting %v", err)
			return
		}

		_, err = conn.Login()
		if err != nil {
			log.Printf("error logging in %v", err)
			return
		}

		req := conn.CreateInstrumentRequestByExchange(flag.Arg(0))

		fmt.Println("ADD", req)

		if req != nil {
			ba, _ := proto.Marshal(req)
			conn.Socket().WriteMessage(2, ba)
		}

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			defer wg.Done()

			hbCount := 0
			for {
				// There's a server sent heartbeat every 10 seconds
				conn.Socket().SetReadDeadline(time.Now().Add(15 * time.Second))
				_, message, err := conn.Socket().ReadMessage()
				if err != nil {
					log.Printf("error reading %v", err)
					return
				}

				var ofmsg openfeed.OpenfeedGatewayMessage
				err = proto.Unmarshal(message, &ofmsg)
				if err != nil {
					log.Printf("of: unable to unmarshal gateway message. %v", err)

				} else {
					switch ofmsg.Data.(type) {
					case *openfeed.OpenfeedGatewayMessage_HeartBeat:
						hbCount++
						if hbCount >= 2 {
							// No more messages
							log.Printf("received %d heartbeats in a row, no more messages. Exiting.", hbCount)
							return
						}
					case *openfeed.OpenfeedGatewayMessage_InstrumentDefinition:
						fmt.Println(ofmsg.GetInstrumentDefinition())
					case *openfeed.OpenfeedGatewayMessage_LogoutResponse:
						lr := ofmsg.GetLogoutResponse()

						if lr.GetStatus().GetResult() == openfeed.Result_DUPLICATE_LOGIN {
							log.Printf("Disconnected due to duplicate login. Terminating retries.")
							return
						}
					default:
						log.Printf("unhandled messge %v", ofmsg)
					}
				}
			}
		}()

		wg.Wait()
	case "quotes":
		if *exchange {
			conn.AddExchangeSubscription(strings.Split(flag.Arg(0), ","), &hnd)
		} else {
			for _, c := range *subscriptions {
				s := strings.ToUpper(string(c))
				switch s {
				case "O":
					// log.Printf("Adding OHLC Request")
					conn.AddSymbolOHLCSubscription(strings.Split(flag.Arg(0), ","), &hnd)
				case "Q":
					// log.Printf("Adding SUBSCRIPTION Request")
					conn.AddSymbolSubscription(strings.Split(flag.Arg(0), ","), &hnd)
				default:
					log.Printf("Unknown subscription %s", s)
					log.Fatalf(usage)
				}
			}
		}
	default:
	}

	// Call on heartbeats
	hb := openfeed.HeartbeatHandler(&messageHandler)
	conn.AddHeartbeatSubscription(&hb)

	// Adds a messge handler for all messages
	// allHnd := AllMessagesHandler{}
	// h := openfeed.MessageHandler(&allHnd)
	// conn.AddMessageSubscription(&h)

	err := conn.Start()
	if err == nil {
		log.Printf("shutting down")
	} else {
		log.Printf("Error on start. error: %v", err)
	}
}
