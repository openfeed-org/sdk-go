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

func messageHandler(msg openfeed.Message) {
	switch msg.MessageType {
	case openfeed.MessageType_INSTRUMENT_DEFINITION:
		fmt.Println("INST DEF", msg.Message)
	case openfeed.MessageType_OHLC:
		fmt.Println("OHLC", msg.Message)
	case openfeed.MessageType_MARKET_SNAPSHOT:
		fmt.Println("MKT SNAP", msg.Message)
	case openfeed.MessageType_MARKET_UPDATE:
		fmt.Println("MKT UPD", msg.Message)
	case openfeed.MessageType_SUBSCRIPTION_RESPONSE:
		fmt.Println("SUB RESP", msg.Message)

	default:
		fmt.Println("Unhandled message type", msg.MessageType)
	}
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
`

func main() {
	log.SetOutput(os.Stdout)

	username := flag.String("u", "", "The username")
	password := flag.String("p", "", "The password")
	server := flag.String("s", "openfeed.aws.barchart.com", "The server")
	exchange := flag.Bool("e", false, "Exchanges mode.")
	subscriptions := flag.String("t", "q", "Quotes.")
	mode := flag.String("m", "quotes", "Mode.")

	flag.Parse()

	if *username == "" || *password == "" || flag.Arg(0) == "" {
		log.Fatalln(usage)
	}

	log.Printf("Using %s/%s connecting to %s\n", *username, *password, *server)
	conn := openfeed.NewConnection(openfeed.Credentials{
		Username: *username,
		Password: *password,
	}, *server)

	defer conn.Close()

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
			conn.AddExchangeSubscription(strings.Split(flag.Arg(0), ","), messageHandler)
		} else {
			for _, c := range *subscriptions {
				s := strings.ToUpper(string(c))
				switch s {
				case "O":
					// log.Printf("Adding OHLC Request")
					conn.AddSymbolOHLCSubscription(strings.Split(flag.Arg(0), ","), messageHandler)
				case "Q":
					// log.Printf("Adding SUBSCRIPTION Request")
					conn.AddSymbolSubscription(strings.Split(flag.Arg(0), ","), messageHandler)
				default:
					log.Printf("Unknown subscription %s", s)
					log.Fatalf(usage)
				}
			}
		}

		conn.AddHeartbeatSubscription(func(msg *openfeed.HeartBeat) {
			t := time.Unix(0, msg.GetTransactionTime())
			log.Printf("HEARTBEAT\t%v", t)
		})

		conn.AddMessageSubscription(func(msg *openfeed.Message) {
			log.Printf("MSG: %v", msg)
		})

		err := conn.Start()
		if err == nil {
			log.Printf("shutting down")
		} else {
			log.Printf("Error on start. %v", err)
		}
	}
}
