// Copyright 2019 - 2021 Barchart.com, Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Client example program

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/openfeed-org/sdk-go/openfeed"
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
	t := time.Unix(0, msg.GetTransactionTime())
	log.Printf("HB\t%v msg: %v", t, msg)
}

func (hnd *ExampleMessageHandler) NewMessage(msg *openfeed.Message) {
	switch msg.MessageType {
	case openfeed.MessageType_HEARTBEAT:
		hnd.stats.HeartBeat++
		// Logged in HB handler
	case openfeed.MessageType_INSTRUMENT_DEFINITION:
		hnd.stats.InstrumentDefinition++
		if hnd.li {
			fmt.Println("INST DEF", msg.Message)
		}
	case openfeed.MessageType_OHLC:
		hnd.stats.OHLC++
		if hnd.lo {
			fmt.Println("OHLC", msg.Message)
		}
	case openfeed.MessageType_MARKET_SNAPSHOT:
		hnd.stats.MarketSnapshot++
		if hnd.ls {
			fmt.Println("MKT SNAP", msg.Message)
		}
	case openfeed.MessageType_MARKET_UPDATE:
		hnd.stats.MarketUpdate++
		if hnd.lu {
			fmt.Println("MKT UPD", msg.Message)
		}
	case openfeed.MessageType_SUBSCRIPTION_RESPONSE:
		hnd.stats.SubscriptionResponse++
		fmt.Println("SUB RESP", msg.Message)
	default:
		hnd.stats.Unknown++
		fmt.Println("Unhandled message type", msg.MessageType)
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

type BIMSResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"tokern_type"`
}

func GetBIMSToken(server, username, password string) (BIMSResponse, error) {
	// Easy JWT getter. No external libs used.
	sb := bytes.NewBufferString(fmt.Sprintf("{\"username\":\"%s\",\"password\":\"%s\",\"domain\":\"realtime\"}", username, password))
	resp, err := http.Post("https://"+server+"/authenticate", "application/json", sb)
	if err != nil {
		return BIMSResponse{}, err
	}

	ba, err := io.ReadAll(resp.Body)
	if err != nil {
		return BIMSResponse{}, err
	}

	var bimsresp BIMSResponse
	err = json.Unmarshal(ba, &bimsresp)
	if err != nil {
		return BIMSResponse{}, err
	}

	return bimsresp, nil
}

const usage = `Usage:
ofclient [flags] symbols|exchanges

Flags:
-u      Required. Username
-p      Required. Password
-bims   Optional. BIMS server, token mode
-s      Optional. Default is openfeed.aws.barchart.com
-e      Optional. Change to exchanges mode
-t      Optional. Default is q. Subscription type. Values include:
            o OHLC
            q Quotes (includes trades)
-li     Optional. Log Instruments
-ls     Optional. Log Snapshots
-lu     Optional. Log Market Updates
-lo     Optional. Log OHLC
`

func main() {
	log.SetOutput(os.Stdout)

	username := flag.String("u", "", "The username")
	password := flag.String("p", "", "The password")
	server := flag.String("s", "openfeed.aws.barchart.com", "The server")
	bims := flag.String("bims", "", "Use BIMS server, which changes connectivity to token-stle login")
	li := flag.Bool("li", false, "Log Instruments")
	ls := flag.Bool("ls", false, "Log Snapshots")
	lu := flag.Bool("lu", false, "Log Market Updates")
	lo := flag.Bool("lo", false, "Log OHLC")
	exchange := flag.Bool("e", false, "Exchanges mode.")
	subscriptions := flag.String("t", "q", "Quotes.")

	flag.Parse()

	if *username == "" || *password == "" || flag.Arg(0) == "" {
		log.Fatalln(usage)
	}

	creds := openfeed.Credentials{}
	if *bims != "" {
		log.Printf("Using BIMS tokens %s", *bims)
		b, err := GetBIMSToken(*bims, *username, *password)
		if err != nil {
			log.Printf("failed to get BIMS token %v", err)
			return
		}
		creds.AccessToken = b.AccessToken
	} else {
		creds.Username = *username
		creds.Password = *password
	}
	fmt.Println("creds", creds)

	log.Printf("Using %s/%s connecting to %s\n", *username, *password, *server)
	conn := openfeed.NewConnection(creds, *server)

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
		log.Printf("Message Count Routine exiting...")
	}()

	hnd := openfeed.MessageHandler(&messageHandler)

	if *exchange {
		log.Printf("Adding Exchange Request")
		conn.AddExchangeSubscription(strings.Split(flag.Arg(0), ","), &hnd)
	} else {
		for _, c := range *subscriptions {
			s := strings.ToUpper(string(c))
			switch s {
			case "O":
				log.Printf("Adding OHLC Request")
				conn.AddSymbolOHLCSubscription(strings.Split(flag.Arg(0), ","), &hnd)
			case "Q":
				log.Printf("Adding SUBSCRIPTION Request")
				conn.AddSymbolSubscription(strings.Split(flag.Arg(0), ","), &hnd)
			default:
				log.Printf("Unknown subscription %s", s)
				log.Fatalf(usage)
			}
		}
	}

	// Call on heartbeats
	hb := openfeed.HeartbeatHandler(&messageHandler)
	conn.AddHeartbeatSubscription(&hb)

	// Add a messge handler for all messages
	allHnd := AllMessagesHandler{}
	h := openfeed.MessageHandler(&allHnd)
	conn.AddMessageSubscription(&h)

	err := conn.Start()
	if err == nil {
		log.Printf("shutting down")
	} else {
		log.Printf("Error on start. error: %v", err)
	}
}
