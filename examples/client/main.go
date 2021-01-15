// Copyright 2019 Barchart.com, Inc. All rights reserved.
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
	"time"

	openfeed "github.com/openfeed-org/sdk-go"
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

	err := conn.Start()
	log.Printf("Error on start. %v", err)
}
