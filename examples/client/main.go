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

func main() {

	log.SetOutput(os.Stdout)

	username := flag.String("username", "", "The username")
	password := flag.String("password", "", "The password")
	server := flag.String("server", "", "The server")
	exchange := flag.Bool("exchange", false, "Exchanges mode.")

	flag.Parse()

	conn := openfeed.NewConnection(openfeed.Credentials{
		Username: *username,
		Password: *password,
	}, *server)

	defer conn.Close()

	if *exchange {
		conn.AddExchangeSubscription(strings.Split(flag.Arg(0), ","), messageHandler)
	} else {
		conn.AddSymbolSubscription(strings.Split(flag.Arg(0), ","), messageHandler)
	}

	conn.AddHeartbeatSubscription(func(msg *openfeed.HeartBeat) {
		t := time.Unix(0, msg.GetTransactionTime())
		fmt.Println("HEARTBEAT", t)
	})

	err := conn.Start()
	log.Printf("Error on start. %v", err)
}
