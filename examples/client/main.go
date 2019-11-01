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

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(0)

	username := flag.String("username", "", "The username")
	password := flag.String("password", "", "The password")
	server := flag.String("server", "", "The server")

	flag.Parse()

	conn, err := openfeed.Connect(openfeed.Credentials{
		Username: *username,
		Password: *password,
	}, *server)

	if err != nil {
		log.Printf("Error connecting to %s. %v", *server, err)
		return
	}

	defer conn.Close()

	b, err := conn.Login()
	if !b || err != nil {
		log.Printf("Error Logging in. %v", err)
		return
	}

	conn.AddSymbolSubscription(strings.Split(flag.Arg(0), ","), func(msg openfeed.Message) {
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
	})

	conn.AddHeartbeatSubscription(func(msg *openfeed.HeartBeat) {
		t := time.Unix(0, msg.GetTransactionTime())
		fmt.Println("HEARTBEAT", t)
	})

	conn.Start()
}
