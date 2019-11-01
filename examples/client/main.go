// Copyright 2019 Barchart.com, Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Client example program

package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	openfeed "github.com/openfeed-org/sdk-go"
)

func main() {
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
		fmt.Println("MSG", msg)
	})

	conn.Start()
}
