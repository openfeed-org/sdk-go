// Copyright 2019 Barchart.com, Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package openfeed implements openfeed API.

package openfeed

import (
	"errors"
	"fmt"
	"log"
	"net/url"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
)

type Credentials struct {
	Username string
	Password string
}

type Connection struct {
	credentials   *Credentials
	connection    *websocket.Conn
	loginResponse *LoginResponse
}

var (
	symbolHandlers  = make(map[string][]func(Message))
	asyncReadActive = false
)

var (
	ErrNetworkRead = errors.New("of: network read")
	ErrProtoRead   = errors.New("of: proto read")
)

func (c *Connection) AddSymbolSubscription(symbols []string, handler func(Message)) error {
	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_SubscriptionRequest{
			SubscriptionRequest: &SubscriptionRequest{
				Token:    c.loginResponse.GetToken(),
				Service:  Service_REAL_TIME,
				Requests: []*SubscriptionRequest_Request{},
			},
		},
	}

	for _, s := range symbols {
		if symbolHandlers[s] == nil {
			symbolHandlers[s] = make([]func(Message), 0)
		}

		symbolHandlers[s] = append(symbolHandlers[s], handler)

		ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests = append(
			ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests,
			&SubscriptionRequest_Request{
				Data: &SubscriptionRequest_Request_Symbol{
					Symbol: s,
				},
			},
		)
	}

	ba, _ := proto.Marshal(&ofreq)
	c.connection.WriteMessage(2, ba)

	// Since this method could be called at any point, we can't
	// rely on strict in -> out, so the responses will come on the
	// reader thread

	if !asyncReadActive {
		_, message, err := c.connection.ReadMessage()
		if err != nil {
			return ErrProtoRead
		}

		var ofmsg OpenfeedGatewayMessage
		err = proto.Unmarshal(message, &ofmsg)
		if err != nil {
			return ErrProtoRead
		}

		broadcastMessage(&ofmsg)
	}
	return nil
}

func (c *Connection) Close() {
	c.connection.Close()
}

func (c *Connection) Connection() *websocket.Conn {
	return c.connection
}

func (c *Connection) Login() (bool, error) {

	ofgwlr := OpenfeedGatewayRequest_LoginRequest{
		LoginRequest: &LoginRequest{
			Username: c.credentials.Username,
			Password: c.credentials.Password,
		},
	}

	ofreq := OpenfeedGatewayRequest{Data: &ofgwlr}

	ba, err := proto.Marshal(&ofreq)
	c.connection.WriteMessage(2, ba)
	// Get the login message
	_, message, err := c.connection.ReadMessage()
	if err != nil {
		return false, err
	}

	var ofmsg OpenfeedGatewayMessage
	err = proto.Unmarshal(message, &ofmsg)
	if err != nil {
		return false, fmt.Errorf("Unable to unmarshal gateway message. %v", err)
	}

	c.loginResponse = ofmsg.GetLoginResponse()

	fmt.Println(ofmsg)
	fmt.Print(c.loginResponse)

	return true, nil
}

func Connect(credentials Credentials, server string) (*Connection, error) {
	var connection Connection

	u := url.URL{Scheme: "ws", Host: server, Path: "/ws"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("connect error. %w", err)
	}

	connection.credentials = &credentials
	connection.connection = c

	return &connection, nil
}

func broadcastMessage(ofmsg *OpenfeedGatewayMessage) (Message, error) {
	var msg = Message{}

	switch ty := ofmsg.Data.(type) {
	case *OpenfeedGatewayMessage_MarketSnapshot:
		msg.MessageType = MessageType_MARKET_SNAPSHOT
		msg.Message = ofmsg.Data
		ms := ofmsg.GetMarketSnapshot()
		ary := symbolHandlers[ms.Symbol]
		if ary != nil {
			for _, h := range ary {
				h(msg)
			}
		}
	case *OpenfeedGatewayMessage_MarketUpdate:
		msg.MessageType = MessageType_MARKET_UPDATE
		msg.Message = ofmsg.Data
		mu := ofmsg.GetMarketUpdate()
		ary := symbolHandlers[mu.Symbol]
		if ary != nil {
			for _, h := range ary {
				h(msg)
			}
		}
	case *OpenfeedGatewayMessage_SubscriptionResponse:
		msg.MessageType = MessageType_SUBSCRIPTION_RESPONSE
		msg.Message = ofmsg.Data
		sr := ofmsg.GetSubscriptionResponse()
		ary := symbolHandlers[sr.Symbol]
		if ary != nil {
			for _, h := range ary {
				h(msg)
			}
		}
	default:
		log.Printf("WARN: Unhandled message type. %s", ty)
	}

	return msg, nil
}

func (c *Connection) Start() {
	done := make(chan struct{})

	go func() {
		asyncReadActive = true
		for {
			defer close(done)
			_, message, err := c.connection.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}

			var ofmsg OpenfeedGatewayMessage
			err = proto.Unmarshal(message, &ofmsg)
			if err != nil {
				log.Printf("Unable to unmarshal gateway message. %v", err)
				break
			}

			broadcastMessage(&ofmsg)
		}

		asyncReadActive = false
	}()

	for {
		select {
		case <-done:
			log.Println("done")
			return
		}
	}
}
