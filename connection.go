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
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
)

// Credentials encapsulates the username/password
type Credentials struct {
	Username string
	Password string
}

// Connection is the main struct that holds the
// underpinning websocket connection
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
	ErrInvalidLogin   = errors.New("of: invalid login")
	ErrNetworkConnect = errors.New("of: connect")
	ErrNetworkRead    = errors.New("of: network read")
	ErrProtoRead      = errors.New("of: proto read")
)

// AddSymbolSubscription subscribes a handler for messages for given
// slice of symbols
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

// Close closes the connection
func (c *Connection) Close() {
	c.connection.Close()
}

// Login sends the login request to the server, and returns
// true/false with optional error information
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

	if c.loginResponse != nil {
		st := c.loginResponse.GetStatus()
		if st != nil {
			switch st.GetResult() {
			case Result_INVALID_CREDENTIALS:
				return false, ErrInvalidLogin
			case Result_SUCCESS:
				return true, nil
			default:
				return false, fmt.Errorf("of: login failure - %s", st.GetMessage())
			}
		}
	}

	return false, fmt.Errorf("of: login failed, invalid response")
}

// Connect connects to the server
func Connect(credentials Credentials, server string) (*Connection, error) {
	var connection Connection

	u := url.URL{Scheme: "ws", Host: server, Path: "/ws"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, ErrNetworkConnect
	}

	connection.credentials = &credentials
	connection.connection = c

	return &connection, nil
}

// Start spins a go routine which then continuosly reads the messages
// from the websocket connection, unmarshals the protobuf into
// Openfeed messages, and then calls the registered handlers for a given
// symbol
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

func broadcastMessage(ofmsg *OpenfeedGatewayMessage) (Message, error) {
	var (
		ary []func(Message)
		msg = Message{}
	)

	switch ty := ofmsg.Data.(type) {
	case *OpenfeedGatewayMessage_InstrumentDefinition:
		msg.MessageType = MessageType_INSTRUMENT_DEFINITION
		ary = symbolHandlers[ofmsg.GetInstrumentDefinition().Symbol]
	case *OpenfeedGatewayMessage_MarketSnapshot:
		msg.MessageType = MessageType_MARKET_SNAPSHOT
		ary = symbolHandlers[ofmsg.GetMarketSnapshot().Symbol]
	case *OpenfeedGatewayMessage_MarketUpdate:
		msg.MessageType = MessageType_MARKET_UPDATE
		ary = symbolHandlers[ofmsg.GetMarketUpdate().Symbol]
	case *OpenfeedGatewayMessage_SubscriptionResponse:
		msg.MessageType = MessageType_SUBSCRIPTION_RESPONSE
		ary = symbolHandlers[ofmsg.GetSubscriptionResponse().Symbol]
	default:
		log.Printf("WARN: Unhandled message type. %s. %s", reflect.TypeOf(ofmsg.Data), ty)
		return Message{}, fmt.Errorf("of: unhandled message type %s", reflect.TypeOf(ofmsg.Data))
	}

	msg.Message = ofmsg.Data
	if ary != nil {
		for _, h := range ary {
			h(msg)
		}
	}

	return msg, nil
}
