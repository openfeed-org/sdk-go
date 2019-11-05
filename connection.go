// Copyright 2019 Barchart.com, Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package openfeed implements openfeed API.

package openfeed

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
)

var (
	ErrAlreadyConnected = errors.New("of: already connected")
	ErrInvalidLogin     = errors.New("of: invalid login")
	ErrNetworkConnect   = errors.New("of: connect")
	ErrNetworkRead      = errors.New("of: network read")
	ErrProtoRead        = errors.New("of: proto read")
)

var (
	instrumentDefinitions = make(map[int64]*InstrumentDefinition)
)

// Credentials encapsulates the username/password
type Credentials struct {
	Username string
	Password string
}

// Connection is the main struct that holds the
// underpinning websocket connection
type Connection struct {
	credentials       *Credentials
	server            string
	connection        *websocket.Conn
	loginResponse     *LoginResponse
	exchangeHandlers  map[string][]func(Message)
	heartbeatHandlers []func(*HeartBeat)
	symbolHandlers    map[string][]func(Message)
	exchangesMode     bool
}

// AddHeartbeatSubscription subscribes a handler to heartbeat messages
func (c *Connection) AddHeartbeatSubscription(handler func(*HeartBeat)) {
	c.heartbeatHandlers = append(c.heartbeatHandlers, handler)
}

// AddExchangeSubscription subscribes a handler for messages for given slice of exchanges
func (c *Connection) AddExchangeSubscription(exchanges []string, handler func(Message)) {
	c.exchangesMode = true
	for _, s := range exchanges {
		if c.exchangeHandlers[s] == nil {
			c.exchangeHandlers[s] = make([]func(Message), 0)
		}

		c.exchangeHandlers[s] = append(c.exchangeHandlers[s], handler)
	}

}

// AddSymbolSubscription subscribes a handler for messages for given slice of symbols
func (c *Connection) AddSymbolSubscription(symbols []string, handler func(Message)) {
	for _, s := range symbols {
		if c.symbolHandlers[s] == nil {
			c.symbolHandlers[s] = make([]func(Message), 0)
		}

		c.symbolHandlers[s] = append(c.symbolHandlers[s], handler)
	}
}

// Close closes the connection
func (c *Connection) Close() {
	c.connection.Close()
}

// Connect connects to the server
func NewConnection(credentials Credentials, server string) *Connection {
	var connection = Connection{
		credentials:       &credentials,
		server:            server,
		exchangeHandlers:  make(map[string][]func(Message)),
		heartbeatHandlers: make([]func(*HeartBeat), 0),
		symbolHandlers:    make(map[string][]func(Message)),
	}

	return &connection
}

// Start spins a go routine which then continuosly reads the messages
// from the websocket connection, unmarshals the protobuf into
// Openfeed messages, and then calls the registered handlers for a given
// symbol
func (c *Connection) Start() error {
	var connectCount int64

	if c.connection != nil {
		return ErrAlreadyConnected
	}

	for {
		// Connect
		conn, err := connect(c.server)
		if err != nil && connectCount == 0 {
			return ErrNetworkConnect
		} else if err == nil {
			c.connection = conn

			// Login
			_, err := c.login()
			if err != nil {
				return err
			}

			if c.exchangesMode {
				// Request Exchanges
				ofexreq := c.createExchangeRequest()
				if ofexreq != nil {
					ba, _ := proto.Marshal(ofexreq)
					c.connection.WriteMessage(2, ba)
				}
			} else {
				// Request Symbols
				ofsyreq := c.createSymbolRequest()
				if ofsyreq != nil {
					ba, _ := proto.Marshal(ofsyreq)
					c.connection.WriteMessage(2, ba)
				}
			}

			chReader := make(chan struct{})
			// Listen for data
			go func() {
				for {
					// There's a server sent heartbeat every 10 seconds
					c.connection.SetReadDeadline(time.Now().Add(15 * time.Second))
					_, message, err := c.connection.ReadMessage()
					if err != nil {
						log.Printf("of: read error %v", err)
						c.connection = nil
						break
					}

					var ofmsg OpenfeedGatewayMessage
					err = proto.Unmarshal(message, &ofmsg)
					if err != nil {
						log.Printf("of: unable to unmarshal gateway message. %v", err)

					} else {
						c.broadcastMessage(&ofmsg)
					}
				}

				close(chReader)
			}()

		L2:
			for {
				select {
				case <-chReader:
					break L2
				}
			}
		}

		log.Println("of: caught network event")

		rand.Seed(time.Now().UnixNano())
		sec := rand.Intn(4) + 1
		log.Printf("of: reconnecting in %d seconds", sec)
		time.Sleep(time.Duration(sec) * time.Second)

		connectCount++
	}

}

func (c *Connection) broadcastMessage(ofmsg *OpenfeedGatewayMessage) (Message, error) {
	var (
		ary []func(Message)
		idf *InstrumentDefinition
		msg = Message{}
	)

	switch ty := ofmsg.Data.(type) {
	case *OpenfeedGatewayMessage_HeartBeat:
		// A bit of a special case
		hb := ofmsg.GetHeartBeat()
		if hb == nil {
			return msg, fmt.Errorf("of: nil heartbeat")
		}

		for _, h := range c.heartbeatHandlers {
			h(hb)
		}

		msg.MessageType = MessageType_HEARTBEAT
		msg.Message = ofmsg.Data

		return msg, nil
	case *OpenfeedGatewayMessage_InstrumentDefinition:
		msg.MessageType = MessageType_INSTRUMENT_DEFINITION
		idf = ofmsg.GetInstrumentDefinition()
		instrumentDefinitions[idf.GetMarketId()] = idf
	case *OpenfeedGatewayMessage_MarketSnapshot:
		msg.MessageType = MessageType_MARKET_SNAPSHOT
		idf = instrumentDefinitions[ofmsg.GetMarketSnapshot().GetMarketId()]
	case *OpenfeedGatewayMessage_MarketUpdate:
		msg.MessageType = MessageType_MARKET_UPDATE
		idf = instrumentDefinitions[ofmsg.GetMarketUpdate().GetMarketId()]
	case *OpenfeedGatewayMessage_SubscriptionResponse:
		msg.MessageType = MessageType_SUBSCRIPTION_RESPONSE
		if c.exchangesMode {
			ary = c.exchangeHandlers[ofmsg.GetSubscriptionResponse().Exchange]
		} else {
			ary = c.symbolHandlers[ofmsg.GetSubscriptionResponse().Symbol]
		}
	default:
		log.Printf("WARN: Unhandled message type. %s. %s", reflect.TypeOf(ofmsg.Data), ty)
		return Message{}, fmt.Errorf("of: unhandled message type %s", reflect.TypeOf(ofmsg.Data))
	}

	if ary == nil {
		if idf == nil {
			log.Println("of: no instrument", ofmsg)
		} else {
			if c.exchangesMode {
				ary = c.exchangeHandlers[idf.BarchartExchangeCode]
			} else {
				ary = c.symbolHandlers[idf.Symbol]
			}
		}
	}

	msg.Message = ofmsg.Data
	if ary != nil {
		for _, h := range ary {
			h(msg)
		}
	}

	return msg, nil
}

func connect(server string) (*websocket.Conn, error) {
	u := url.URL{Scheme: "ws", Host: server, Path: "/ws"}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Connection) createExchangeRequest() *OpenfeedGatewayRequest {
	if len(c.exchangeHandlers) == 0 {
		return nil
	}

	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_SubscriptionRequest{
			SubscriptionRequest: &SubscriptionRequest{
				Token:    c.loginResponse.GetToken(),
				Service:  Service_REAL_TIME,
				Requests: []*SubscriptionRequest_Request{},
			},
		},
	}

	for s := range c.exchangeHandlers {
		ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests = append(
			ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests,
			&SubscriptionRequest_Request{
				Data: &SubscriptionRequest_Request_Exchange{
					Exchange: s,
				},
				SubscriptionType: []SubscriptionType{SubscriptionType_QUOTE},
			},
		)
	}

	return &ofreq
}

func (c *Connection) createSymbolRequest() *OpenfeedGatewayRequest {
	if len(c.symbolHandlers) == 0 {
		return nil
	}

	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_SubscriptionRequest{
			SubscriptionRequest: &SubscriptionRequest{
				Token:    c.loginResponse.GetToken(),
				Service:  Service_REAL_TIME,
				Requests: []*SubscriptionRequest_Request{},
			},
		},
	}

	for s := range c.symbolHandlers {
		ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests = append(
			ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests,
			&SubscriptionRequest_Request{
				Data: &SubscriptionRequest_Request_Symbol{
					Symbol: s,
				},
			},
		)
	}

	return &ofreq
}

// Login sends the login request to the server, and returns
// true/false with optional error information
func (c *Connection) login() (bool, error) {
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
