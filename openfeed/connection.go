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
	sync "sync"
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
	instrumentsBySymbol   = make(map[string]*InstrumentDefinition)
)

// Credentials encapsulates the username/password
type Credentials struct {
	Username string
	Password string
}

// Connection is the main struct that holds the
// underpinning websocket connection
type Connection struct {
	sync.RWMutex
	credentials         *Credentials
	server              string
	connection          *websocket.Conn
	loginResponse       *LoginResponse
	messageHandlers     []*MessageHandler
	exchangeHandlers    map[string][]*MessageHandler
	heartbeatHandlers   []*HeartbeatHandler
	ohlcHandlers        map[string][]*MessageHandler
	symbolHandlers      map[string][]*MessageHandler
	symbolSubscriptions map[int64]string
	exchangesMode       bool
	connected           bool
}

type HeartbeatHandler interface {
	NewHeartbeat(*HeartBeat)
}

type MessageHandler interface {
	NewMessage(*Message)
}

// AddHeartbeatSubscription subscribes a handler to heartbeat messages
func (c *Connection) AddHeartbeatSubscription(handler *HeartbeatHandler) {
	c.heartbeatHandlers = append(c.heartbeatHandlers, handler)
}

// AddExchangeSubscription subscribes a handler for messages for given slice of exchanges
func (c *Connection) AddExchangeSubscription(exchanges []string, handler *MessageHandler) {
	c.exchangesMode = true
	for _, s := range exchanges {
		if c.exchangeHandlers[s] == nil {
			c.exchangeHandlers[s] = make([]*MessageHandler, 0)
		}

		c.exchangeHandlers[s] = append(c.exchangeHandlers[s], handler)
	}

}

// AddMessageSubscription subscribes a handler to all messages
func (c *Connection) AddMessageSubscription(handler *MessageHandler) {
	c.messageHandlers = append(c.messageHandlers, handler)
}

// AddSymbolSubscription subscribes a handler for messages for given slice of symbols
func (c *Connection) AddSymbolSubscription(symbols []string, handler *MessageHandler) {
	c.Lock()
	defer c.Unlock()

	for _, s := range symbols {
		if c.symbolHandlers[s] == nil {
			c.symbolHandlers[s] = make([]*MessageHandler, 0)
			if c.connected {
				c.subscribe([]string{s})
			}
		}

		c.symbolHandlers[s] = append(c.symbolHandlers[s], handler)

		fmt.Println("ADD", s, &handler)

	}
}

// AddSymbolSubscription subscribes a handler for messages for given slice of symbols
func (c *Connection) AddSymbolOHLCSubscription(symbols []string, handler *MessageHandler) {

	for _, s := range symbols {
		if c.ohlcHandlers[s] == nil {
			c.ohlcHandlers[s] = make([]*MessageHandler, 0)
		}

		c.ohlcHandlers[s] = append(c.ohlcHandlers[s], handler)
	}
}

// Close closes the connection
func (c *Connection) Close() {
	if c.connection != nil {
		log.Printf("Closing web socket")
		c.connection.Close()
		c.connection = nil
	}
}

func GetInstrumentDefinition(id int64) *InstrumentDefinition {
	return instrumentDefinitions[id]
}

func GetInstrumentDefinitionBySymbol(symbol string) *InstrumentDefinition {
	return instrumentsBySymbol[symbol]
}

func (c *Connection) CreateInstrumentRequestByChannelId(id int32) *OpenfeedGatewayRequest {
	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_InstrumentRequest{
			InstrumentRequest: &InstrumentRequest{
				Token: c.loginResponse.GetToken(),
				Request: &InstrumentRequest_ChannelId{
					ChannelId: id,
				},
			},
		},
	}

	return &ofreq
}

func (c *Connection) CreateInstrumentRequestByExchange(exch string) *OpenfeedGatewayRequest {
	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_InstrumentRequest{
			InstrumentRequest: &InstrumentRequest{
				Token: c.loginResponse.GetToken(),
				Request: &InstrumentRequest_Exchange{
					Exchange: exch,
				},
			},
		},
	}

	return &ofreq
}

func (c *Connection) CreateInstrumentReferenceRequest(exch string) *OpenfeedGatewayRequest {
	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_InstrumentReferenceRequest{
			InstrumentReferenceRequest: &InstrumentReferenceRequest{
				Token: c.loginResponse.GetToken(),
				Request: &InstrumentReferenceRequest_Exchange{
					Exchange: exch,
				},
			},
		},
	}

	return &ofreq
}

// RemoveSymbolSubscription subscribes a handler for messages for given slice of symbols
func (c *Connection) RemoveSymbolSubscription(symbols []string, handler *MessageHandler) {
	for _, s := range symbols {
		arr := c.symbolHandlers[s]

		if arr != nil {
			idx := -1
			for i, h := range arr {
				if h == handler {
					idx = i
					break
				}
			}

			if idx == -1 {
				log.Printf("warn - listener not found while trying to remove from %s", s)
			} else {
				if len(arr) == 1 {
					c.unsubscribe([]string{s})
					fmt.Printf("UNSUBSCRIBE %s\n", s)
					delete(c.symbolHandlers, s)
				} else {
					arr[idx] = arr[len(arr)-1] // Move last item to idx
					arr = arr[:len(arr)-1]     // Truncate
					c.symbolHandlers[s] = arr
				}
			}
		}
	}
}

// Connect connects to the server
func NewConnection(credentials Credentials, server string) *Connection {
	var connection = Connection{
		credentials:         &credentials,
		server:              server,
		exchangeHandlers:    make(map[string][]*MessageHandler),
		heartbeatHandlers:   make([]*HeartbeatHandler, 0),
		messageHandlers:     make([]*MessageHandler, 0),
		ohlcHandlers:        make(map[string][]*MessageHandler),
		symbolHandlers:      make(map[string][]*MessageHandler),
		symbolSubscriptions: make(map[int64]string),
		connected:           false,
	}

	return &connection
}

func (c *Connection) Socket() *websocket.Conn {
	return c.connection
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
		keepReconnecting := true

		// Connect
		err := c.Connect()
		if err != nil && connectCount == 0 {
			return ErrNetworkConnect
		} else if err == nil {
			log.Printf("of: connected to %s", c.server)
			connectCount++

			// Login
			_, err := c.Login()
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

				ofsyreq2 := c.createOHLCRequest()
				if ofsyreq2 != nil {
					ba, _ := proto.Marshal(ofsyreq2)
					c.connection.WriteMessage(2, ba)
				}

			}

			chReader := make(chan struct{})
			// Listen for data
			go func() {
				for {
					// There's a server sent heartbeat every 10 seconds
					c.connection.SetReadDeadline(time.Now().Add(15 * time.Second))
					c.connected = true
					_, message, err := c.connection.ReadMessage()
					if err != nil {
						if keepReconnecting == true {
							log.Printf("of: read error %v", err)
						}
						c.connection = nil
						break
					}

					var ofmsg OpenfeedGatewayMessage
					err = proto.Unmarshal(message, &ofmsg)
					if err != nil {
						log.Printf("of: unable to unmarshal gateway message. %v", err)

					} else {
						m, _ := c.broadcastMessage(&ofmsg)
						for _, h := range c.messageHandlers {
							iface := *h
							iface.NewMessage(&m)
						}

						switch ofmsg.Data.(type) {
						case *OpenfeedGatewayMessage_LogoutResponse:
							lr := ofmsg.GetLogoutResponse()

							if lr.GetStatus().GetResult() == Result_DUPLICATE_LOGIN {
								log.Printf("of: disconnected due to duplicate login. Terminating retries.")
								keepReconnecting = false
								break
							}
						}
					}
				}
				c.connected = false
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

		if keepReconnecting {
			rand.Seed(time.Now().UnixNano())
			sec := rand.Intn(4) + 1
			log.Printf("of: disconnected due to netwrok error, reconnecting in %d seconds", sec)
			time.Sleep(time.Duration(sec) * time.Second)

		} else {
			break
		}
	}

	return nil
}

func (c *Connection) broadcastMessage(ofmsg *OpenfeedGatewayMessage) (Message, error) {
	var (
		ary []*MessageHandler
		idf *InstrumentDefinition
		msg = Message{}
	)

	expectInstrument := true

	switch ty := ofmsg.Data.(type) {
	case *OpenfeedGatewayMessage_HeartBeat:
		// A bit of a special case
		hb := ofmsg.GetHeartBeat()
		if hb == nil {
			return msg, fmt.Errorf("of: nil heartbeat")
		}

		for _, h := range c.heartbeatHandlers {
			hnd := *h
			hnd.NewHeartbeat(hb)
		}

		msg.MessageType = MessageType_HEARTBEAT
		msg.Message = ofmsg.Data

		return msg, nil
	case *OpenfeedGatewayMessage_InstrumentDefinition:
		msg.MessageType = MessageType_INSTRUMENT_DEFINITION
		idf = ofmsg.GetInstrumentDefinition()
		instrumentDefinitions[idf.GetMarketId()] = idf
		instrumentsBySymbol[idf.Symbol] = idf
	case *OpenfeedGatewayMessage_InstrumentResponse:
	case *OpenfeedGatewayMessage_LogoutResponse:
		msg.MessageType = MessageType_LOGOUT
		expectInstrument = false
	case *OpenfeedGatewayMessage_MarketSnapshot:
		msg.MessageType = MessageType_MARKET_SNAPSHOT
		idf = instrumentDefinitions[ofmsg.GetMarketSnapshot().GetMarketId()]
	case *OpenfeedGatewayMessage_MarketUpdate:
		msg.MessageType = MessageType_MARKET_UPDATE
		idf = instrumentDefinitions[ofmsg.GetMarketUpdate().GetMarketId()]
	case *OpenfeedGatewayMessage_Ohlc:
		msg.MessageType = MessageType_OHLC
		idf = instrumentDefinitions[ofmsg.GetOhlc().GetMarketId()]
	case *OpenfeedGatewayMessage_SubscriptionResponse:
		msg.MessageType = MessageType_SUBSCRIPTION_RESPONSE
		if c.exchangesMode {
			ary = c.exchangeHandlers[ofmsg.GetSubscriptionResponse().Exchange]
		} else {
			rsp := ofmsg.GetSubscriptionResponse()
			c.symbolSubscriptions[rsp.GetMarketId()] = rsp.GetSymbol()
			ary = c.symbolHandlers[rsp.GetSymbol()]

		}
	default:
		log.Printf("WARN: Unhandled message type. %s. %s", reflect.TypeOf(ofmsg.Data), ty)
		return Message{MessageType: MessageType_UNHANDLED, Message: ofmsg.Data}, fmt.Errorf("of: unhandled message type %s", reflect.TypeOf(ofmsg.Data))
	}

	if ary == nil {
		if idf == nil {
			if expectInstrument {
				log.Println("of: no instrument", ofmsg)
			}
		} else {
			if c.exchangesMode {
				ary = c.exchangeHandlers[idf.ExchangeCode]
			} else {
				sym := c.symbolSubscriptions[idf.MarketId]
				if sym == "" {
					log.Printf("No mapped symbol for %d", idf.MarketId)
				} else {
					switch msg.MessageType {
					case MessageType_OHLC:
						ary = c.ohlcHandlers[sym]
					default:
						ary = c.symbolHandlers[sym]
					}
				}
			}
		}
	}

	msg.Message = ofmsg.Data
	msg.Message2 = ofmsg
	if ary != nil {
		for _, h := range ary {
			iface := *h
			iface.NewMessage(&msg)
		}
	}

	return msg, nil
}

func (c *Connection) Connect() error {
	u := url.URL{Scheme: "ws", Host: c.server, Path: "/ws"}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

	if err == nil {
		c.connection = conn
		return nil
	}

	return err
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

func (c *Connection) createOHLCRequest() *OpenfeedGatewayRequest {
	if len(c.ohlcHandlers) == 0 {
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

	for s := range c.ohlcHandlers {
		ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests = append(
			ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests,
			&SubscriptionRequest_Request{
				Data: &SubscriptionRequest_Request_Symbol{
					Symbol: s,
				},
				SubscriptionType: []SubscriptionType{SubscriptionType_OHLC, SubscriptionType_OHLC_NON_REGULAR},
			},
		)
	}

	return &ofreq
}

func (c *Connection) subscribe(arr []string) {
	ofreq := c.generateSymbolRequest(arr)
	if ofreq != nil {
		ba, _ := proto.Marshal(ofreq)
		c.connection.WriteMessage(2, ba)
	}
}

func (c *Connection) unsubscribe(arr []string) {
	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_SubscriptionRequest{
			SubscriptionRequest: &SubscriptionRequest{
				Token:         c.loginResponse.GetToken(),
				Service:       Service_REAL_TIME,
				Unsubscribe:   true,
				CorrelationId: 12345678,
				Requests:      []*SubscriptionRequest_Request{},
			},
		},
	}

	for _, s := range arr {
		ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests = append(
			ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests,
			&SubscriptionRequest_Request{
				Data: &SubscriptionRequest_Request_Symbol{
					Symbol: s,
				},
				SubscriptionType: []SubscriptionType{SubscriptionType_QUOTE},
			},
		)
	}

	ba, _ := proto.Marshal(&ofreq)
	c.connection.WriteMessage(2, ba)
}

func (c *Connection) generateSymbolRequest(arr []string) *OpenfeedGatewayRequest {
	ofreq := OpenfeedGatewayRequest{
		Data: &OpenfeedGatewayRequest_SubscriptionRequest{
			SubscriptionRequest: &SubscriptionRequest{
				Token:    c.loginResponse.GetToken(),
				Service:  Service_REAL_TIME,
				Requests: []*SubscriptionRequest_Request{},
			},
		},
	}

	for _, s := range arr {
		ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests = append(
			ofreq.Data.(*OpenfeedGatewayRequest_SubscriptionRequest).SubscriptionRequest.Requests,
			&SubscriptionRequest_Request{
				Data: &SubscriptionRequest_Request_Symbol{
					Symbol: s,
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

	arr := make([]string, 0)
	for s := range c.symbolHandlers {
		arr = append(arr, s)
	}

	return c.generateSymbolRequest(arr)
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

	ba, _ := proto.Marshal(&ofreq)
	c.connection.WriteMessage(2, ba)
	// Get the login message
	_, message, err := c.connection.ReadMessage()
	if err != nil {
		return false, err
	}

	var ofmsg OpenfeedGatewayMessage
	err = proto.Unmarshal(message, &ofmsg)
	if err != nil {
		return false, fmt.Errorf("unable to unmarshal gateway message. %v", err)
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
