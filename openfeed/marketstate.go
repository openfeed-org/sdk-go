// Copyright 2019 - 2022 Barchart.com, Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package openfeed implements openfeed API.

package openfeed

import (
	"log"
	"reflect"
)

type Quote struct {
	Symbol    string `json:"symbol"`
	Open      *int64 `json:"open"`
	High      *int64 `json:"high"`
	Low       *int64 `json:"low"`
	Last      *int64 `json:"last"`
	TradeTime int64  `json:"tradeTime"`
}

func (q *Quote) applyOpen(o *Open) {
	if o != nil {
		q.Open = &o.Price
	}
}

func (q *Quote) applyHigh(h *High) {
	if h != nil {
		q.High = &h.Price
	}
}

func (q *Quote) applyLast(l *Last) {
	if l != nil {
		q.Last = &l.Price
	}
}

func (q *Quote) applyLow(l *Low) {
	if l != nil {
		q.Low = &l.Price
	}
}

func (q *Quote) applyTrade(t *Trade) {
	if t != nil {
		if !t.DoesNotUpdateLast {
			q.Last = &t.Price
			q.TradeTime = t.TransactionTime
		}
	}
}

type MarketState struct {
	quotes      map[string]*Quote
	definitions map[int64]*InstrumentDefinition
	defmap1     map[int64]string
	defmap2     map[string]int64
}

func NewMarketState() MarketState {
	return MarketState{
		quotes:      make(map[string]*Quote),
		definitions: make(map[int64]*InstrumentDefinition),
		defmap1:     make(map[int64]string),
		defmap2:     make(map[string]int64),
	}
}

func (m *MarketState) GetQuote(symbol string) *Quote {
	return m.quotes[symbol]
}

func (m *MarketState) GetInstrumentDefinition(symbol string) *InstrumentDefinition {
	i := m.defmap2[symbol]
	return m.definitions[i]
}

func (m *MarketState) ProcessMessage(message *OpenfeedGatewayMessage) {
	switch ty := message.Data.(type) {
	case *OpenfeedGatewayMessage_InstrumentDefinition:
		inst := message.GetInstrumentDefinition()
		m.definitions[inst.GetMarketId()] = inst
	case *OpenfeedGatewayMessage_InstrumentResponse:
	case *OpenfeedGatewayMessage_MarketSnapshot:
		ms := message.GetMarketSnapshot()
		q := m.quotes[ms.GetSymbol()]
		if q == nil {
			q = &Quote{
				Symbol: ms.GetSymbol(),
			}

			q.applyOpen(ms.GetOpen())
			q.applyHigh(ms.GetHigh())
			q.applyLow(ms.GetLow())
			q.applyLast(ms.GetLast())

			m.quotes[q.Symbol] = q
		}
	case *OpenfeedGatewayMessage_MarketUpdate:
		mu := message.GetMarketUpdate()
		q := m.quotes[mu.GetSymbol()]
		if q == nil {
			log.Printf("warn: no quote found for MarketUpdate %s", mu.GetSymbol())
			break
		}

		switch ty := mu.Data.(type) {
		case *MarketUpdate_Bbo:
		case *MarketUpdate_MarketSummary:
			// s := mu.GetMarketSummary()

		case *MarketUpdate_Trades:
			arr := mu.GetTrades().GetTrades()
			for _, t := range arr {
				switch ty := t.Data.(type) {
				case *Trades_Entry_Trade:
					q.applyTrade(t.GetTrade())
				default:
					log.Printf("warn: unhandled trade type %s", ty)
				}
			}
		default:
			log.Printf("warn: unhandled MarketUpdate: %s. %s", reflect.TypeOf(mu.Data), ty)
		}
	case *OpenfeedGatewayMessage_SubscriptionResponse:
		msg := message.GetSubscriptionResponse()
		if msg.GetStatus().GetResult() == Result_SUCCESS {
			m.defmap1[msg.GetMarketId()] = msg.GetSymbol()
			m.defmap2[msg.GetSymbol()] = msg.GetMarketId()
		} else {
			log.Printf("error: unsuccessful subscription for %s, type %s, result: %s", msg.GetSymbol(), msg.GetSubscriptionType(), msg.GetStatus().GetResult())
		}
	default:
		log.Printf("warn: unhandled message type. %s. %s", reflect.TypeOf(message.Data), ty)
	}
}
