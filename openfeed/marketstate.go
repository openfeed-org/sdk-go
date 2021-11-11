package openfeed

import (
	"fmt"
	"log"
	"reflect"
)

type Quote struct {
	Symbol string `json:"symbol"`
	Open   *int64 `json:"open"`
	High   *int64 `json:"high"`
	Low    *int64 `json:"low"`
	Last   *int64 `json:"last"`
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

type MarketState struct {
	quotes map[string]*Quote
}

func NewMarketState() MarketState {
	return MarketState{
		quotes: make(map[string]*Quote),
	}
}

func (m *MarketState) GetQuote(symbol string) *Quote {
	return m.quotes[symbol]
}

func (m *MarketState) ProcessMessage(message *OpenfeedGatewayMessage) {
	switch ty := message.Data.(type) {
	case *OpenfeedGatewayMessage_InstrumentDefinition:
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

		l := mu.GetLast()
		if l != nil {
			q.applyLast(l)
		} else {
			fmt.Println(mu, mu.GetLast())
		}
	default:
		log.Printf("warn: unhandled message type. %s. %s", reflect.TypeOf(message.Data), ty)
	}
}
