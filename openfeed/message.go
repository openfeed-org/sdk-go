package openfeed

type MessageType int32

const (
	MessageType_UNHANDLED             MessageType = 0
	MessageType_INSTRUMENT_RESPONSE   MessageType = 3
	MessageType_SUBSCRIPTION_RESPONSE MessageType = 5
	MessageType_HEARTBEAT             MessageType = 11
	MessageType_INSTRUMENT_DEFINITION MessageType = 13
	MessageType_LOGOUT                MessageType = 7
	MessageType_MARKET_SNAPSHOT       MessageType = 15
	MessageType_MARKET_UPDATE         MessageType = 16
	MessageType_OHLC                  MessageType = 99
)

type Message struct {
	MessageType MessageType
	Message     isOpenfeedGatewayMessage_Data
	Message2    *OpenfeedGatewayMessage
}
