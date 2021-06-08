# Example Client for Golang SDK for Openfeed

The client example shows how to use the SDK. The SDK is tasked with connecting to the server, logging in, and requesting a series of symbols or exchanges. The SDK then spins a go routine and continues to get messages and event the appropriate handler function, which was passed in during the subscription process.

To request a set of symbols, run:

`go run *.go -u MyUsername -p MyPassword symbol1,symbol2`


To request a set of exchanges, run: (Please note that your username must be specially permissioned for this)

`go run *.go -u MyUsername -p MyPassword -e exchange1,exchange2`
