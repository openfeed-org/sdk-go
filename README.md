# sdk-go

## Golang SDK for Openfeed

This SDK encapuslates the openfeed proto objects and faciliates client connections
to openfeed servers.

### Client Example

The client example shows how to use the SDK. The SDK is tasked with connecting to the server, logging in, and requesting a series of symbols or exchanges. The SDK then spins a go routine and continues to get messages and event the appropriate handler function, which was passed in during the subscription process.

To request a set of symbols, run:

`go run *.go --username=MyUsername --password=MyPassword --server=openfeed.aws.barchart.com symbol1,symbol2`


To request a set of exchanges, run:

`go run *.go --username=MyUsername --password=MyPassword --server=openfeed.aws.barchart.com --exchange exchange1,exchange2`
