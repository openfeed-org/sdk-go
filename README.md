# sdk-go

## Golang SDK for Openfeed

This SDK encapuslates the openfeed proto objects and faciliates client connections
to openfeed servers.

### Client Example

The client example shows how to use the SDK. The SDK is tasked with connecting to the server, logging in, and requesting a series of symbols. The SDK then spins a go routine and continues to get messages and event the appropriate handler function, which was passed in during the subscription process.

Run the client with:

`go run *.go --username=MyUsername --password=MyPassword --server=OpenfeedServer symbol1,symbol2`
