#!/bin/bash

curl https://raw.githubusercontent.com/openfeed-org/proto/master/openfeed.proto > openfeed.proto
curl https://raw.githubusercontent.com/openfeed-org/proto/master/openfeed_api.proto > openfeed_api.proto
curl https://raw.githubusercontent.com/openfeed-org/proto/master/openfeed_instrument.proto > openfeed_instrument.proto

protoc --go_out=. *.proto

