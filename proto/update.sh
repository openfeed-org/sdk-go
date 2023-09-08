#!/bin/bash

curl https://raw.githubusercontent.com/openfeed-org/proto/master/openfeed.proto > openfeed.proto
curl https://raw.githubusercontent.com/openfeed-org/proto/master/openfeed_api.proto > openfeed_api.proto
curl https://raw.githubusercontent.com/openfeed-org/proto/master/openfeed_instrument.proto > openfeed_instrument.proto


protoc -I=. --proto_path=. \
    --go_opt=Mopenfeed_instrument.proto=../openfeed/ \
    --go_opt=Mopenfeed_api.proto=../openfeed/ \
    --go_opt=Mopenfeed.proto=../openfeed/ \
    --go_out=. *.proto
