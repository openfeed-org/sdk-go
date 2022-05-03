// Copyright 2019 - 2022 Barchart.com, Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package openfeed implements openfeed API.

package openfeed

import "log"

func init() {
	log.Default().SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
