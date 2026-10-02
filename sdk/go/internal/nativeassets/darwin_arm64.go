//go:build darwin && arm64

package nativeassets

import _ "embed"

//go:embed darwin_arm64.gz
var Library []byte
