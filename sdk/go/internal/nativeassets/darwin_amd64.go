//go:build darwin && amd64

package nativeassets

import _ "embed"

//go:embed darwin_amd64.gz
var Library []byte
