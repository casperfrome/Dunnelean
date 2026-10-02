//go:build linux && amd64

package nativeassets

import _ "embed"

//go:embed linux_amd64.gz
var Library []byte
