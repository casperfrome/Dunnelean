//go:build windows && amd64

package nativeassets

import _ "embed"

//go:embed windows_amd64.gz
var Library []byte
