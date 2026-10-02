//go:build !(windows && amd64) && !(linux && amd64) && !(darwin && (amd64 || arm64))

package nativeassets

var Library []byte
