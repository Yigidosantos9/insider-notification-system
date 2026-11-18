// Package apispec embeds the OpenAPI specification so the binary can serve it.
package apispec

import _ "embed"

// OpenAPI is the raw openapi.yaml document.
//
//go:embed openapi.yaml
var OpenAPI []byte
