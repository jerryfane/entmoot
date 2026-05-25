package skills

import "embed"

// FS contains the canonical Entmoot skill package used for generated plugins.
//
//go:embed entmoot/**
var FS embed.FS
