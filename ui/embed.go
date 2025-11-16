package ui

import "embed"

// Static contains the compiled web assets for the UI.
//
//go:embed static/*
var Static embed.FS
