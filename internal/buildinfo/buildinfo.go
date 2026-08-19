// Package buildinfo exposes immutable release metadata injected by the build
// pipeline.  Defaults are intentionally obvious so an unversioned local build
// cannot be mistaken for a production release.
package buildinfo

var (
	Version = "dev"
	Commit  = "unknown"
)
