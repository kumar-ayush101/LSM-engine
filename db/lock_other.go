//go:build !unix && !windows

package db

import "os"

// lockFile is a no-op on platforms without a supported locking primitive.
func lockFile(*os.File) error { return nil }
