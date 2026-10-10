// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build !unix

package filedb

// syncDirEntries does nothing where a directory cannot be fsynced, as on
// Windows: there a new name is as durable as the file system's own
// metadata journaling makes it.
func syncDirEntries(string) error { return nil }
