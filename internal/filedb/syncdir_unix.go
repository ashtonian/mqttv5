//go:build unix

package filedb

import "os"

// syncDirEntries fsyncs directory dir, so the names created in it
// survive power loss.
func syncDirEntries(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
