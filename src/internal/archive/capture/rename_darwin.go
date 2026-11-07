package capture

import "golang.org/x/sys/unix"

func renameExclusive(old, new string) error {
	return unix.RenamexNp(old, new, unix.RENAME_EXCL)
}
