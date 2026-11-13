package compat

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func rssSampleChild(t *testing.T) {
	t.Helper()
	_, err := fmt.Fprintln(os.Stdout, "ready")
	require.NoError(t, err)
	var command [1]byte
	_, err = io.ReadFull(os.Stdin, command[:])
	require.NoError(t, err)
	memory := make([]byte, 64<<20)
	for i := 0; i < len(memory); i += os.Getpagesize() {
		memory[i] = byte(i/os.Getpagesize()) + 1
	}
	_, err = fmt.Fprintln(os.Stdout, "grown")
	require.NoError(t, err)
	_, err = io.ReadFull(os.Stdin, command[:])
	require.NoError(t, err)
	runtime.KeepAlive(memory)
}
