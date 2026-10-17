package main

import (
	"context"
	"github.com/pagrus7/kaferry/internal/app"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }() // A second signal uses the operating system's default action.
	os.Exit(app.Execute(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
