// Command notifier runs the notification system. Usage:
//
//	notifier api      # HTTP API
//	notifier worker   # queue workers delivering notifications
//	notifier migrate  # apply database migrations and exit
package main

import (
	"fmt"
	"os"

	"github.com/Yigidosantos9/insider-notification-system/internal/config"
	"github.com/Yigidosantos9/insider-notification-system/internal/logging"
	"github.com/Yigidosantos9/insider-notification-system/migrations"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: notifier <api|worker|migrate>")
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	log := logging.New(cfg.LogLevel)

	switch os.Args[1] {
	case "migrate":
		if err := migrations.Up(cfg.DatabaseURL); err != nil {
			log.Error("migration failed", "error", err)
			os.Exit(1)
		}
		log.Info("migrations applied")
	default:
		fmt.Fprintf(os.Stderr, "unknown or not yet implemented command %q\n", os.Args[1])
		os.Exit(1)
	}
}
