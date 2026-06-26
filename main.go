package main

import (
	"reactor/models"
	"os"
	"os/signal"
	"syscall"
	"reactor/utils"
)

func main() {
	log := utils.GlobalLogger()
	log.SetLevel(utils.Debug)
	log.Info("Start Reactor!")
	defer log.Info("Reactor Ends!")

	if len(os.Args) < 2 {
		log.Error("Should set path to the config file!")
		return
	}

	cfg, err := models.NewConfig(os.Args[1])
	if err != nil {
		log.Error("Config error: %v", err)
		return
	}

	reactor := models.NewReactor(cfg)
	if err := reactor.Start(); err != nil {
		log.Error("Start reactor error: %v", err)
		os.Exit(1)
	}

	log.Info("Reactor started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Info("Shutting down gracefully...")

	if err := reactor.Stop(); err != nil {
		log.Error("Error during shutdown: %v", err)
		os.Exit(1)
	}
	log.Info("Shutdown completed")
}