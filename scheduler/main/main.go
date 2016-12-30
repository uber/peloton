package main

import (
	log "github.com/Sirupsen/logrus"
)

type appConfig struct{}

func main() {
	log.Fatal("the scheduler is currently embedded in the master, dont use this")
	select {}
}
