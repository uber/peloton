package main

import (
	log "github.com/Sirupsen/logrus"
)

type appConfig struct{}

func main() {
	log.Fatal("the executor does nothing (yet)")
	select {}
}
