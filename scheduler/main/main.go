package main

import (
	log "github.com/Sirupsen/logrus"
)

func main() {
	log.Fatal("the scheduler is currently embedded in the master, dont use this")
	select {}
}
