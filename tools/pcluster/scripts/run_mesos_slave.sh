#!/bin/bash

# Remove slave metadata to ensure slave start does not pick up old state.
rm -rf /var/lib/mesos/meta/slaves/latest

mesos-slave
