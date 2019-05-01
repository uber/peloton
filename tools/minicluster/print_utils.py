#!/usr/bin/env python


class bcolors:
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    FAIL = "\033[91m"
    WARNING = "\033[93m"
    ENDC = "\033[0m"


def okblue(message):
    print(bcolors.OKBLUE + message + bcolors.ENDC)


def okgreen(message):
    print(bcolors.OKGREEN + message + bcolors.ENDC)


def fail(message):
    print(bcolors.FAIL + message + bcolors.ENDC)


def warn(message):
    print(bcolors.WARNING + message + bcolors.ENDC)
