package offers

/*
Package offers contains the offer service interface and an implementation of it. The offer service is responsible for
acquiring offers for the placement engine main loop and releasing them when the placement engine does not need them any
more. In the future the offer service will also keep offers between placement rounds to decrease the latency of the
placement rounds. The offer service is also responsible for any kind of logging and metrics emission so that these
things will not pollute the code in the placement engine main loop.
*/
