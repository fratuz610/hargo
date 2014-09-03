harpro
======

High Availability Redis Proxy

Designed as  and to be used by php or scripting languages

* a standalone server written in go running on localhost on port 36379
* automatically discovers [redis sentinels](http://redis.io/topics/sentinel) as well as the master and slaves
* automatically routes read requests (GET, LRANGE, SMEMBERS, HGETALL) to a random slave
* automatically fails over to a new master when triggered by the sentinels
* caches read requests for up to 1 seconds (fake pipelining)
* multiplexes all incoming to a static number of connections to each Redis instance (fixed to 50 for the time being)
* uses one 4kb read/write buffer per request
* tested with [redis benchmark](http://redis.io/topics/benchmarks)
