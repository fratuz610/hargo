hargo
======

High Availability Redis Proxy written in [Go](http://golang.org/)

Designed as standalone server implementing the Redis protocol

* runs on localhost on port 36379
* is specifically designed to support php-like scripting languages that open a connection to the redis server on each http request
* automatically discovers [redis sentinels](http://redis.io/topics/sentinel) as well as the master and slaves
* automatically routes read requests (GET, LRANGE, SMEMBERS, HGETALL) to a random slave
* automatica lly fails over to a new master when triggered by the sentinels
* caches read requests for up to 1 seconds (fake pipelining)
* multiplexes all incoming requests to 50 connections per Redis instance (master or slave)
* uses one 4kb read/write buffer per request
* tested with [redis benchmark](http://redis.io/topics/benchmarks)
* tested with Go 1.3.1

## Performance

Overall there is a *30% performance loss* over connecting directly to Redis. 
This number varies depending on what virtualisation technology the test servers run on and is due to the extra hop in the connection.

Read requests that hit the cache have little to no performance loss

Still the performnce remains quite good in terms of throuput:

```
redis-benchmark -p 36379 -n 50000 -c 50 -t get,set,incr -q
SET: 26399.15 requests per second
GET: 32573.29 requests per second
INCR: 31113.88 requests per second
```

More benchmarks to follow

## TODO

* add unit testing
* make localhost port customizable 
* make connection number to each redis instance customizable 
* experiment with linux [Zero copy](http://www.linuxjournal.com/article/6345) to improve performance
* more benchmarks for real case scenarios
* test autofailover
* proper load testing
* augment the accepted redis command list to explicitly route read requests to the master