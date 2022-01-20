#!/bin/bash

go test -bench=. -benchmem -benchtime=10000x -memprofile=mem.pprof .
go tool pprof -http=:9090 mem.pprof
