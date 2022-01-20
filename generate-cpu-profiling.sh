#!/bin/bash

go test -bench=. -benchmem -benchtime=10000x -cpuprofile=cpu.pprof .
go tool pprof -http=:9090 cpu.pprof
