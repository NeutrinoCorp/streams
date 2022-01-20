#!/bin/bash

go test -gcflags "-m" -bench=. -benchmem ./"$1".go