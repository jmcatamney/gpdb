SHELL := /bin/bash
.DEFAULT_GOAL := test
MODULE_NAME=gpbackup
DIR_PATH=$(shell dirname `pwd`)

.PHONY : build

DEST = .

GOFLAGS := -o $(DEST)

dependencies :
		go get github.com/jmoiron/sqlx
		go get github.com/maxbrunsfeld/counterfeiter
		go get github.com/onsi/ginkgo/ginkgo
		go get github.com/onsi/gomega
		go get golang.org/x/tools/cmd/goimports

		go get

format :
		goimports -w .
		go fmt .

ginkgo :
		ginkgo -r -randomizeSuites -randomizeAllSpecs 2>&1

test : format ginkgo

ci : ginkgo

depend : dependencies

build : format
		go build $(GOFLAGS) -o $(MODULE_NAME)
