top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

.DEFAULT_GOAL := all

THIS_MAKEFILE_DIR=$(shell pwd)
MODULE_NAME=$(shell basename $(THIS_MAKEFILE_DIR))
GO_UTILS_DIR=$(THIS_MAKEFILE_DIR)/../..
BIN_DIR=$(shell echo $${GOPATH:-~/go} | awk -F':' '{ print $$1 "/bin"}')
GPSCP=gpscp
ARCH := amd64
GPDB_VERSION := $(shell ../../../../getversion --short)

# If you want to do cross-compilation,
# BUILD_TARGET=linux for linux and
# BUILD_TARGET=darwin macos.
# See go build GOOS for more information.
BRANCH := $(shell git for-each-ref --format='%(objectname) %(refname:short)' refs/heads | awk "/^$$(git rev-parse HEAD)/ {print \$$2}")
PLATFORM_POSTFIX := $(if $(BUILD_TARGET),.$(BUILD_TARGET).$(BRANCH),)
TARGET_PLATFORM := $(if $(BUILD_TARGET),GOOS=$(BUILD_TARGET) GOARCH=$(ARCH),)

.NOTPARALLEL:

all : dependencies build

# The inheritied LD_LIBRARY_PATH setting causes git clone in go get to fail.  Hence, nullifying it.
dependencies :  export LD_LIBRARY_PATH =
dependencies :
		go get -u github.com/golang/protobuf/protoc-gen-go
		go get golang.org/x/tools/cmd/goimports
		go get github.com/golang/lint/golint
		go get github.com/onsi/ginkgo/ginkgo
		go get github.com/alecthomas/gometalinter
		gometalinter --install
		go get github.com/golang/dep/cmd/dep
		dep ensure

format :
		gofmt -s -w .

lint :
		gometalinter --config=gometalinter.config -s vendor ./...

unit :
		ginkgo -r -randomizeSuites -randomizeAllSpecs

test : format lint unit

build :
		go build -tags '$(GPSCP)' -o $(BIN_DIR)/$(GPSCP)
