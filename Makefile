.PHONY: all
all: build

ALL_PACKAGES=$(shell go list ./... | grep -v "vendor")

setup:
	mkdir -p $(GOPATH)/bin

build-deps:
	dep ensure
	go build -o out/inventory_building main.go

compile:
	mkdir -p out/
	go build -race ./...

build: build-deps setup compile fmt

fmt:
	go fmt ./...

vet:
	go vet ./...

lint:
	golint -set_exit_status $(ALL_PACKAGES)

test: build-deps fmt vet build
	ENVIRONMENT=test go test -race ./...