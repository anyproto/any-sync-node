.PHONY: proto build test deps
export GOPRIVATE=github.com/anytypeio
export PATH:=deps:$(PATH)
CGO_ENABLED=1

ifeq ($(CGO_ENABLED), 0)
	TAGS:=-tags nographviz
else
	TAGS:=
endif

build:
	@$(eval FLAGS := $$(shell PATH=$(PATH) govvv -flags -pkg github.com/anytypeio/any-sync/app))
	CGO_ENABLED=$(CGO_ENABLED) go build -v $(TAGS) -o bin/any-sync-node -ldflags "$(FLAGS)" github.com/anytypeio/any-sync-node/cmd

test:
	go test ./... --cover $(TAGS)

proto:
	protoc --gogofaster_out=:. --go-drpc_out=protolib=github.com/gogo/protobuf:. nodesync/nodesyncproto/protos/*.proto
	protoc --gogofaster_out=:. --go-drpc_out=protolib=github.com/gogo/protobuf:. debug/nodedebugrpc/nodedebugrpcproto/protos/*.proto

deps:
	go mod download
	CGO_ENABLED=$(CGO_ENABLED) go build $(TAGS) -o deps storj.io/drpc/cmd/protoc-gen-go-drpc
	CGO_ENABLED=$(CGO_ENABLED) go build $(TAGS) -o deps/protoc-gen-gogofaster github.com/gogo/protobuf/protoc-gen-gogofaster
	CGO_ENABLED=$(CGO_ENABLED) go build $(TAGS) -o deps github.com/ahmetb/govvv

