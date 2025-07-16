.PHONY: proto build test deps
SHELL=/usr/bin/env bash
export GOPRIVATE=github.com/anyproto
export PATH:=deps:$(PATH)
export CGO_ENABLED:=1
BUILD_GOOS:=$(shell go env GOOS)
BUILD_GOARCH:=$(shell go env GOARCH)

ifeq ($(CGO_ENABLED), 0)
	TAGS:=-tags nographviz
else
	TAGS:=
endif

build:
	@$(eval FLAGS := $$(shell PATH=$(PATH) govvv -flags -pkg github.com/anyproto/any-sync/app))
	GOOS=$(BUILD_GOOS) GOARCH=$(BUILD_GOARCH) go build -v $(TAGS) -o bin/any-sync-node -ldflags "$(FLAGS) -X github.com/anyproto/any-sync/app.AppName=any-sync-node" github.com/anyproto/any-sync-node/cmd

test:
	go test ./... --cover $(TAGS)


PROTOC=protoc
PROTOC_GEN_GO=deps/protoc-gen-go
PROTOC_GEN_DRPC=deps/protoc-gen-go-drpc
PROTOC_GEN_VTPROTO=deps/protoc-gen-go-vtproto

define generate_proto
	@echo "Generating Protobuf for directory: $(1)"
	$(PROTOC) \
		--go_out=. --plugin protoc-gen-go="$(PROTOC_GEN_GO)" \
		--go-vtproto_out=. --plugin protoc-gen-go-vtproto="$(PROTOC_GEN_VTPROTO)" \
		--go-vtproto_opt=features=marshal+unmarshal+size \
		--proto_path=$(1) $(wildcard $(1)/*.proto)
endef

define generate_drpc
	@echo "Generating Protobuf for directory: $(1) $(which protoc-gen-go)"
	$(PROTOC) \
		--go_out=. --plugin protoc-gen-go=$$(which protoc-gen-go) \
		--plugin protoc-gen-go-drpc=$(PROTOC_GEN_DRPC) \
		--go_opt=$(1) \
		--go-vtproto_out=:. --plugin protoc-gen-go-vtproto=$(PROTOC_GEN_VTPROTO) \
		--go-vtproto_opt=features=marshal+unmarshal+size \
		--go-drpc_out=protolib=github.com/planetscale/vtprotobuf/codec/drpc:. $(wildcard $(2)/*.proto)
endef


deps:
	go mod download
	go build -o deps storj.io/drpc/cmd/protoc-gen-go-drpc
	go build -o deps google.golang.org/protobuf/cmd/protoc-gen-go
	go build -o deps github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto
	go build -o deps github.com/ahmetb/govvv


proto:
	$(call generate_drpc,,nodesync/nodesyncproto/protos)
	$(call generate_drpc,,debug/nodedebugrpc/nodedebugrpcproto/protos)

