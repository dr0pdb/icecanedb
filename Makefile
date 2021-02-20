VERSION?="0.0.1"

.PHONY: compile-linux
compile-linux:
	GOOS=linux GOARCH=amd64 go build -race -v ./...

.PHONY: compile-windows
compile-windows:
	GOOS=windows GOARCH=amd64 go build -race -v ./...

.PHONY: unit-test
unit-test:
	go test -covermode=count -coverprofile=profile.cov -v ./...

.PHONY: protogen
protogen:
	./generate_proto.sh

.PHONY: build-example-storage
build-example-storage:
	go build -o _output/example_storage -v ./examples/storage/

.PHONY: build-run-kv
build-run-kv:
	go build -o _output/kv -v ./cmd/icecanekv/
	export ICECANEKV_CONFIG_FILE=./cluster/local/1.yaml
	./_output/kv

#### Dev Commands ####
.PHONY: clean-example-directory
clean-example-directory:
	rm -rf example-directory/*
