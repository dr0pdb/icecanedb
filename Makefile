VERSION?="0.0.1"

.PHONY: compile-linux
compile-linux:
	GOOS=linux GOARCH=386 go build -race -v ./...

.PHONY: compile-windows
compile-windows:
	GOOS=windows GOARCH=386 go build -race -v ./...

.PHONY: unit-test
unit-test:
	go test -race -covermode=count -coverprofile=profile.cov -v ./...

.PHONY: build-example-storage
build-example-storage:
	go build -o _output/example_storage -v ./examples/storage/

#### Dev Commands ####
.PHONY: clean-example-directory
clean-example-directory:
	rm -rf example-directory/*
