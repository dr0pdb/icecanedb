VERSION?="0.0.1"

.PHONY: compile-linux
compile-linux:
	GOOS=linux GOARCH=386 go build -a ./...

.PHONY: compile-windows
compile-windows:
	GOOS=windows GOARCH=386 go build -a ./...

.PHONY: unit-test
unit-test:
	go test -covermode=count -coverprofile=profile.cov -v ./... 
