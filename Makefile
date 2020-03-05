all: build

build: 
	mkdir -p build
	GO111MODULE=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -o build/go-bin2es ./main

clean:
	GO111MODULE=on go clean -i ./...
	@rm -rf build