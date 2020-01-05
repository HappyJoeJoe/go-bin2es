all: build

build: 
	GO111MODULE=on go build -o bin/go-bin2es ./main

clean:
	GO111MODULE=on go clean -i ./...
	@rm -rf bin