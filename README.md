# openalex-processing-go
openalex process by go

# build cmd

install cobra-cli
`go install github.com/spf13/cobra-cli@latest`

init
`cobra-cli init -a admim@knogen.cn -l MIT `

add script
`cobra-cli add test`


## processing all from file

## prepare proto

```
python3 -m pip install grpcio
python3 -m pip install grpcio-tools
python3 -m grpc_tools.protoc -I proto --python_out=./server/graph/grpcgraph --grpc_python_out=./server/graph/grpcgraph proto/graph.proto
```

```
go get -u github.com/golang/protobuf/protoc-gen-go
protoc --go_out=./internal/grpc --go-grpc_out=./internal/grpc ./proto/*.proto

```