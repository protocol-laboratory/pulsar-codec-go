# pulsar-codec-go
```
go get github.com/gogo/protobuf/protoc-gen-gogofast
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/gogoproto
```

Generate code:

```
cd pb
protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofast_out=. PulsarApi.proto
```
