.PHONY: run run-noetcd client build build-linux tidy vet test genproto clean

# 直接运行（需要本地 MongoDB 27017 与 etcd 2379）
run:
	go run ./cmd/server

# 本地没有 etcd 时禁用服务注册
run-noetcd:
	SG_ETCD_ENABLED=false go run ./cmd/server

# 演示客户端
client:
	go run ./cmd/client

# 构建产物到 bin/
build:
	go build -ldflags="-s -w" -o bin/servergame.exe ./cmd/server
	go build -ldflags="-s -w" -o bin/client.exe ./cmd/client

# 构建跨平台 Linux 版本（部署服务器用）
build-linux:
	GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/servergame-linux ./cmd/server

tidy:
	go mod tidy

vet:
	go vet ./...

test:
	go test ./...

# 重新生成 pb 代码
genproto:
	bash ./genproto.sh

clean:
	rm -rf bin/
