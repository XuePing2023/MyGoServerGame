#!/usr/bin/env bash
# 重新生成 protobuf Go 代码（需要 protoc 与 protoc-gen-go）
# 安装方式：
#   1. 下载 protoc: https://github.com/protocolbuffers/protobuf/releases
#   2. go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
set -euo pipefail

command -v protoc >/dev/null 2>&1 || {
  echo "错误: 未找到 protoc，请先安装并加入 PATH" >&2
  exit 1
}

protoc --proto_path=proto --go_out=. --go_opt=module=servergame proto/game/v1/game.proto
echo "已生成 pb/game/v1/game.pb.go"
