@echo off
rem 重新生成 protobuf Go 代码（需要 protoc 与 protoc-gen-go）
rem 安装方式：
rem   1. 下载 protoc: https://github.com/protocolbuffers/protobuf/releases
rem   2. go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

where protoc >nul 2>nul
if errorlevel 1 (
    echo 错误: 未找到 protoc，请先安装并加入 PATH
    exit /b 1
)

protoc --proto_path=proto --go_out=. --go_opt=module=servergame proto/game/v1/game.proto
if errorlevel 1 (
    echo 生成失败
    exit /b 1
)
echo 已生成 pb/game/v1/game.pb.go
