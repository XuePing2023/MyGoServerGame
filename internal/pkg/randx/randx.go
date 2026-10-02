// Package randx 提供基于 crypto/rand 的随机工具。
package randx

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
)

// RandomHex 生成 n 字节的十六进制随机串（长度为 2n）。
func RandomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败属于系统级异常，直接 panic 合理
		panic(fmt.Sprintf("读取随机数失败: %v", err))
	}
	return hex.EncodeToString(b)
}

// RandomInt 返回 [min, max] 闭区间内的随机整数。
func RandomInt(min, max int) int {
	if max <= min {
		return min
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max-min+1)))
	if err != nil {
		return min
	}
	return min + int(n.Int64())
}
