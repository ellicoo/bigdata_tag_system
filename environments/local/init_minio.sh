#!/bin/bash

# MinIO存储桶初始化脚本
# 注意: 需要先安装mc客户端: https://docs.min.io/docs/minio-client-quickstart-guide.html

echo "📁 初始化MinIO存储桶..."

# 配置MinIO客户端
mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || {
    echo "⚠️ 无法连接到MinIO，请确保:"
    echo "1. MinIO服务正在运行: docker ps | grep minio"
    echo "2. 安装了mc客户端: brew install minio/stable/mc"
    exit 1
}

# 创建必要的存储桶
buckets=("test-data-lake" "hive-warehouse" "tag-results" "temp-data")

for bucket in "${buckets[@]}"; do
    if mc mb local/$bucket 2>/dev/null; then
        echo "✅ 创建存储桶: $bucket"
    else
        echo "ℹ️ 存储桶已存在: $bucket"
    fi
done

# 设置存储桶策略（可选）
mc policy set public local/test-data-lake 2>/dev/null || true

echo "✅ MinIO存储桶初始化完成"
echo ""
echo "📋 可用存储桶:"
mc ls local 2>/dev/null || echo "请在MinIO Console中查看: http://localhost:9001"