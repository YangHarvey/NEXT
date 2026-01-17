#!/bin/bash

# 配置参数
DB_PATH="/home/ysm/work/test/next_db_10"
QUERY_DIR="/home/ysm/work/NEXT/tpch_query"
OUTPUT_DIR="./query_results"
BINARY="./secondary_index_query"

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

# 获取脚本所在目录（用于查找 binary）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 检查 binary 是否存在
if [ ! -f "$BINARY" ]; then
    echo "Error: $BINARY not found in $SCRIPT_DIR"
    exit 1
fi

# 遍历所有 query 文件（排除 Untitled 和 query_q6）
for query_file in "$QUERY_DIR"/query_q6*; do
    query_name=$(basename "$query_file")
    
    # 跳过 Untitled 文件
    if [[ "$query_name" == "Untitled" ]]; then
        continue
    fi
    
    # 跳过 query_q6 文件（只运行 query_q6_xxx 文件）
    if [[ "$query_name" == "query_q6" ]]; then
        continue
    fi
    
    # 计算 query_size（文件行数）
    query_size=5
    
    # 如果文件为空或行数为0，跳过
    if [ "$query_size" -eq 0 ]; then
        echo "Warning: $query_name is empty, skipping..."
        continue
    fi
    
    # 输出文件路径
    output_file="$OUTPUT_DIR/${query_name}.out"
    
    echo "========================================="
    echo "Running: $query_name"
    echo "Query size: $query_size"
    echo "Output: $output_file"
    echo "========================================="
    
    # 运行查询并将输出保存到文件（同时显示在终端）
    "$BINARY" "$DB_PATH" "$query_size" "$query_file" 2>&1 | tee "$output_file"
    
    # 检查执行状态
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo "✓ Successfully completed: $query_name"
    else
        echo "✗ Failed: $query_name"
    fi
    
    echo ""
done

echo "========================================="
echo "All queries completed!"
echo "Results saved in: $OUTPUT_DIR"
echo "========================================="
