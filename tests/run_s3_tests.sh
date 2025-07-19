#!/bin/bash
"""
S3数据测试脚本运行器
提供便捷的命令来运行各种S3数据检查测试
"""

# 设置代理绕过环境变量
export NO_PROXY=localhost,127.0.0.1,192.168.31.89

echo "🚀 S3数据测试工具"
echo "==============================================="
echo "选择要运行的测试:"
echo "1. 快速检查 - 查看所有表的基本信息"
echo "2. 完整检查 - 详细数据结构和内容分析"
echo "3. 标签规则验证 - 验证标签计算规则"
echo "4. 运行所有测试"
echo "==============================================="

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# 切换到项目根目录
cd "$PROJECT_DIR"

if [ "$1" = "quick" ] || [ "$1" = "1" ]; then
    echo "🔍 运行快速检查..."
    python tests/quick_s3_check.py
elif [ "$1" = "full" ] || [ "$1" = "2" ]; then
    echo "🔍 运行完整检查..."
    python tests/test_s3_data_inspection.py
elif [ "$1" = "rules" ] || [ "$1" = "3" ]; then
    echo "🔍 运行标签规则验证..."
    python tests/test_tag_rules_validation.py
elif [ "$1" = "all" ] || [ "$1" = "4" ]; then
    echo "🔍 运行所有测试..."
    echo ""
    echo "1️⃣ 快速检查..."
    python tests/quick_s3_check.py
    echo ""
    echo "2️⃣ 标签规则验证..."
    python tests/test_tag_rules_validation.py
    echo ""
    echo "✅ 所有测试完成！"
else
    echo "请选择测试类型:"
    echo "  ./run_s3_tests.sh quick   - 快速检查"
    echo "  ./run_s3_tests.sh full    - 完整检查"
    echo "  ./run_s3_tests.sh rules   - 标签规则验证"
    echo "  ./run_s3_tests.sh all     - 运行所有测试"
    echo ""
    echo "或者直接运行:"
    echo "  NO_PROXY=localhost,127.0.0.1,192.168.31.89 python tests/quick_s3_check.py"
    echo "  NO_PROXY=localhost,127.0.0.1,192.168.31.89 python tests/test_s3_data_inspection.py"
    echo "  NO_PROXY=localhost,127.0.0.1,192.168.31.89 python tests/test_tag_rules_validation.py"
fi