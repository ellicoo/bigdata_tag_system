#!/bin/bash

# 大数据标签系统 - 部署验证脚本
# 用于验证新的并行优化功能是否正常工作

set -e

echo "🔍 开始验证系统部署..."

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 函数：打印彩色消息
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# 记录当前目录
ORIGINAL_DIR=$(pwd)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 回到项目根目录
cd "$SCRIPT_DIR/../.."

print_message $BLUE "📍 当前工作目录: $(pwd)"

# 测试计数器
PASSED=0
FAILED=0

# 函数：运行测试
run_test() {
    local test_name=$1
    local command=$2
    
    print_message $BLUE "🧪 测试: $test_name"
    echo "   命令: $command"
    
    if eval $command > /dev/null 2>&1; then
        print_message $GREEN "   ✅ 通过"
        ((PASSED++))
    else
        print_message $RED "   ❌ 失败"
        ((FAILED++))
    fi
    echo
}

# 开始测试
print_message $BLUE "=" * 60
print_message $BLUE "🎯 验证新的并行优化功能"
print_message $BLUE "=" * 60

# 基础测试
run_test "系统健康检查" "python main.py --env local --mode health"

# 并行优化版本测试
run_test "全量用户打全量标签（并行版）" "python main.py --env local --mode full-parallel"
run_test "全量用户打指定标签（并行版）" "python main.py --env local --mode tags-parallel --tag-ids 1,2"
run_test "增量用户打全量标签（并行版）" "python main.py --env local --mode incremental-parallel --days 7"
run_test "增量用户打指定标签（并行版）" "python main.py --env local --mode incremental-tags-parallel --days 7 --tag-ids 2,3"
run_test "指定用户打全量标签（并行版）" "python main.py --env local --mode users-parallel --user-ids user_000001,user_000002"
run_test "指定用户打指定标签（并行版）" "python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,3"

# 传统模式测试（确保向后兼容）
print_message $BLUE "🔄 验证向后兼容性"
run_test "传统全量计算" "python main.py --env local --mode full"
run_test "传统增量计算" "python main.py --env local --mode incremental --days 3"

# 数据库验证
print_message $BLUE "🗄️ 验证数据库状态"

# 检查MySQL连接
if docker exec tag-system-mysql mysql -u root -proot123 -e "USE tag_system; SELECT COUNT(*) FROM user_tags;" > /dev/null 2>&1; then
    print_message $GREEN "   ✅ MySQL连接正常"
    ((PASSED++))
else
    print_message $RED "   ❌ MySQL连接失败"
    ((FAILED++))
fi

# 检查数据结构
if docker exec tag-system-mysql mysql -u root -proot123 -e "USE tag_system; DESCRIBE user_tags;" > /dev/null 2>&1; then
    print_message $GREEN "   ✅ 数据表结构正常"
    ((PASSED++))
else
    print_message $RED "   ❌ 数据表结构异常"
    ((FAILED++))
fi

# 检查唯一键约束
if docker exec tag-system-mysql mysql -u root -proot123 -e "USE tag_system; SHOW INDEX FROM user_tags WHERE Key_name = 'uk_user_id';" > /dev/null 2>&1; then
    print_message $GREEN "   ✅ 唯一键约束正常"
    ((PASSED++))
else
    print_message $RED "   ❌ 唯一键约束异常"
    ((FAILED++))
fi

# 汇总结果
print_message $BLUE "=" * 60
print_message $BLUE "📊 验证结果汇总"
print_message $BLUE "=" * 60

TOTAL=$((PASSED + FAILED))
print_message $BLUE "总测试数: $TOTAL"
print_message $GREEN "通过: $PASSED"
print_message $RED "失败: $FAILED"

if [ $FAILED -eq 0 ]; then
    print_message $GREEN "🎉 所有验证都通过了！系统部署成功！"
    echo ""
    print_message $BLUE "📋 推荐使用以下并行优化命令："
    echo "  python main.py --env local --mode full-parallel"
    echo "  python main.py --env local --mode tags-parallel --tag-ids 1,2,3"
    echo "  python main.py --env local --mode incremental-parallel --days 7"
    echo ""
    print_message $BLUE "🧪 运行完整测试："
    echo "  python test_scenarios.py"
    exit 0
else
    print_message $RED "💔 有 $FAILED 个验证失败，请检查系统状态"
    echo ""
    print_message $YELLOW "🔧 故障排除建议："
    echo "  1. 检查Docker服务是否正常运行"
    echo "  2. 检查MySQL和MinIO容器状态"
    echo "  3. 重新运行部署脚本: ./redeploy.sh"
    echo "  4. 查看详细日志: docker logs tag-system-mysql"
    exit 1
fi