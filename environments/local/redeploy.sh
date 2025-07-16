#!/bin/bash

# 大数据标签系统 - 本地环境重新部署脚本
# 用于部署新的并行优化功能和UPSERT写入策略

set -e  # 遇到错误立即退出

echo "🚀 开始重新部署大数据标签系统..."
echo "⚠️  注意：此操作将清空所有现有数据！"

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

# 函数：检查命令是否存在
check_command() {
    if ! command -v $1 &> /dev/null; then
        print_message $RED "❌ 错误: $1 命令未找到，请先安装"
        exit 1
    fi
}

# 函数：等待用户确认
confirm_action() {
    local message=$1
    echo -e "${YELLOW}$message${NC}"
    read -p "继续吗？(y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_message $YELLOW "操作已取消"
        exit 0
    fi
}

# 检查必要工具
print_message $BLUE "🔍 检查必要工具..."
check_command docker
check_command docker-compose

# 确认操作
confirm_action "这将停止所有服务并清空数据，重新部署整个系统。"

# 记录当前目录
ORIGINAL_DIR=$(pwd)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 确保在正确目录
cd "$SCRIPT_DIR"

print_message $BLUE "📍 当前工作目录: $(pwd)"

# 步骤1：停止现有服务
print_message $BLUE "🛑 步骤1: 停止现有服务..."
if ./setup.sh stop; then
    print_message $GREEN "✅ 服务停止成功"
else
    print_message $YELLOW "⚠️ 服务停止可能有问题，继续执行..."
fi

# 步骤2：清理数据卷和网络
print_message $BLUE "🧹 步骤2: 清理数据卷和网络..."
if ./setup.sh clean; then
    print_message $GREEN "✅ 数据清理成功"
else
    print_message $RED "❌ 数据清理失败"
    exit 1
fi

# 步骤3：重新启动服务
print_message $BLUE "🔄 步骤3: 重新启动服务..."
if ./setup.sh start; then
    print_message $GREEN "✅ 服务启动成功"
else
    print_message $RED "❌ 服务启动失败"
    exit 1
fi

# 等待服务就绪
print_message $BLUE "⏳ 等待服务就绪..."
sleep 10

# 步骤4：重新初始化数据库
print_message $BLUE "🗄️ 步骤4: 重新初始化数据库..."
if ./init_data.sh; then
    print_message $GREEN "✅ 数据库初始化成功"
else
    print_message $RED "❌ 数据库初始化失败"
    exit 1
fi

# 步骤5：验证部署
print_message $BLUE "🔍 步骤5: 验证系统部署..."
cd "$ORIGINAL_DIR"

# 健康检查
print_message $BLUE "执行健康检查..."
if python main.py --env local --mode health; then
    print_message $GREEN "✅ 系统健康检查通过"
else
    print_message $RED "❌ 系统健康检查失败"
    exit 1
fi

# 测试基本功能
print_message $BLUE "测试基本功能..."
if python main.py --env local --mode full-parallel; then
    print_message $GREEN "✅ 基本功能测试通过"
else
    print_message $RED "❌ 基本功能测试失败"
    exit 1
fi

# 步骤6：运行完整场景测试（可选）
print_message $BLUE "🧪 步骤6: 运行完整场景测试..."
echo "是否运行完整的6个场景测试？这可能需要几分钟时间。"
read -p "运行完整测试吗？(y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_message $BLUE "运行完整场景测试..."
    if python test_scenarios.py; then
        print_message $GREEN "✅ 完整场景测试通过"
    else
        print_message $YELLOW "⚠️ 部分场景测试失败，但系统基本功能正常"
    fi
else
    print_message $YELLOW "跳过完整场景测试"
fi

# 部署完成
print_message $GREEN "🎉 系统重新部署完成！"
echo ""
print_message $BLUE "📋 可用的并行优化命令："
echo "  python main.py --env local --mode full-parallel                      # 全量用户打全量标签"
echo "  python main.py --env local --mode tags-parallel --tag-ids 1,2,3      # 全量用户打指定标签"
echo "  python main.py --env local --mode incremental-parallel --days 7      # 增量用户打全量标签"
echo "  python main.py --env local --mode incremental-tags-parallel --days 7 --tag-ids 2,4    # 增量用户打指定标签"
echo "  python main.py --env local --mode users-parallel --user-ids user_000001,user_000002    # 指定用户打全量标签"
echo "  python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,3,5    # 指定用户打指定标签"
echo ""
print_message $BLUE "🔧 测试命令："
echo "  python test_scenarios.py                                            # 测试所有6个场景"
echo ""
print_message $GREEN "系统已准备就绪，可以开始使用新的并行优化功能！"