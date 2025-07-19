#!/bin/bash

# 完整的本地环境部署脚本（包含Hive表支持）

set -e

echo "🚀 部署本地标签系统（含Hive表支持）"
echo "==========================================="

# 脚本配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 彩色输出函数
print_step() {
    echo -e "\n🔸 $1"
    echo "-------------------------------------------"
}

print_success() {
    echo -e "✅ $1"
}

print_warning() {
    echo -e "⚠️ $1"
}

print_error() {
    echo -e "❌ $1"
}

# 检查前置条件
check_prerequisites() {
    print_step "检查前置条件"
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    
    # 检查Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
    
    # 检查Python
    if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
        print_error "Python未安装，请先安装Python 3.8+"
        exit 1
    fi
    
    # 检查必需的JAR文件
    local jars_dir="$SCRIPT_DIR/jars"
    local required_jars=(
        "hadoop-aws-3.3.4.jar"
        "aws-java-sdk-bundle-1.12.262.jar"
        "mysql-connector-j-8.0.33.jar"
    )
    
    for jar in "${required_jars[@]}"; do
        if [ ! -f "$jars_dir/$jar" ]; then
            print_error "缺少必需的JAR文件: $jar"
            print_error "请确保 $jars_dir 目录包含所有必需的JAR文件"
            exit 1
        fi
    done
    
    print_success "前置条件检查通过"
}

# 停止现有服务
stop_services() {
    print_step "停止现有服务"
    
    cd "$SCRIPT_DIR"
    
    if docker-compose ps | grep -q "Up"; then
        docker-compose down
        print_success "现有服务已停止"
    else
        print_warning "没有运行中的服务"
    fi
}

# 清理现有数据
clean_existing_data() {
    print_step "清理现有数据"
    
    cd "$SCRIPT_DIR"
    
    # 清理数据卷
    docker volume ls | grep tag_system && {
        docker volume rm $(docker volume ls | grep tag_system | awk '{print $2}') 2>/dev/null || true
        print_success "Docker数据卷清理完成"
    } || print_warning "没有需要清理的Docker数据卷"
    
    # 清理网络
    docker network ls | grep tag_system && {
        docker network rm $(docker network ls | grep tag_system | awk '{print $2}') 2>/dev/null || true
        print_success "Docker网络清理完成"
    } || print_warning "没有需要清理的Docker网络"
}

# 启动基础服务
start_base_services() {
    print_step "启动基础服务（MySQL + MinIO）"
    
    cd "$SCRIPT_DIR"
    
    # 启动服务
    docker-compose up -d
    
    # 等待服务就绪
    echo "⏳ 等待服务启动..."
    sleep 10
    
    # 检查MySQL
    local mysql_attempts=0
    while [ $mysql_attempts -lt 30 ]; do
        if docker exec tag_system_mysql mysql -u root -proot123 -e "SELECT 1;" > /dev/null 2>&1; then
            print_success "MySQL服务已就绪"
            break
        fi
        echo "   等待MySQL... ($((mysql_attempts + 1))/30)"
        sleep 2
        ((mysql_attempts++))
    done
    
    if [ $mysql_attempts -eq 30 ]; then
        print_error "MySQL服务启动超时"
        exit 1
    fi
    
    # 检查MinIO
    local minio_attempts=0
    while [ $minio_attempts -lt 30 ]; do
        if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            print_success "MinIO服务已就绪"
            break
        fi
        echo "   等待MinIO... ($((minio_attempts + 1))/30)"
        sleep 2
        ((minio_attempts++))
    done
    
    if [ $minio_attempts -eq 30 ]; then
        print_error "MinIO服务启动超时"
        exit 1
    fi
}

# 初始化数据库
init_database() {
    print_step "初始化MySQL数据库"
    
    cd "$SCRIPT_DIR"
    ./init_data.sh db-only
    
    print_success "数据库初始化完成"
}

# 初始化Hive表
init_hive_tables() {
    print_step "初始化Hive表数据"
    
    cd "$SCRIPT_DIR"
    
    # 使用Python脚本初始化Hive表
    if python init_hive_tables.py; then
        print_success "Hive表初始化完成"
    else
        print_error "Hive表初始化失败"
        exit 1
    fi
}

# 运行健康检查
run_health_check() {
    print_step "运行系统健康检查"
    
    cd "$PROJECT_ROOT"
    
    if python main.py --env local --mode health --log-level INFO; then
        print_success "健康检查通过"
    else
        print_error "健康检查失败"
        exit 1
    fi
}

# 运行测试
run_tests() {
    print_step "运行标签计算测试"
    
    cd "$PROJECT_ROOT"
    
    echo "📊 测试1: 全量并行计算（前100个用户）"
    if python main.py --env local --mode users-parallel --user-ids $(python -c "print(','.join([f'user_{i:06d}' for i in range(1, 101)]))") --log-level INFO; then
        print_success "全量并行计算测试通过"
    else
        print_warning "全量并行计算测试失败，继续其他测试"
    fi
    
    echo ""
    echo "📊 测试2: 指定标签计算"
    if python main.py --env local --mode tags-parallel --tag-ids 1,2,3 --log-level INFO; then
        print_success "指定标签计算测试通过"
    else
        print_warning "指定标签计算测试失败"
    fi
}

# 显示部署结果
show_deployment_summary() {
    print_step "部署完成总结"
    
    echo "🎉 本地标签系统部署成功！"
    echo ""
    echo "📊 已部署的组件:"
    echo "  ✅ MySQL数据库 (localhost:3307)"
    echo "  ✅ MinIO对象存储 (localhost:9000)"
    echo "  ✅ 标签系统数据库表结构"
    echo "  ✅ Hive表数据 (S3兼容存储)"
    echo ""
    echo "📋 Hive表列表:"
    echo "  ✅ user_basic_info - 用户基础信息表 (2000用户)"
    echo "  ✅ user_asset_summary - 用户资产汇总表"
    echo "  ✅ user_activity_summary - 用户活动汇总表"
    echo "  ✅ user_transaction_detail - 用户交易明细表"
    echo ""
    echo "🎯 支持的标签任务:"
    echo "  - 标签1: 高净值用户 (user_asset_summary)"
    echo "  - 标签2: VIP客户 (user_basic_info)"
    echo "  - 标签3: 年轻用户 (user_basic_info)"
    echo "  - 标签4: 活跃交易者 (user_activity_summary)"
    echo "  - 标签5: 低风险用户 (user_activity_summary)"
    echo "  - 标签6: 新用户 (user_basic_info)"
    echo "  - 标签7: 现金富裕用户 (user_asset_summary)"
    echo ""
    echo "🚀 现在可以运行:"
    echo "  cd $PROJECT_ROOT"
    echo "  python main.py --env local --mode health                    # 健康检查"
    echo "  python main.py --env local --mode full-parallel            # 全量并行计算"
    echo "  python main.py --env local --mode tags-parallel --tag-ids 1,2,3  # 指定标签计算"
    echo ""
    echo "🔧 管理命令:"
    echo "  cd $SCRIPT_DIR"
    echo "  ./setup.sh stop                                            # 停止服务"
    echo "  ./init_hive_data.sh stats                                  # 查看表统计"
    echo "  ./init_hive_data.sh reset                                  # 重置Hive数据"
    echo ""
    echo "🌐 服务访问:"
    echo "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    echo "  MySQL: localhost:3307 (root/root123)"
    echo ""
}

# 主函数
main() {
    local action="${1:-deploy}"
    
    case "$action" in
        "deploy")
            check_prerequisites
            stop_services
            clean_existing_data
            start_base_services
            init_database
            init_hive_tables
            run_health_check
            run_tests
            show_deployment_summary
            ;;
        "quick")
            print_step "快速重新部署（保留服务）"
            cd "$SCRIPT_DIR"
            init_database
            init_hive_tables
            run_health_check
            show_deployment_summary
            ;;
        "clean")
            stop_services
            clean_existing_data
            print_success "环境清理完成"
            ;;
        "restart")
            stop_services
            start_base_services
            print_success "服务重启完成"
            ;;
        *)
            echo "用法: $0 {deploy|quick|clean|restart}"
            echo ""
            echo "  deploy   - 完整部署（默认）"
            echo "  quick    - 快速重新部署（保留服务）"
            echo "  clean    - 清理环境"
            echo "  restart  - 重启服务"
            exit 1
            ;;
    esac
}

# 错误处理
trap 'print_error "部署过程中发生错误，请检查日志"; exit 1' ERR

# 执行主函数
main "$@"