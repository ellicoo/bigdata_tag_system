#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算系统命令行入口
支持多种执行模式和参数配置
"""
import sys
import os
import argparse
from typing import List, Optional, Dict
from pyspark.sql import SparkSession

# 动态添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))  # 向上两级到项目根目录
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.tag_engine.engine.TagEngine import TagEngine


def create_spark_session(app_name: str = "TagComputeEngine") -> SparkSession:
    """创建Spark会话
    
    Args:
        app_name: 应用程序名称
        
    Returns:
        SparkSession: Spark会话
    """
    print(f"🚀 创建Spark会话: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    # 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark会话创建完成，版本: {spark.version}")
    return spark


def load_mysql_config() -> Dict[str, str]:
    """加载MySQL配置
    
    Returns:
        Dict: MySQL配置字典
    """
    # 从环境变量或配置文件加载
    # 海豚调度器环境使用统一配置
    import os
    
    return {
        "host": os.getenv("MYSQL_HOST", "cex-mysql-test.c5mgk4qm8m2z.ap-southeast-1.rds.amazonaws.com"),
        "port": int(os.getenv("MYSQL_PORT", "3358")),
        "database": os.getenv("MYSQL_DATABASE", "biz_statistics"),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "ayjUzzH8b7gcQYRh"),
        "charset": "utf8mb4"
    }


def parse_tag_ids(tag_ids_str: Optional[str]) -> Optional[List[int]]:
    """解析标签ID字符串
    
    Args:
        tag_ids_str: 逗号分隔的标签ID字符串
        
    Returns:
        List[int]: 标签ID列表，None表示所有标签
    """
    if not tag_ids_str:
        return None
    
    try:
        tag_ids = [int(tag_id.strip()) for tag_id in tag_ids_str.split(",")]
        return tag_ids
    except ValueError as e:
        print(f"❌ 标签ID解析失败: {e}")
        return None


def generate_comprehensive_test_data(spark) -> bool:
    """生成完整的测试数据，匹配json_demo.txt中的所有字段
    
    Args:
        spark: SparkSession
        
    Returns:
        bool: 生成是否成功
    """
    try:
        print("🏗️ 创建表结构...")
        
        # 1. 创建 user_basic_info 表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS tag_system.user_basic_info (
                user_id STRING,
                age INT,
                user_level STRING,
                registration_date DATE,
                birthday DATE,
                first_name STRING,
                last_name STRING,
                middle_name STRING,
                phone_number STRING,
                email STRING,
                is_vip BOOLEAN,
                is_banned BOOLEAN,
                kyc_status STRING,
                account_status STRING,
                primary_status STRING,
                secondary_status STRING
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 2. 创建 user_asset_summary 表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS tag_system.user_asset_summary (
                user_id STRING,
                total_asset_value DECIMAL(18,2),
                cash_balance DECIMAL(18,2),
                debt_amount DECIMAL(18,2)
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 3. 创建 user_activity_summary 表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS tag_system.user_activity_summary (
                user_id STRING,
                trade_count_30d INT,
                last_login_date DATE,
                last_trade_date DATE
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 4. 创建 user_risk_profile 表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS tag_system.user_risk_profile (
                user_id STRING,
                risk_score INT
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        # 5. 创建 user_preferences 表
        spark.sql("""
            CREATE TABLE IF NOT EXISTS tag_system.user_preferences (
                user_id STRING,
                interested_products ARRAY<STRING>,
                owned_products ARRAY<STRING>,
                blacklisted_products ARRAY<STRING>,
                active_products ARRAY<STRING>,
                expired_products ARRAY<STRING>,
                optional_services ARRAY<STRING>,
                required_services ARRAY<STRING>
            ) USING HIVE
            STORED AS PARQUET
        """)
        
        print("✅ 表结构创建完成")
        
        # 生成多样化测试数据
        print("📊 生成多样化测试数据...")
        
        import random
        from datetime import datetime, timedelta
        
        # 生成1000个用户的基础信息，确保数据多样性
        basic_info_data = []
        for i in range(1000):
            user_id = f"user_{i+1:05d}"
            age = random.randint(18, 80)
            user_level = random.choice(["VIP1", "VIP2", "VIP3", "VIP4", "VIP5", "NORMAL"])
            reg_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
            birthday = datetime(1943 + age, random.randint(1, 12), random.randint(1, 28))
            
            first_name = random.choice(["John", "Jane", "Mike", "Lisa", "Tom", "Alice", "Bob", "Carol", None])
            last_name = random.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", None])
            middle_name = random.choice(["A", "B", "C", None, None, None])  # 多数为空
            
            phone_prefix = random.choice(["+86", "+1", "+44"])
            phone_number = f"{phone_prefix}{random.choice(['138', '139', '186'])}{random.randint(10000000, 99999999)}"
            
            email_domain = random.choice(["gmail.com", "yahoo.com", "hotmail.com", "temp.com"])
            email = f"{user_id}@{email_domain}"
            
            is_vip = random.choice([True, False])
            is_banned = random.choice([True, False, False, False])  # 大部分不被封禁
            
            kyc_status = random.choice(["verified", "pending", "rejected"])
            account_status = random.choice(["active", "suspended", "banned", "normal"])
            primary_status = random.choice(["gold", "silver", "bronze", None])
            secondary_status = random.choice(["premium", "standard", None, None])  # 多数为空
            
            basic_info_data.append((
                user_id, age, user_level, reg_date.date(), birthday.date(),
                first_name, last_name, middle_name, phone_number, email,
                is_vip, is_banned, kyc_status, account_status, primary_status, secondary_status
            ))
        
        basic_info_df = spark.createDataFrame(basic_info_data, [
            "user_id", "age", "user_level", "registration_date", "birthday",
            "first_name", "last_name", "middle_name", "phone_number", "email",
            "is_vip", "is_banned", "kyc_status", "account_status", "primary_status", "secondary_status"
        ])
        
        # 生成资产数据
        asset_data = []
        for i in range(1000):
            user_id = f"user_{i+1:05d}"
            total_asset = random.choice([0, 100000, 50000, 200000, 1000, 500000])  # 包含精确匹配值
            cash_balance = random.choice([0, 50000, 25000, 75000, 100000])  # 包含精确匹配值
            debt_amount = random.choice([None, 0, 10000, 5000]) if random.random() > 0.3 else None
            
            asset_data.append((user_id, float(total_asset), float(cash_balance), 
                             float(debt_amount) if debt_amount is not None else None))
        
        asset_df = spark.createDataFrame(asset_data, ["user_id", "total_asset_value", "cash_balance", "debt_amount"])
        
        # 生成活动数据
        activity_data = []
        for i in range(1000):
            user_id = f"user_{i+1:05d}"
            trade_count = random.choice([0, 5, 10, 15, 1, 2])  # 包含精确匹配值
            
            # 确保有用户匹配特定日期条件
            if i < 50:  # 前50个用户有特定日期
                last_login = datetime(2025, 1, 1)
            elif i < 100:
                last_login = datetime(2025, 7, 15)
            else:
                last_login = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 500))
            
            # last_trade_date 部分为空
            last_trade = None if random.random() > 0.7 else last_login - timedelta(days=random.randint(0, 30))
            
            activity_data.append((user_id, trade_count, last_login.date(), 
                                last_trade.date() if last_trade else None))
        
        activity_df = spark.createDataFrame(activity_data, ["user_id", "trade_count_30d", "last_login_date", "last_trade_date"])
        
        # 生成风险档案数据
        risk_data = []
        for i in range(1000):
            user_id = f"user_{i+1:05d}"
            risk_score = random.choice([10, 20, 30, 40, 50])  # 包含 <= 30 的值
            risk_data.append((user_id, risk_score))
        
        risk_df = spark.createDataFrame(risk_data, ["user_id", "risk_score"])
        
        # 生成偏好数据
        preferences_data = []
        product_options = ["stocks", "bonds", "forex", "savings", "checking", "premium", "gold", "platinum", "high_risk", "speculative"]
        service_options = ["advisory", "trading", "research", "premium_support"]
        
        for i in range(1000):
            user_id = f"user_{i+1:05d}"
            
            # 确保不同的列表组合用于测试不同操作符
            interested = random.sample(product_options[:4], random.randint(1, 3))
            owned = random.sample(["savings", "checking", "premium"], random.randint(0, 2))
            blacklisted = ["forex"] if random.random() > 0.8 else []
            active = random.sample(["premium", "gold", "silver"], random.randint(0, 2))
            expired = random.sample(["premium", "platinum"], random.randint(0, 1))
            optional = [] if random.random() > 0.6 else random.sample(service_options, 1)
            required = random.sample(service_options, random.randint(1, 2))
            
            preferences_data.append((user_id, interested, owned, blacklisted, active, expired, optional, required))
        
        preferences_df = spark.createDataFrame(preferences_data, [
            "user_id", "interested_products", "owned_products", "blacklisted_products",
            "active_products", "expired_products", "optional_services", "required_services"
        ])
        
        # 插入数据
        print("💾 插入测试数据...")
        basic_info_df.write.mode("overwrite").insertInto("tag_system.user_basic_info")
        asset_df.write.mode("overwrite").insertInto("tag_system.user_asset_summary")
        activity_df.write.mode("overwrite").insertInto("tag_system.user_activity_summary")
        risk_df.write.mode("overwrite").insertInto("tag_system.user_risk_profile")
        preferences_df.write.mode("overwrite").insertInto("tag_system.user_preferences")
        
        # 验证数据
        print("🔍 验证生成的测试数据...")
        tables = ["user_basic_info", "user_asset_summary", "user_activity_summary", "user_risk_profile", "user_preferences"]
        for table in tables:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM tag_system.{table}").collect()[0]['cnt']
            print(f"   📊 tag_system.{table}: {count} 条记录")
        
        print("🎯 测试数据生成完成，已确保多样性匹配所有标签条件")
        return True
        
    except Exception as e:
        print(f"❌ 测试数据生成失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="标签计算系统")
    parser.add_argument(
        "--mode", 
        choices=["health", "full", "specific", "task-all", "task-tags", "generate-test-data", "list-tasks"],
        default="health",
        help="执行模式：health(健康检查)、full/task-all(全量计算)、specific/task-tags(指定标签)、generate-test-data(生成测试数据)、list-tasks(列出任务)"
    )
    parser.add_argument(
        "--tag-ids",
        type=str,
        help="指定标签ID列表，逗号分隔，如: 1,2,3"
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="TagComputeEngine",
        help="Spark应用程序名称"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="详细输出模式"
    )
    
    args = parser.parse_args()
    
    # 设置详细输出
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.INFO)
    
    print("=" * 60)
    print("🏷️  大数据标签计算系统")
    print("=" * 60)
    print(f"执行模式: {args.mode}")
    print(f"当前工作目录: {os.getcwd()}")
    print(f"脚本目录: {current_dir}")
    print(f"项目根目录: {project_root}")
    print(f"Python路径前3项: {sys.path[:3]}")
    
    if args.tag_ids:
        print(f"指定标签: {args.tag_ids}")
    
    spark = None
    tag_engine = None
    
    try:
        # 1. 创建Spark会话
        spark = create_spark_session(args.app_name)
        
        # 2. 加载配置
        mysql_config = load_mysql_config()
        print(f"MySQL配置: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
        
        # 3. 创建标签引擎
        tag_engine = TagEngine(spark, mysqlConfig=mysql_config)
        
        # 4. 根据模式执行相应操作
        success = False
        
        if args.mode == "health":
            print("\n🔍 执行健康检查...")
            success = tag_engine.healthCheck()
            
        elif args.mode in ["full", "task-all"]:
            print("\n🚀 执行全量标签计算...")
            success = tag_engine.computeTags(mode="task-all")
            
        elif args.mode in ["specific", "task-tags"]:
            tag_ids = parse_tag_ids(args.tag_ids)
            if tag_ids is None:
                print("❌ 指定标签模式需要提供 --tag-ids 参数")
                sys.exit(1)
            
            print(f"\n🎯 执行指定标签计算: {tag_ids}")
            success = tag_engine.computeTags(mode="task-tags", tagIds=tag_ids)
            
        elif args.mode == "generate-test-data":
            print("\n🧪 生成测试数据...")
            # 先创建数据库
            spark.sql("CREATE DATABASE IF NOT EXISTS tag_system")
            print("✅ 数据库 tag_system 创建成功")
            
            # 使用部署包中的测试数据生成器
            try:
                # 尝试导入部署包中的测试数据生成器（海豚调度器环境）
                from generate_test_data import generate_test_data
                generate_test_data(spark)
                success = True
                print("✅ 测试数据生成完成")
            except ImportError:
                # 如果找不到部署包的生成器，使用内置生成器
                print("🔄 使用内置测试数据生成器...")
                success = generate_comprehensive_test_data(spark)
                if success:
                    print("✅ 内置测试数据生成完成")
                else:
                    print("❌ 内置测试数据生成失败")
            
        elif args.mode == "list-tasks":
            print("\n📋 列出可用标签任务...")
            from src.tag_engine.meta.MysqlMeta import MysqlMeta
            mysql_meta = MysqlMeta(spark, mysql_config)
            
            try:
                tags = mysql_meta.loadTagRules()
                print("可用标签任务:")
                for tag in tags.collect():
                    print(f"  {tag.tag_id}: {tag.tag_name if hasattr(tag, 'tag_name') else '未知标签'}")
                success = True
            except Exception as e:
                print(f"❌ 获取标签列表失败: {e}")
                success = False
        
        # 5. 输出结果
        print("\n" + "=" * 60)
        if success:
            print("✅ 任务执行成功")
            exit_code = 0
        else:
            print("❌ 任务执行失败")
            exit_code = 1
        
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n⚠️  任务被用户中断")
        exit_code = 2
        
    except Exception as e:
        print(f"\n❌ 系统异常: {e}")
        import traceback
        if args.verbose:
            traceback.print_exc()
        exit_code = 3
        
    finally:
        # 清理资源
        if tag_engine:
            tag_engine.cleanup()
        
        if spark:
            print("🧹 关闭Spark会话...")
            spark.stop()
        
        print("👋 程序退出")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()