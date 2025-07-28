#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算系统命令行入口
支持多种执行模式和参数配置
"""
import sys
import argparse
from typing import List, Optional, Dict
from pyspark.sql import SparkSession

from .engine.TagEngine import TagEngine


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
                print("❌ 无法找到测试数据生成器")
                print("海豚调度器环境需要 generate_test_data.py 在当前目录中")
                print("请确保部署包正确解压并包含 generate_test_data.py 文件")
                success = False
            
        elif args.mode == "list-tasks":
            print("\n📋 列出可用标签任务...")
            from .meta.MysqlMeta import MysqlMeta
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