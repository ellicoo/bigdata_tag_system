#!/usr/bin/env python3
"""
海豚调度器主程序入口
支持通过海豚调度器图形界面的主程序参数执行
使用统一的MySQL配置（src.config.base.MySQLConfig）
"""

import sys
import os
import argparse
from pyspark.sql import SparkSession

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


def create_spark_session():
    """创建Spark会话 - 基于现有HiveToKafka.py模式"""
    spark = SparkSession.builder \
        .appName("BigDataTagSystem-Dolphin") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="海豚调度器标签系统")
    parser.add_argument("--mode", required=True, choices=[
        "health", "task-all", "task-tags", "task-users", "list-tasks", "generate-test-data"
    ], help="执行模式")
    parser.add_argument("--tag-ids", help="标签ID列表，逗号分隔")
    parser.add_argument("--user-ids", help="用户ID列表，逗号分隔")
    parser.add_argument("--dt", default="2025-01-20", help="数据日期")

    args = parser.parse_args()

    print(f"🚀 海豚调度器标签系统启动")
    print(f"📋 执行模式: {args.mode}")

    # 创建Spark会话
    spark = create_spark_session()

    try:
        if args.mode == "generate-test-data":
            # 生成测试数据
            print("🚀 开始生成测试数据...")

            # 先创建数据库
            spark.sql("CREATE DATABASE IF NOT EXISTS tag_test")
            print("✅ 数据库 tag_test 创建成功")

            from generate_test_data import generate_test_data
            generate_test_data(spark, args.dt)

            # 生成测试数据
            # from generate_test_data import generate_test_data
            # generate_test_data(spark, args.dt)

        elif args.mode == "health":
            # 健康检查
            print("🔍 执行系统健康检查...")

            # 检查Hive表访问
            try:
                spark.sql("SHOW DATABASES").show()
                print("✅ Hive访问正常")
            except Exception as e:
                print(f"❌ Hive访问失败: {e}")
                return 1

            # 检查MySQL连接（使用统一配置）
            try:
                from src.config.base import MySQLConfig
                config = MySQLConfig()

                # 测试MySQL连接
                mysql_df = spark.read \
                    .format("jdbc") \
                    .option("url", config.jdbc_url) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("user", config.username) \
                    .option("password", config.password) \
                    .option("query", "SELECT 1 as test") \
                    .load()

                mysql_df.show()
                print("✅ MySQL连接正常")

            except Exception as e:
                print(f"❌ MySQL连接失败: {e}")
                return 1

            print("🎉 系统健康检查通过")

        elif args.mode == "task-all":
            # 全量标签计算
            from src.entry.tag_system_api import TagSystemAPI

            with TagSystemAPI(environment='dolphinscheduler') as api:
                success = api.run_task_all_users_all_tags()
                if not success:
                    return 1

        elif args.mode == "task-tags":
            # 指定标签计算
            if not args.tag_ids:
                print("❌ 指定标签模式需要提供 --tag-ids 参数")
                return 1

            tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]

            from src.entry.tag_system_api import TagSystemAPI
            with TagSystemAPI(environment='dolphinscheduler') as api:
                success = api.run_task_specific_tags(tag_ids)
                if not success:
                    return 1

        elif args.mode == "list-tasks":
            # 列出可用任务
            from src.tasks.task_registry import TagTaskFactory
            TagTaskFactory.register_all_tasks()
            tasks = TagTaskFactory.get_all_available_tasks()

            print("📋 可用标签任务:")
            for task_id, task_class in tasks.items():
                print(f"  {task_id}: {task_class.__name__}")

        else:
            print(f"❌ 不支持的模式: {args.mode}")
            return 1

        print("✅ 任务执行成功")
        return 0

    except Exception as e:
        print(f"❌ 任务执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)