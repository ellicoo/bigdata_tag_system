#!/usr/bin/env python3
"""
海豚调度器主程序入口
支持通过海豚调度器图形界面的主程序参数执行
使用新的src/tag_engine架构
"""

import sys
import os
import argparse
from pyspark.sql import SparkSession

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


def create_spark_session():
    """创建Spark会话 - 基于现有架构"""
    spark = SparkSession.builder \
        .appName("BigDataTagSystem-Dolphin") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="海豚调度器标签系统")
    parser.add_argument("--mode", required=True, choices=[
        "health", "task-all", "task-tags", "generate-test-data", "list-tasks"
    ], help="执行模式")
    parser.add_argument("--tag-ids", help="标签ID列表，逗号分隔")
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

            # 检查MySQL连接（使用新架构）
            try:
                from src.tag_engine.meta.MysqlMeta import MysqlMeta
                mysql_meta = MysqlMeta(spark)

                # 测试MySQL连接
                result = mysql_meta.testConnection()
                if result:
                    print("✅ MySQL连接正常")
                else:
                    print("❌ MySQL连接失败")
                    return 1

            except Exception as e:
                print(f"❌ MySQL连接失败: {e}")
                return 1

            print("🎉 系统健康检查通过")

        elif args.mode == "task-all":
            # 全量标签计算
            from src.tag_engine.engine.TagEngine import TagEngine

            engine = TagEngine(spark, environment='dolphinscheduler')
            success = engine.computeTags(mode="task-all", tagIds=None)
            if not success:
                return 1

        elif args.mode == "task-tags":
            # 指定标签计算
            if not args.tag_ids:
                print("❌ 指定标签模式需要提供 --tag-ids 参数")
                return 1

            tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]

            from src.tag_engine.engine.TagEngine import TagEngine
            engine = TagEngine(spark, environment='dolphinscheduler')
            success = engine.computeTags(mode="task-tags", tagIds=tag_ids)
            if not success:
                return 1

        elif args.mode == "list-tasks":
            # 列出可用任务
            from src.tag_engine.meta.MysqlMeta import MysqlMeta
            mysql_meta = MysqlMeta(spark)

            try:
                tags = mysql_meta.loadActiveTagRules()
                print("📋 可用标签任务:")
                for tag in tags.collect():
                    print(f"  {tag.tag_id}: {tag.tag_name}")
            except Exception as e:
                print(f"❌ 获取标签列表失败: {e}")
                return 1

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