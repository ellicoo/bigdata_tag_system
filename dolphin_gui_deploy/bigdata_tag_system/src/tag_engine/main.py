#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算系统命令行入口
支持多种执行模式和参数配置
支持多环境配置：dev/test/pre/prod
"""
import sys
import os
import argparse
import yaml
from typing import List, Optional, Dict
from pyspark.sql import SparkSession

# 导入TagEngine
try:
    from tag_engine.engine.TagEngine import TagEngine
except ImportError:
    # 添加项目根路径到sys.path以支持从项目根目录执行
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.tag_engine.engine.TagEngine import TagEngine


def create_spark_session(config: Dict[str, any]) -> SparkSession:
    """创建Spark会话
    
    Args:
        config: 包含spark配置的字典
        
    Returns:
        SparkSession: Spark会话实例
    """
    spark_config = config.get("spark", {})
    app_name = spark_config.get("app_name", "TagComputeEngine")
    spark_configs = spark_config.get("configs", {})
    
    print(f"🚀 创建Spark会话: {app_name}")
    
    builder = SparkSession.builder.appName(app_name).enableHiveSupport()
    
    # 应用所有Spark配置
    for key, value in spark_configs.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # 设置日志级别
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark会话创建完成，版本: {spark.version}")
    print(f"📋 应用配置数: {len(spark_configs)}")
    return spark


def load_config(environment: str = "test") -> Dict[str, any]:
    """从YAML配置文件加载环境配置
    
    Args:
        environment: 环境标识 (dev/test/pre/prod)
        
    Returns:
        Dict: 完整的环境配置
    """
    # 查找配置文件路径
    config_file = None
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml"),  # 相对路径
        os.path.join(os.getcwd(), "src", "config", "config.yaml"),  # 工作目录
        os.path.join("/", "opt", "bigdata_tag_system", "src", "config", "config.yaml"),  # 部署路径
        # DolphinScheduler部署路径
        os.path.join(os.getcwd(), "dolphinscheduler", "default", "resources", "bigdata_tag_system", "src", "config", "config.yaml"),
        os.path.join(os.getcwd(), "dolphinscheduler", "default", "resources", "src", "config", "config.yaml"),
        # 从__file__路径推导 - 最关键的路径
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "config.yaml")
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            config_file = path
            break
    
    if not config_file:
        raise FileNotFoundError("❌ 配置文件未找到，请确保 config.yaml 文件存在于正确路径")
    
    try:
        with open(config_file, 'r', encoding='utf-8') as file:
            all_configs = yaml.safe_load(file)
        
        if environment not in all_configs:
            available_envs = list(all_configs.keys())
            raise ValueError(f"不支持的环境: {environment}，支持的环境：{available_envs}")
        
        config = all_configs[environment]
        
        # 环境变量覆盖MySQL配置
        if "mysql" in config:
            mysql_config = config["mysql"]
            mysql_config["host"] = os.getenv("MYSQL_HOST", mysql_config.get("host"))
            mysql_config["port"] = int(os.getenv("MYSQL_PORT", str(mysql_config.get("port", 3306))))
            mysql_config["database"] = os.getenv("MYSQL_DATABASE", mysql_config.get("database"))
            mysql_config["user"] = os.getenv("MYSQL_USER", mysql_config.get("user"))
            mysql_config["password"] = os.getenv("MYSQL_PASSWORD", mysql_config.get("password"))
        
        # 环境变量覆盖ES配置
        if "elasticsearch" in config:
            es_config = config["elasticsearch"]
            # ES主要通过hosts配置，支持环境变量覆盖
            es_hosts = os.getenv("ES_HOSTS", None)
            if es_hosts:
                es_config["hosts"] = [host.strip() for host in es_hosts.split(",")]
            
            es_config["user"] = os.getenv("ES_USER", es_config.get("user", ""))
            es_config["password"] = os.getenv("ES_PASSWORD", es_config.get("password", ""))
            es_config["index"] = os.getenv("ES_INDEX", es_config.get("index", "user_tag_relation"))
        
        print(f"✅ 成功加载 {environment.upper()} 环境配置: {config_file}")
        return config
        
    except Exception as e:
        raise RuntimeError(f"❌ 配置文件加载失败: {e}")


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
    parser.add_argument(
        "--env", "--environment",
        type=str,
        default="test",
        choices=["dev", "test", "pre", "prod"],
        help="环境配置 (dev/test/pre/prod)"
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
    print(f"Python路径前3项: {sys.path[:3]}")
    
    if args.tag_ids:
        print(f"指定标签: {args.tag_ids}")
    
    spark = None
    tag_engine = None
    
    try:
        # 1. 加载环境配置
        print(f"🔧 加载环境配置: {args.env.upper()}")
        config = load_config(args.env)
        mysql_config = config.get("mysql", {})
        es_config = config.get("elasticsearch", None)  # ES配置可选
        maxcompute_config = config.get("maxcompute", {})  # MaxCompute配置

        print(f"MySQL配置: {mysql_config.get('host', 'N/A')}:{mysql_config.get('port', 'N/A')}/{mysql_config.get('database', 'N/A')}")
        if es_config:
            print(f"ES配置: {es_config.get('hosts', 'N/A')} / 索引: {es_config.get('index', 'N/A')}")
        else:
            print("⚠️  未ES配置，将使用MySQL存储用户标签关系")

        if maxcompute_config:
            print(f"MaxCompute配置: 项目={maxcompute_config.get('project', 'N/A')}, Endpoint={maxcompute_config.get('endpoint', 'N/A')}")

        # 2. 创建Spark会话
        spark = create_spark_session(config)

        # 3. 创建标签引擎（使用MaxCompute数据源）
        tag_engine = TagEngine(
            spark,
            maxComputeConfig=maxcompute_config,
            mysqlConfig=mysql_config,
            esConfig=es_config  # 传入ES配置，如果没有则使用MySQL
        )
        
        # 4. 根据模式执行相应操作
        success = False
        
        if args.mode == "health":
            print("\n🔍 执行健康检查...")
            success = tag_engine.healthCheck()
            
        elif args.mode in ["full", "task-all"]:
            print("\n🚀 执行全量标签计算...")
            success, failed_tag_ids = tag_engine.computeTags(mode="task-all")
            if failed_tag_ids:
                print(f"⚠️  {len(failed_tag_ids)} 个标签因表加载失败而跳过: {failed_tag_ids}")
            else:
                print("✅ 所有标签计算成功")
            
        elif args.mode in ["specific", "task-tags"]:
            tag_ids = parse_tag_ids(args.tag_ids)
            if tag_ids is None:
                print("❌ 指定标签模式需要提供 --tag-ids 参数")
                sys.exit(1)
            
            print(f"\n🎯 执行指定标签计算: {tag_ids}")
            success, failed_tag_ids = tag_engine.computeTags(mode="task-tags", tagIds=tag_ids)
            if failed_tag_ids:
                print(f"⚠️  {len(failed_tag_ids)} 个标签因表加载失败而跳过: {failed_tag_ids}")
            else:
                print("✅ 所有指定标签计算成功")
            
        elif args.mode == "generate-test-data":
            print("\n⚠️  测试数据生成功能已移除")
            print("请使用独立的测试数据生成脚本")
            success = False
            
        elif args.mode == "list-tasks":
            print("\n📋 列出可用标签任务...")
            
            try:
                try:
                    from tag_engine.meta.MysqlMeta import MysqlMeta
                except ImportError:
                    from src.tag_engine.meta.MysqlMeta import MysqlMeta
                    
                mysql_meta = MysqlMeta(spark, mysql_config)
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