#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算系统 - DataWorks MaxCompute Spark 版本

使用场景：
- 支持后端接口触发标签计算
- 支持 DataWorks 工作流参数化调度
- 部署在 DataWorks MaxCompute Spark 环境
- 支持多环境配置：dev/test/pre/prod（必须在任务配置时指定）

DataWorks 任务配置示例：
    # 健康检查（测试环境）
    --mode health --env test

    # 计算指定标签（生产环境，支持参数化）
    --mode task-tags --tag-ids ${tag_ids} --env prod

    # 计算所有标签（生产环境）
    --mode task-all --env prod

    # 列出所有标签任务
    --mode list-tasks --env prod

命令行测试示例：
    python main_dataworks.py --mode health --env test
    python main_dataworks.py --mode task-tags --tag-ids 1,2,3 --env prod
    python main_dataworks.py --mode task-all --env prod

接口调用示例（环境已在启动时配置）：
    # 计算指定标签
    result = compute_tags_by_ids(tag_ids=[1, 2, 3])

    # 计算所有标签
    result = compute_tags_by_ids(tag_ids=None)
"""
import sys
import os
import yaml
from typing import List, Optional, Dict
from pyspark.sql import SparkSession

# ⚠️ DataWorks环境特殊处理：添加ZIP包路径到sys.path
# 在 DataWorks 中，bigdata_tag_system.zip 通过 PYTHONPATH 可访问
# 即使路径在文件系统中不存在，Python 也能通过 PYTHONPATH 访问
zip_src_paths = [
    './bigdata_tag_system.zip/src',  # DataWorks PYTHONPATH 指向的路径
    './src',  # 备用路径（如果 ZIP 被解压）
    os.path.join(os.path.dirname(__file__), '..', '..')  # 本地开发路径
]
# 不检查路径是否存在，直接添加（因为 ZIP 文件通过 PYTHONPATH 可访问）
for path in zip_src_paths:
    if path not in sys.path:
        sys.path.insert(0, path)

# 全局环境变量，在启动时从命令行参数加载，必须明确指定
_GLOBAL_ENVIRONMENT = None

# 导入TagEngine
from tag_engine.engine.TagEngine import TagEngine


def get_spark_session() -> SparkSession:
    """获取或创建Spark会话（MaxCompute Spark环境）

    DataWorks MaxCompute Spark会自动创建Spark会话，直接获取即可

    Returns:
        SparkSession: Spark会话实例
    """
    try:
        # MaxCompute Spark环境会预创建会话
        spark = SparkSession.getActiveSession()
        if spark:
            print(f"✅ 使用已存在的Spark会话，版本: {spark.version}")
            return spark
    except:
        pass

    # 如果没有预创建会话，则创建新会话
    print("🚀 创建新的MaxCompute Spark会话...")

    # 🔑 关键：MaxCompute不需要enableHiveSupport()
    # MaxCompute有自己的表格式和元数据管理，不依赖Hive MetaStore
    # 只有当DataWorks配置了Hive MetaStore映射时才需要启用
    builder = SparkSession.builder.appName("TagComputeEngine-MaxCompute")

    # 注意：如果DataWorks环境配置了Hive MetaStore映射MaxCompute表，可以启用：
    # builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ Spark会话创建完成，版本: {spark.version}")

    return spark


def load_config(environment: str) -> Dict[str, any]:
    """加载指定环境配置

    Args:
        environment: 环境标识 (dev/test/pre/prod)

    Returns:
        Dict: 环境配置
    """
    # 配置文件路径（DataWorks资源文件路径）
    possible_paths = [
        os.path.join(os.getcwd(), "config", "config.yaml"),
        os.path.join(os.getcwd(), "src", "config", "config.yaml"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "config.yaml"),
    ]

    config_file = None
    for path in possible_paths:
        if os.path.exists(path):
            config_file = path
            break

    if not config_file:
        raise FileNotFoundError(
            f"❌ 配置文件未找到\n尝试过的路径:\n" +
            "\n".join(f"  - {p}" for p in possible_paths)
        )

    with open(config_file, 'r', encoding='utf-8') as file:
        all_configs = yaml.safe_load(file)

    # 检查环境配置是否存在
    if environment not in all_configs:
        available_envs = list(all_configs.keys())
        raise ValueError(
            f"❌ 配置文件中未找到 {environment} 环境配置\n"
            f"可用环境: {available_envs}"
        )

    config = all_configs[environment]

    # 环境变量覆盖（支持接口传入敏感配置）
    if "mysql" in config:
        mysql = config["mysql"]
        mysql["host"] = os.getenv("MYSQL_HOST", mysql.get("host"))
        mysql["port"] = int(os.getenv("MYSQL_PORT", str(mysql.get("port", 3306))))
        mysql["database"] = os.getenv("MYSQL_DATABASE", mysql.get("database"))
        mysql["user"] = os.getenv("MYSQL_USER", mysql.get("user"))
        mysql["password"] = os.getenv("MYSQL_PASSWORD", mysql.get("password"))

    if "elasticsearch" in config:
        es = config["elasticsearch"]
        es_hosts = os.getenv("ES_HOSTS")
        if es_hosts:
            es["hosts"] = [h.strip() for h in es_hosts.split(",")]
        es["user"] = os.getenv("ES_USER", es.get("user", ""))
        es["password"] = os.getenv("ES_PASSWORD", es.get("password", ""))

    if "maxcompute" in config:
        mc = config["maxcompute"]
        mc["project"] = os.getenv("MAXCOMPUTE_PROJECT", mc.get("project"))
        mc["access_id"] = os.getenv("MAXCOMPUTE_ACCESS_ID", mc.get("access_id"))
        mc["access_key"] = os.getenv("MAXCOMPUTE_ACCESS_KEY", mc.get("access_key"))
        mc["endpoint"] = os.getenv("MAXCOMPUTE_ENDPOINT", mc.get("endpoint"))

    print(f"✅ 成功加载 {environment.upper()} 环境配置: {config_file}")
    return config


def compute_tags_by_ids(tag_ids: Optional[List[int]] = None) -> Dict[str, any]:
    """
    标签计算主接口（供后端API调用）

    Args:
        tag_ids: 标签ID列表，None表示计算所有标签

    Returns:
        Dict: 执行结果
        {
            "success": True/False,
            "message": "执行结果信息",
            "total_tags": 计算的标签总数,
            "failed_tags": [失败的标签ID列表],
            "computed_tags": [成功计算的标签ID列表]
        }
    """
    global _GLOBAL_ENVIRONMENT

    if _GLOBAL_ENVIRONMENT is None:
        raise RuntimeError(
            "❌ 环境未配置！请在启动时通过命令行参数指定 --env 参数\n"
            "示例: python main_dataworks.py --mode task-tags --tag-ids 1,2,3 --env prod"
        )

    print("=" * 70)
    print("🏷️  标签计算系统 - MaxCompute Spark 接口触发")
    print("=" * 70)
    print(f"环境: {_GLOBAL_ENVIRONMENT.upper()}")
    print(f"标签ID: {tag_ids if tag_ids else '全部标签'}")
    print(f"当前工作目录: {os.getcwd()}")

    result = {
        "success": False,
        "message": "",
        "total_tags": 0,
        "failed_tags": [],
        "computed_tags": []
    }

    spark = None
    tag_engine = None

    try:
        # 1. 加载配置
        print(f"\n🔧 加载 {_GLOBAL_ENVIRONMENT.upper()} 环境配置...")
        config = load_config(_GLOBAL_ENVIRONMENT)

        mysql_config = config.get("mysql", {})
        es_config = config.get("elasticsearch", None)
        maxcompute_config = config.get("maxcompute", {})

        print(f"MySQL: {mysql_config.get('host')}:{mysql_config.get('port')}/{mysql_config.get('database')}")
        if es_config:
            print(f"ES: {es_config.get('hosts')} / 索引: {es_config.get('index')}")
        if maxcompute_config:
            print(f"MaxCompute: 项目={maxcompute_config.get('project')}")

        # 2. 获取Spark会话
        spark = get_spark_session()

        # 3. 创建标签引擎
        print("\n🚀 初始化标签引擎...")
        tag_engine = TagEngine(
            spark,
            maxComputeConfig=maxcompute_config,
            mysqlConfig=mysql_config,
            esConfig=es_config
        )

        # 4. 执行标签计算
        if tag_ids:
            print(f"\n🎯 开始计算指定标签: {tag_ids}")
            mode = "task-tags"
            compute_success, failed_tag_ids = tag_engine.computeTags(mode=mode, tagIds=tag_ids)
        else:
            print("\n🚀 开始计算所有标签...")
            mode = "task-all"
            compute_success, failed_tag_ids = tag_engine.computeTags(mode=mode)

        # 5. 构建返回结果
        total_tags = len(tag_ids) if tag_ids else 0
        computed_tags = [tid for tid in (tag_ids or []) if tid not in failed_tag_ids]

        result["success"] = compute_success
        result["total_tags"] = total_tags
        result["failed_tags"] = failed_tag_ids
        result["computed_tags"] = computed_tags

        if failed_tag_ids:
            result["message"] = f"部分成功：{len(computed_tags)} 个标签计算成功，{len(failed_tag_ids)} 个标签失败"
            print(f"\n⚠️  {result['message']}")
            print(f"失败标签ID: {failed_tag_ids}")
        else:
            result["message"] = f"全部成功：{total_tags if total_tags > 0 else '所有'} 个标签计算完成"
            print(f"\n✅ {result['message']}")

        print("\n" + "=" * 70)
        print("✅ 任务执行完成")
        print("=" * 70)

    except Exception as e:
        error_msg = f"系统异常: {str(e)}"
        result["success"] = False
        result["message"] = error_msg

        print(f"\n❌ {error_msg}")
        import traceback
        traceback.print_exc()

    finally:
        # 清理资源
        if tag_engine:
            tag_engine.cleanup()

        # 注意：DataWorks托管的Spark会话不需要手动关闭
        print("🧹 资源清理完成")

    return result


def health_check() -> Dict[str, any]:
    """
    健康检查接口

    Returns:
        Dict: 健康检查结果
    """
    global _GLOBAL_ENVIRONMENT

    if _GLOBAL_ENVIRONMENT is None:
        raise RuntimeError(
            "❌ 环境未配置！请在启动时通过命令行参数指定 --env 参数\n"
            "示例: python main_dataworks.py --mode health --env test"
        )

    print("=" * 70)
    print("🔍 标签系统健康检查")
    print("=" * 70)
    print(f"环境: {_GLOBAL_ENVIRONMENT.upper()}")

    result = {
        "success": False,
        "checks": {},
        "message": ""
    }

    try:
        config = load_config(_GLOBAL_ENVIRONMENT)
        spark = get_spark_session()

        tag_engine = TagEngine(
            spark,
            maxComputeConfig=config.get("maxcompute", {}),
            mysqlConfig=config.get("mysql", {}),
            esConfig=config.get("elasticsearch", None)
        )

        health_ok = tag_engine.healthCheck()

        result["success"] = health_ok
        result["message"] = "系统健康" if health_ok else "系统异常"

        print(f"\n{'✅' if health_ok else '❌'} {result['message']}")

    except Exception as e:
        result["success"] = False
        result["message"] = f"健康检查失败: {str(e)}"
        print(f"\n❌ {result['message']}")

    finally:
        print("=" * 70)

    return result


def list_tasks() -> Dict[str, any]:
    """
    列出所有可用标签任务

    Returns:
        Dict: 任务列表结果
    """
    global _GLOBAL_ENVIRONMENT

    if _GLOBAL_ENVIRONMENT is None:
        raise RuntimeError(
            "❌ 环境未配置！请在启动时通过命令行参数指定 --env 参数\n"
            "示例: python main_dataworks.py --mode list-tasks --env prod"
        )

    print("=" * 70)
    print("📋 列出所有标签任务")
    print("=" * 70)
    print(f"环境: {_GLOBAL_ENVIRONMENT.upper()}")

    result = {
        "success": False,
        "tasks": [],
        "message": ""
    }

    try:
        config = load_config(_GLOBAL_ENVIRONMENT)
        spark = get_spark_session()
        mysql_config = config.get("mysql", {})

        # 导入MysqlMeta
        try:
            from tag_engine.meta.MysqlMeta import MysqlMeta
        except ImportError:
            from src.tag_engine.meta.MysqlMeta import MysqlMeta

        mysql_meta = MysqlMeta(spark, mysql_config)
        tags = mysql_meta.loadTagRules()

        print("\n可用标签任务:")
        task_list = []
        for tag in tags.collect():
            tag_info = {
                "tag_id": tag.tag_id,
                "tag_name": tag.tag_name if hasattr(tag, 'tag_name') else '未知标签'
            }
            task_list.append(tag_info)
            print(f"  {tag_info['tag_id']}: {tag_info['tag_name']}")

        result["success"] = True
        result["tasks"] = task_list
        result["message"] = f"共 {len(task_list)} 个标签任务"

        print(f"\n✅ {result['message']}")

    except Exception as e:
        result["success"] = False
        result["message"] = f"获取标签列表失败: {str(e)}"
        print(f"\n❌ {result['message']}")
        import traceback
        traceback.print_exc()

    finally:
        print("=" * 70)

    return result


# ========== 命令行入口 ==========

def main():
    """
    主程序入口函数

    支持命令行调用（DataWorks 任务配置）

    示例：
        # 健康检查
        python main_dataworks.py --mode health --env test

        # 计算指定标签
        python main_dataworks.py --mode task-tags --tag-ids 1,2,3 --env prod

        # 计算所有标签
        python main_dataworks.py --mode task-all --env prod

        # 列出所有标签
        python main_dataworks.py --mode list-tasks --env prod

    DataWorks 参数化配置：
        python main_dataworks.py --mode task-tags --tag-ids ${tag_ids} --env prod
    """
    global _GLOBAL_ENVIRONMENT

    import argparse

    parser = argparse.ArgumentParser(description="标签计算系统 - DataWorks版")
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["health", "task-all", "task-tags", "list-tasks"],
        help="执行模式: health(健康检查) / task-all(全量计算) / task-tags(指定标签) / list-tasks(列出任务)"
    )
    parser.add_argument(
        "--tag-ids",
        type=str,
        help="标签ID列表，逗号分隔，如: 1,2,3 (task-tags模式必需)"
    )
    parser.add_argument(
        "--env",
        "--environment",
        type=str,
        required=True,
        choices=["dev", "test", "pre", "prod"],
        help="环境配置: dev(开发) / test(测试) / pre(预发) / prod(生产) - 必须指定"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="详细输出模式"
    )

    args = parser.parse_args()

    # 设置全局环境
    _GLOBAL_ENVIRONMENT = args.env

    # 设置详细输出
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.INFO)

    print("=" * 70)
    print("🏷️  大数据标签计算系统 - DataWorks版")
    print("=" * 70)
    print(f"执行模式: {args.mode}")
    print(f"环境配置: {args.env.upper()}")
    if args.tag_ids:
        print(f"标签ID: {args.tag_ids}")
    print("=" * 70)

    # 根据模式执行
    result = None

    try:
        if args.mode == "health":
            result = health_check()

        elif args.mode == "task-all":
            result = compute_tags_by_ids(tag_ids=None)

        elif args.mode == "task-tags":
            if not args.tag_ids:
                print("❌ task-tags 模式需要提供 --tag-ids 参数")
                sys.exit(1)
            tag_ids = [int(tid.strip()) for tid in args.tag_ids.split(",")]
            result = compute_tags_by_ids(tag_ids=tag_ids)

        elif args.mode == "list-tasks":
            result = list_tasks()

        # 输出结果摘要
        print("\n" + "=" * 70)
        print("📊 执行结果摘要:")
        print("=" * 70)
        for key, value in result.items():
            if key != "tasks":  # tasks列表太长，不在摘要中显示
                print(f"  {key}: {value}")
        print("=" * 70)

        # 根据结果设置退出码
        sys.exit(0 if result["success"] else 1)

    except KeyboardInterrupt:
        print("\n⚠️  任务被用户中断")
        sys.exit(2)

    except Exception as e:
        print(f"\n❌ 系统异常: {e}")
        import traceback
        if args.verbose:
            traceback.print_exc()
        sys.exit(3)


if __name__ == "__main__":
    main()