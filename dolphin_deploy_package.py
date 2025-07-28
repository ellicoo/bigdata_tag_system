#!/usr/bin/env python3
"""
海豚调度器图形界面部署包生成器
基于现有S3 Hive能力，为海豚调度器图形界面生成部署包
"""

import os
import zipfile
import tempfile
from pathlib import Path


class DolphinGUIDeployPackager:
    """海豚调度器图形界面部署包生成器"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.output_dir = self.project_root / "dolphin_gui_deploy"
        self.output_dir.mkdir(exist_ok=True)
    
    
    def create_hive_test_tables(self) -> str:
        """创建Hive测试表SQL - 基于现有建表语句"""
        sql_content = '''-- 标签系统测试表
-- 基于现有 crate_table_demo.sql 格式

-- 用户基本信息表
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_basic_info (
    user_id string COMMENT '用户ID',
    age int COMMENT '年龄',
    user_level string COMMENT '用户等级',
    kyc_status string COMMENT 'KYC状态',
    registration_date string COMMENT '注册日期',
    risk_score double COMMENT '风险评分'
) COMMENT '用户基本信息表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_basic_info/';

-- 用户资产汇总表
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_asset_summary (
    user_id string COMMENT '用户ID',
    total_asset_value double COMMENT '总资产价值',
    cash_balance double COMMENT '现金余额'
) COMMENT '用户资产汇总表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_asset_summary/';

-- 用户活动汇总表
CREATE EXTERNAL TABLE IF NOT EXISTS tag_system.user_activity_summary (
    user_id string COMMENT '用户ID',
    trade_count_30d int COMMENT '30天交易次数',
    last_login_date string COMMENT '最后登录日期'
) COMMENT '用户活动汇总表'
PARTITIONED BY (`dt` string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://exchanges-flink-test/batch/data/tag_system/user_activity_summary/';
'''
        return sql_content
    
    def create_test_data_generator(self) -> str:
        """创建测试数据生成器"""
        generator_content = '''#!/usr/bin/env python3
"""
海豚调度器测试数据生成器
直接写入Hive表，基于现有Spark能力
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

def create_spark_session():
    """创建Spark会话 - 基于现有HiveToKafka.py模式"""
    spark = SparkSession.builder \\
        .appName("TagSystemTestDataGenerator") \\
        .enableHiveSupport() \\
        .getOrCreate()
    return spark

def generate_test_data(spark, dt='2025-01-20'):
    """生成测试数据并写入Hive表"""
    
    print(f"🚀 生成测试数据，日期: {dt}")
    
    # 生成用户基本信息测试数据
    user_basic_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        age = random.randint(18, 65)
        user_level = random.choice(['VIP1', 'VIP2', 'VIP3', 'NORMAL'])
        kyc_status = random.choice(['verified', 'pending', 'rejected'])
        registration_date = (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d')
        risk_score = random.uniform(0, 100)
        
        user_basic_data.append((user_id, age, user_level, kyc_status, registration_date, risk_score))
    
    # 创建DataFrame并写入Hive
    user_basic_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("age", IntegerType(), True), 
        StructField("user_level", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("risk_score", DoubleType(), True)
    ])
    
    user_basic_df = spark.createDataFrame(user_basic_data, user_basic_schema)
    user_basic_df = user_basic_df.withColumn("dt", lit(dt))
    
    # 写入Hive表
    user_basic_df.write \\
        .mode("overwrite") \\
        .partitionBy("dt") \\
        .saveAsTable("tag_system.user_basic_info")
    
    print("✅ 用户基本信息测试数据生成完成")
    
    # 生成用户资产数据
    user_asset_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        total_asset = random.uniform(1000, 1000000)
        cash_balance = random.uniform(100, total_asset * 0.5)
        
        user_asset_data.append((user_id, total_asset, cash_balance))
    
    user_asset_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("total_asset_value", DoubleType(), True),
        StructField("cash_balance", DoubleType(), True)
    ])
    
    user_asset_df = spark.createDataFrame(user_asset_data, user_asset_schema)
    user_asset_df = user_asset_df.withColumn("dt", lit(dt))
    
    user_asset_df.write \\
        .mode("overwrite") \\
        .partitionBy("dt") \\
        .saveAsTable("tag_system.user_asset_summary")
    
    print("✅ 用户资产测试数据生成完成")
    
    # 生成用户活动数据
    user_activity_data = []
    for i in range(1000):
        user_id = f"user_{i:06d}"
        trade_count = random.randint(0, 100)
        last_login = (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
        
        user_activity_data.append((user_id, trade_count, last_login))
    
    user_activity_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("trade_count_30d", IntegerType(), True),
        StructField("last_login_date", StringType(), True)
    ])
    
    user_activity_df = spark.createDataFrame(user_activity_data, user_activity_schema)
    user_activity_df = user_activity_df.withColumn("dt", lit(dt))
    
    user_activity_df.write \\
        .mode("overwrite") \\
        .partitionBy("dt") \\
        .saveAsTable("tag_system.user_activity_summary")
    
    print("✅ 用户活动测试数据生成完成")
    
    # 验证数据
    print("\\n📊 数据验证:")
    print(f"用户基本信息表记录数: {spark.table('tag_system.user_basic_info').count()}")
    print(f"用户资产表记录数: {spark.table('tag_system.user_asset_summary').count()}")
    print(f"用户活动表记录数: {spark.table('tag_system.user_activity_summary').count()}")

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        # 创建数据库
        spark.sql("CREATE DATABASE IF NOT EXISTS tag_system")
        print("✅ 数据库 tag_system 创建成功")
        
        # 生成测试数据
        generate_test_data(spark)
        
        print("🎉 测试数据生成完成！")
        
    finally:
        spark.stop()
'''
        return generator_content
    
    def create_main_entry(self) -> str:
        """创建主程序入口 - 用于海豚调度器主程序参数"""
        main_content = '''#!/usr/bin/env python3
"""
海豚调度器主程序入口
支持通过海豚调度器图形界面的主程序参数执行
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
    spark = SparkSession.builder \\
        .appName("BigDataTagSystem-Dolphin") \\
        .enableHiveSupport() \\
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
            
            # 检查MySQL连接
            try:
                from mysql_config import DolphinMySQLConfig
                config = DolphinMySQLConfig()
                
                # 测试MySQL连接
                mysql_df = spark.read \\
                    .format("jdbc") \\
                    .option("url", config.get_connection_url()) \\
                    .option("driver", "com.mysql.cj.jdbc.Driver") \\
                    .option("user", config.MYSQL_USERNAME) \\
                    .option("password", config.MYSQL_PASSWORD) \\
                    .option("query", "SELECT 1 as test") \\
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
'''
        return main_content
    
    def create_optimized_main_entry(self) -> str:
        """创建优化的主程序入口 - 直接调用src/tag_engine/main.py"""
        main_content = '''#!/usr/bin/env python3
"""
海豚调度器主程序入口 - 统一调用入口
直接调用src/tag_engine/main.py，避免重复代码
"""

import sys
import os

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

def main():
    """主程序入口 - 直接调用核心main.py"""
    print("🐬 海豚调度器标签系统 - 统一入口")
    print("📡 调用核心标签引擎...")
    
    try:
        # 直接调用src/tag_engine/main.py
        from src.tag_engine.main import main as core_main
        
        # 设置默认应用名称为海豚调度器版本
        if '--app-name' not in sys.argv:
            sys.argv.extend(['--app-name', 'BigDataTagSystem-Dolphin'])
            
        # 调用核心main函数
        core_main()
        
    except ImportError as e:
        print(f"❌ 无法导入核心模块: {e}")
        print("请确保src/tag_engine目录存在并包含所需模块")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ 系统异常: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
'''
        return main_content
    
    def create_optimized_deploy_guide(self, custom_extract_path: str = None) -> str:
        """创建优化的部署指南"""
        extract_path = custom_extract_path or "/dolphinscheduler/default/resources/"
        
        guide = f'''# 🐬 海豚调度器图形界面部署指南

## 📦 部署包内容（精简版）
- `main.py` - 主程序入口（使用src.config.base.MySQLConfig统一配置）
- `src/` - 项目源码（直接读取Hive表，移除S3依赖）
- `generate_test_data.py` - 测试数据生成器
- `create_test_tables.sql` - 测试表创建SQL
- `requirements.txt` - Python依赖

## 🚀 UI界面部署步骤（推荐）

### 1. 上传ZIP包到资源中心
1. 登录海豚调度器Web界面
2. 进入 **资源中心** → **文件管理**
3. 上传 `tag_system_dolphin.zip`

### 2. 直接在资源中心解压
1. 在资源中心中右键点击上传的`tag_system_dolphin.zip`
2. 选择解压，或者创建Shell任务解压：
```bash
#!/bin/bash
cd {extract_path}
unzip -o tag_system_dolphin.zip
echo "✅ 标签系统部署包解压完成到: {extract_path}"
```

### 3. 创建标签计算工作流
1. 创建新工作流："标签计算任务"
2. 添加Spark节点：
   - **主程序**: `{extract_path}main.py`
   - **主程序参数**: `--mode health`
   - **Spark任务名称**: BigDataTagSystem-Dolphin

### 4. Spark任务配置
**基础配置**:
- Driver核心数: 2
- Driver内存: 2g
- Executor数量: 3
- Executor内存: 4g
- Executor核心数: 2

**高级配置**:
- YARN队列: default
- 主程序参数示例:
  - 健康检查: `--mode health`
  - 全量标签: `--mode task-all`
  - 指定标签: `--mode task-tags --tag-ids 1,2,3`

## 🎯 支持的执行模式

### 健康检查模式
```bash
--mode health
```
验证Hive和MySQL连接

### 测试数据生成
```bash
--mode generate-test-data --dt 2025-01-20
```
生成测试数据到Hive表

### 全量标签计算
```bash
--mode task-all
```
计算所有用户的所有标签

### 指定标签计算
```bash
--mode task-tags --tag-ids 1,2,3
```
计算指定标签ID的标签

### 任务列表查看
```bash
--mode list-tasks
```
查看所有可用的标签任务

## 💡 最佳实践

### 1. 参数化工作流
在工作流中使用全局参数:
- `tag_ids`: 标签ID列表
- `data_date`: 数据日期
- `mode`: 执行模式

### 2. 依赖管理
无需额外安装Python依赖，使用集群预装的PySpark环境

### 3. 错误处理
- 设置任务失败重试次数: 2
- 设置任务超时时间: 30分钟
- 配置告警通知

### 4. 监控建议
- 定期执行健康检查任务
- 监控MySQL标签数据增长
- 查看Spark UI资源使用情况

## 🔧 故障排除

### MySQL连接问题
- 检查网络连通性
- 验证用户名密码
- 确认数据库权限

### Hive表访问问题
- 验证表是否存在
- 检查分区数据
- 确认权限配置

### 性能优化
- 根据数据量调整Executor配置
- 考虑增加并行度
- 优化SQL查询逻辑
'''
        return guide
    
    def create_requirements(self) -> str:
        """创建requirements.txt（海豚调度器环境可选依赖）"""
        requirements = '''# 海豚调度器标签系统依赖
# 注意：PySpark通常已在集群环境中预装，无需安装

# 必需依赖（如果集群环境缺少）
pymysql>=1.0.0       # MySQL连接器

# 可选依赖（通常集群已有）
# pyspark>=3.2.0     # Spark分布式计算引擎（集群预装）
# pandas>=1.3.0      # 数据分析库（集群预装）

# 安装命令（仅在需要时执行）:
# pip3 install pymysql
'''
        return requirements
    
    def create_deploy_guide(self) -> str:
        """创建部署指南"""
        guide = '''# 🐬 海豚调度器图形界面部署指南

## 📦 部署包内容
- `main.py` - 主程序入口
- `src/` - 项目源码
- `generate_test_data.py` - 测试数据生成器
- `create_test_tables.sql` - 测试表创建SQL
- `requirements.txt` - Python依赖

## 🚀 部署步骤

### 1. 上传到资源中心
1. 登录海豚调度器Web界面
2. 进入 **资源中心** → **文件管理**
3. 上传 `tag_system_dolphin.zip`
4. 直接在资源中心解压到 `/dolphinscheduler/default/resources/`

### 2. 依赖管理
通常无需安装额外依赖，集群环境已预装PySpark。
如需要，可创建Shell任务：
```bash
#!/bin/bash
pip3 install pymysql
echo "✅ 安装MySQL连接器完成"
```

### 3. 创建测试表和数据
创建Spark任务执行：
```bash
# 主程序参数
--mode generate-test-data --dt 2025-01-20

# 或者先创建表
spark-sql -f create_test_tables.sql
```

### 4. 健康检查
创建Spark任务测试：
```bash
# 主程序参数  
--mode health
```

## 🎯 任务配置

### Spark任务配置（在海豚图形界面中）
- **主程序**: `/dolphinscheduler/default/resources/main.py`
- **主程序参数**: `--mode health` (根据需要调整)
- **Driver核心数**: 2
- **Driver内存**: 2g
- **Executor数量**: 5
- **Executor内存**: 4g
- **Executor核心数**: 2
- **YARN队列**: default

### 常用主程序参数
```bash
# 健康检查
--mode health

# 生成测试数据
--mode generate-test-data --dt 2025-01-20

# 全量标签计算
--mode task-all

# 指定标签计算
--mode task-tags --tag-ids 1,2,3

# 列出可用任务
--mode list-tasks
```

## 🔧 Java接口集成（后续）
项目支持通过Java接口触发：
```java
// 通过海豚调度器API触发Spark任务
DolphinSchedulerClient client = new DolphinSchedulerClient();
client.triggerWorkflow("tag_system_compute", Map.of(
    "mode", "task-tags",
    "tag_ids", "1,2,3"
));
```

## 📊 监控和日志
- 通过海豚调度器UI查看任务执行状态
- Spark UI监控资源使用情况
- 任务日志在海豚调度器中查看
'''
        return guide
    
    def create_zip_package(self, custom_extract_path: str = None):
        """创建ZIP部署包，支持自定义解压路径"""
        zip_path = self.output_dir / "tag_system_dolphin.zip"
        
        print("📦 创建海豚调度器部署包...")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            
            # 添加src目录
            src_dir = self.project_root / "src"
            if src_dir.exists():
                for file_path in src_dir.rglob("*.py"):
                    arc_name = f"src/{file_path.relative_to(src_dir)}"
                    zip_file.write(file_path, arc_name)
                    print(f"  ✅ 添加源码: {arc_name}")
            
            # 添加优化后的主程序入口（使用统一MySQL配置）
            main_content = self.create_optimized_main_entry()
            zip_file.writestr("main.py", main_content)
            print("  ✅ 添加主程序: main.py")
            
            # 添加测试数据生成器
            test_generator = self.create_test_data_generator()
            zip_file.writestr("generate_test_data.py", test_generator)
            print("  ✅ 添加测试数据生成器: generate_test_data.py")
            
            # 添加建表SQL
            create_tables_sql = self.create_hive_test_tables()
            zip_file.writestr("create_test_tables.sql", create_tables_sql)
            print("  ✅ 添加建表SQL: create_test_tables.sql")
            
            # 添加依赖文件
            requirements = self.create_requirements()
            zip_file.writestr("requirements.txt", requirements)
            print("  ✅ 添加依赖: requirements.txt")
        
        print(f"\n🎉 部署包创建完成!")
        print(f"📁 输出目录: {self.output_dir}")
        print(f"📦 ZIP包: {zip_path}")
        print(f"📋 大小: {zip_path.stat().st_size / 1024:.1f} KB")
        
        return zip_path

def main():
    """主函数"""
    packager = DolphinGUIDeployPackager()
    zip_path = packager.create_zip_package()
    
    print(f"\n📋 后续步骤:")
    print(f"1. 上传 {zip_path} 到海豚调度器资源中心")
    print(f"2. 直接在资源中心解压到 /dolphinscheduler/default/resources/")
    print(f"3. 按照 dolphin_gui_deploy/部署说明.md 进行配置")
    print(f"4. 创建Spark任务，主程序路径：/dolphinscheduler/default/resources/main.py")

if __name__ == "__main__":
    main()