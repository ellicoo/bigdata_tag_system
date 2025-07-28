# 大数据标签系统

基于PySpark的分布式标签计算系统，使用DSL和UDF从S3 Hive表读取用户数据，结合MySQL规则进行标签计算，专为海豚调度器部署设计。

## 项目架构

### 核心特性
- PySpark DSL + UDF：充分利用Spark DataFrame API和自定义用户函数
- 智能并行处理：基于表依赖关系的智能分组和并发计算  
- 标签合并机制：支持新老标签智能合并，避免数据丢失
- 海豚调度器集成：原生支持DolphinScheduler部署和调度
- 多环境支持：本地开发、测试、生产环境无缝切换

### 技术栈
- 计算引擎：PySpark 3.5+ (Spark SQL + DataFrame API)
- 数据源：S3 Hive Tables (Parquet格式)  
- 规则存储：MySQL (JSON格式规则)
- 调度系统：DolphinScheduler
- 部署方式：YARN Cluster模式

## 项目结构

```
src/tag_engine/
├── main.py                 # 命令行入口，支持多种执行模式
├── engine/                 # 核心计算引擎
│   ├── TagEngine.py       # 主编排引擎，工作流协调
│   └── TagGroup.py        # 智能分组，基于表依赖的并行处理
├── meta/                  # 数据源管理
│   ├── HiveMeta.py        # Hive表操作，智能缓存与优化
│   └── MysqlMeta.py       # MySQL规则和结果管理
├── parser/                # 规则解析与SQL生成
│   └── TagRuleParser.py   # JSON规则转SQL条件
└── utils/                 # 用户自定义函数
    └── TagUdfs.py         # PySpark UDF函数集合
```

## 快速开始

### 本地开发环境

```bash
# 1. 启动本地环境 (MySQL + MinIO)
cd environments/local
./setup.sh

# 2. 初始化数据库和测试数据
./init_data.sh

# 3. 运行健康检查
cd ../../
python src/tag_engine/main.py --mode health

# 4. 执行全量标签计算
python src/tag_engine/main.py --mode task-all

# 5. 计算指定标签
python src/tag_engine/main.py --mode task-tags --tag-ids 1,2,3
```

### 海豚调度器部署

```bash
# 1. 生成部署包
python dolphin_deploy_package.py

# 2. 上传到DolphinScheduler资源中心
# 将生成的ZIP包上传到海豚调度器资源管理

# 3. 创建Spark任务
# 主类: src.tag_engine.main
# 程序参数: --mode task-all
# 资源文件: bigdata_tag_system.zip
```

## 核心功能

### 1. 智能标签分组 (TagGroup.py)

系统根据标签规则的表依赖关系进行智能分组，实现最优并行计算：

```python
# 示例分组策略:
# 组1: 标签[1,2,3] → 依赖表[user_basic_info, user_asset_summary]  
# 组2: 标签[4,5] → 依赖表[user_activity_summary]
# 组3: 标签[6] → 依赖表[user_basic_info, user_activity_summary]

def computeTagGroup(self, tagGroup):
    # 1. 获取组内所有标签依赖的表
    # 2. 执行一次性JOIN操作
    # 3. 并行计算组内所有标签
    # 4. 返回用户标签结果
```

### 2. PySpark DSL应用

充分利用Spark DataFrame API进行分布式计算：

```python
# Hive表智能缓存和JOIN
cachedDF = spark.sql(f"SELECT * FROM {table_name}") \
               .persist(StorageLevel.MEMORY_AND_DISK)

# 标签条件过滤
tagDF = joinedDF.filter(expr(sqlCondition)) \
               .select("user_id") \
               .withColumn("tag_id", lit(tagId))

# 用户标签聚合
userTagsDF = tagResultsDF.groupBy("user_id").agg(
    tagUdfs.mergeUserTags(collect_list("tag_id")).alias("tag_ids")
)
```

### 3. 自定义UDF函数

提供专用的标签处理UDF：

```python
def mergeUserTags(tagList):
    """标签去重和排序"""
    if not tagList:
        return []
    uniqueTags = list(set(tagList))
    uniqueTags.sort()
    return uniqueTags

def mergeWithExistingTags(newTags, existingTags):
    """新老标签智能合并"""
    # 实现增量标签合并逻辑
```

### 4. JSON规则系统

支持复杂的业务规则定义：

```json
{
  "logic": "AND",
  "conditions": [
    {
      "fields": [
        {
          "table": "user_basic_info",
          "field": "age",
          "operator": ">=", 
          "value": 30,
          "type": "number"
        },
        {
          "table": "user_asset_summary", 
          "field": "total_assets",
          "operator": ">=",
          "value": 100000,
          "type": "number"
        }
      ]
    }
  ]
}
```

## 性能优化

### 1. 智能缓存策略
- 表级缓存：频繁访问的Hive表缓存到内存
- 分区优化：动态调整分区数提升并行度
- 字段投影：只加载必要字段减少I/O

### 2. 并行处理优化  
- 依赖分析：基于表依赖关系的智能分组
- 批量计算：同组标签并行计算，减少重复JOIN
- 资源管理：合理的Spark资源配置和内存管理

### 3. 数据写入优化
- 批量UPSERT：高效的MySQL批量更新
- 标签合并：智能的新老标签合并机制
- 时间戳管理：精确的创建和更新时间追踪

## 执行模式

系统支持多种执行模式，适配不同业务场景：

| 模式 | 命令 | 说明 |
|------|------|------|
| 健康检查 | --mode health | 检查Hive和MySQL连接状态 |
| 全量计算 | --mode task-all | 计算所有激活标签 |
| 指定标签 | --mode task-tags --tag-ids 1,2,3 | 计算指定标签ID |
| 测试数据生成 | --mode generate-test-data --dt 2025-01-20 | 生成测试数据 |
| 任务列表 | --mode list-tasks | 列出可用标签任务 |

## 海豚调度器集成

### 部署配置

```python
# environments/dolphinscheduler/config.py
class DolphinschedulerConfig(BaseConfig):
    def __init__(self):
        super().__init__(
            environment='dolphinscheduler',
            spark=SparkConfig(
                app_name="BigDataTagSystem-Dolphin",
                master="yarn",
                executor_memory="4g",
                driver_memory="2g",
                executor_cores=2,
                num_executors=10
            )
        )
```

### 任务调度示例

```bash
# 海豚调度器Spark任务配置
主类: src.tag_engine.main
程序参数: --mode task-all
部署模式: cluster  
驱动程序内存: 2g
执行器内存: 4g
执行器数量: 10
```

## 数据流架构

```
S3 Hive Tables → TagEngine → Smart Grouping → Parallel Computation → Tag Merging → MySQL Results
     ↓              ↓            ↓                   ↓                ↓             ↓
  用户数据        规则加载      依赖分析           并行标签计算         结果合并      持久化存储
```

### 数据模型

输入数据:
- user_basic_info: 用户基础信息 (年龄、性别、注册时间等)
- user_asset_summary: 用户资产汇总 (总资产、现金余额等)  
- user_activity_summary: 用户活动汇总 (交易次数、登录时间等)

输出结果:
- user_tags表: user_id → tag_ids (JSON数组格式: [1,2,3,5])

## 监控和运维

### 系统健康检查
```bash
python src/tag_engine/main.py --mode health
```

输出示例:
```
[INFO] Hive连接状态: ✓ 正常 (3个表可用)
[INFO] MySQL连接状态: ✓ 正常 (5个激活标签)
[INFO] 系统内存使用: 2.1GB / 8GB  
[INFO] Spark执行器状态: 10个执行器正常运行
```

### 性能指标
- 计算速度：100万用户 × 10个标签 ≈ 5-8分钟
- 内存效率：智能缓存 + 分区优化，内存使用率 < 70%
- 准确性：标签合并零丢失，支持幂等操作

## 开发指南

### 新增标签步骤

1. MySQL规则配置:
```sql
INSERT INTO tag_rules (tag_id, rule_content, status) VALUES 
(新标签ID, JSON规则, 'active');
```

2. 测试验证:
```bash
python src/tag_engine/main.py --mode task-tags --tag-ids 新标签ID
```

3. 海豚调度器部署:
```bash
python dolphin_deploy_package.py
# 上传新的部署包到资源中心
```

### 自定义UDF开发

```python
# 在TagUdfs.py中添加新的UDF
def customTagLogic(field1, field2):
    """自定义标签逻辑"""
    # 实现业务逻辑
    return result
```

## 让数据驱动业务，让标签创造价值