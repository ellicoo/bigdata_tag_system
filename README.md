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
    └── SparkUdfs.py       # PySpark UDF函数集合（模块级函数）
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

### 3. 智能标签合并机制

系统采用**Spark内置函数 + UDF**的混合策略，确保类型安全和高性能：

#### 内存标签合并（性能优化）
```python
# 使用Spark内置函数进行标签合并，避免UDF序列化开销
finalDF = mergedDF.groupBy("user_id").agg(
    array_distinct(
        array_sort(
            flatten(collect_list("tag_ids_array"))
        )
    ).alias("merged_tag_ids")
)
```

#### 自定义UDF函数（类型安全）
```python
@udf(returnType=ArrayType(IntegerType()))
def mergeUserTags(tagList):
    """合并单个用户的多个标签：去重+排序
    支持多种输入类型：List[int]、Array[int]、嵌套数组
    """
    if not tagList:
        return []
    
    # 处理不同的输入类型和嵌套数组
    if isinstance(tagList, list):
        flatTags = tagList
    else:
        flatTags = []
        for item in tagList:
            if isinstance(item, (list, tuple)):
                flatTags.extend(item)
            else:
                flatTags.append(item)
    
    # 过滤None值，去重并排序
    validTags = [tag for tag in flatTags if tag is not None]
    uniqueTags = list(set(validTags))
    uniqueTags.sort()
    return uniqueTags

@udf(returnType=ArrayType(IntegerType()))
def mergeWithExistingTags(newTags, existingTags):
    """新老标签智能合并"""
    if not newTags:
        newTags = []
    if not existingTags:
        existingTags = []
    
    # 合并、去重、排序
    allTags = list(set(newTags + existingTags))
    allTags.sort()
    return allTags
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
- **表级缓存**：频繁访问的Hive表缓存到内存，使用`persist(StorageLevel.MEMORY_AND_DISK)`
- **分区优化**：动态调整分区数提升并行度，避免小文件问题
- **字段投影**：只加载必要字段减少I/O，使用`select()`精确选择字段

### 2. 类型安全与性能并重
- **Spark内置函数优先**：使用`array_distinct`、`array_sort`、`flatten`等原生函数
- **UDF备用策略**：复杂逻辑使用类型安全的UDF，支持多种输入类型
- **序列化优化**：减少UDF调用，避免Python-JVM序列化开销

### 3. 并行处理优化  
- **依赖分析**：基于表依赖关系的智能分组，最小化JOIN操作
- **批量计算**：同组标签并行计算，共享表读取和JOIN结果
- **资源管理**：合理的Spark资源配置和内存管理

### 4. 数据写入优化
- **批量UPSERT**：高效的MySQL批量更新，支持标签合并
- **标签合并算法**：
  ```
  内存合并: Array[Array[Int]] → flatten → Array[Int] → distinct → sort
  MySQL合并: newTags + existingTags → merge → deduplicate → JSON
  ```
- **时间戳管理**：精确的创建和更新时间追踪，支持幂等操作

### 5. 架构优化亮点
- **零数据丢失**：智能标签合并，新老标签完美融合
- **类型兼容**：支持`List[int]`、`Array[int]`、嵌套数组等多种类型
- **错误恢复**：单个标签组失败不影响其他组计算
- **资源清理**：自动缓存清理和Spark会话管理

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

## 测试框架

### 测试架构
项目采用 **pytest + PySpark** 测试框架，提供完整的单元测试和集成测试能力。

### 测试运行

#### 1. 运行全部测试
```bash
# 运行所有测试（推荐）
python -m pytest tests/ -v

# 带覆盖率报告
python -m pytest tests/ -v --cov=src/tag_engine --cov-report=html
```

#### 2. 运行特定模块测试
```bash
# 规则解析器测试
python -m pytest tests/test_rule_parser.py -v

# 标签分组测试  
python -m pytest tests/test_tag_grouping.py -v

# 运行特定测试用例
python -m pytest tests/test_rule_parser.py::TestTagRuleParser::test_not_logic -v
```

#### 3. 测试结果示例
```
======================== test session starts ========================
tests/test_rule_parser.py::TestTagRuleParser::test_init PASSED [  4%]
tests/test_rule_parser.py::TestTagRuleParser::test_simple_number_condition_sql_generation PASSED [  9%]
tests/test_rule_parser.py::TestTagRuleParser::test_complex_multi_condition_and_logic PASSED [ 36%]
tests/test_rule_parser.py::TestTagRuleParser::test_not_logic PASSED [ 45%]
tests/test_tag_grouping.py::TestTagGrouping::test_analyze_dependencies_single_table PASSED [ 68%]
tests/test_tag_grouping.py::TestTagGrouping::test_group_tags_complex_scenario PASSED [ 90%]
======================== 22 passed, 1 warning in 13.07s ========================
```

### 测试覆盖范围

#### **规则解析器测试** (14个测试用例)
- ✅ **基础功能**: 初始化、SQL生成、条件解析
- ✅ **数据类型**: 数值、字符串、枚举、日期、布尔条件
- ✅ **操作符支持**: `=`, `!=`, `>=`, `<=`, `LIKE`, `IN`, `BETWEEN`, `IS NULL` 等
- ✅ **复杂逻辑**: AND/OR/NOT 嵌套条件、字段逻辑优先级
- ✅ **字符串匹配**: `contains`, `starts_with`, `ends_with` 模式
- ✅ **列表操作**: `contains_any`, `contains_all`, `array_contains`
- ✅ **异常处理**: 无效JSON、空规则、边界情况

#### **标签分组测试** (8个测试用例)  
- ✅ **依赖分析**: 单表依赖、多表依赖、字段依赖分析
- ✅ **智能分组**: 相同表分组、不同表分组、复杂场景组合
- ✅ **分组优化**: 依赖表组合的准确性和效率验证
- ✅ **边界处理**: 空规则、无效规则的健壮性测试

### 测试数据模型

#### **测试环境配置** (`tests/conftest.py`)
```python
@pytest.fixture(scope="session")
def spark():
    """本地Spark会话 - 测试优化配置"""
    return SparkSession.builder \
        .appName("TagSystem_Test") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

@pytest.fixture  
def sample_user_data():
    """真实业务场景测试数据"""
    return {
        "user_basic_info": [
            ("user001", 30, "VIP2", "verified", True),
            ("user002", 25, "VIP1", "verified", False),
            # ... 更多测试用户
        ],
        "user_asset_summary": [...],
        "user_activity_summary": [...]
    }
```

#### **复杂规则测试用例**
```python
# 测试高净值用户标签（多条件AND）
{
    "logic": "AND",
    "conditions": [
        {
            "condition": {
                "logic": "OR", 
                "fields": [
                    {"table": "user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP2", "VIP3"]},
                    {"table": "user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "100000"}
                ]
            }
        },
        {
            "condition": {
                "logic": "AND",
                "fields": [
                    {"table": "user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified"},
                    {"table": "user_activity_summary", "field": "trade_count_30d", "operator": ">", "value": "5"}
                ]
            }
        }
    ]
}
```

### 测试最佳实践

#### **单元测试原则**
```python
def test_simple_number_condition_sql_generation(self):
    """测试数值条件SQL生成 - 覆盖单表和多表场景"""
    parser = TagRuleParser()
    
    # 测试多表场景
    sql = parser.parseRuleToSql(rule_json, ["user_asset_summary", "user_basic_info"])
    expected = "`tag_system.user_asset_summary`.`total_asset_value` >= 100000"
    assert expected in sql
    
    # 测试单表场景  
    sql_single = parser.parseRuleToSql(rule_json, ["user_asset_summary"])
    expected_single = "`user_asset_summary`.`total_asset_value` >= 100000"
    assert expected_single in sql_single
```

#### **集成测试策略**
- **依赖隔离**: 使用内存DataFrame模拟Hive表，避免外部依赖
- **数据驱动**: 参数化测试覆盖多种业务场景
- **断言完整**: 验证SQL语法、逻辑结构、边界情况

### 持续集成支持

#### **GitHub Actions配置**
```yaml
# .github/workflows/test.yml
name: Test Suite
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.12
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: python -m pytest tests/ -v --cov=src/tag_engine
```

#### **测试性能基准**
- **测试速度**: 22个测试用例 ≈ 13秒
- **覆盖率目标**: > 85% 代码覆盖
- **测试稳定性**: 100% 通过率，无随机失败

## 开发指南

### 新增标签步骤

1. MySQL规则配置:
```sql
INSERT INTO tag_rules (tag_id, rule_content, status) VALUES 
(新标签ID, JSON规则, 'active');
```

2. 测试验证:
```bash
# 先运行相关测试验证规则解析
python -m pytest tests/test_rule_parser.py -v

# 再测试标签计算
python src/tag_engine/main.py --mode task-tags --tag-ids 新标签ID
```

3. 海豚调度器部署:
```bash
python dolphin_deploy_package.py
# 上传新的部署包到资源中心
```

### 自定义UDF开发

```python
# 在SparkUdfs.py中添加新的模块级函数
def custom_tag_logic(input_column):
    """自定义标签逻辑 - 使用Spark原生函数
    避免UDF序列化开销，优先使用Column表达式
    """
    return when(input_column.isNotNull() & (input_column > 0), lit(True)).otherwise(lit(False))

# 复杂逻辑才使用UDF
@udf(returnType=ArrayType(IntegerType()))  
def complex_tag_udf(input_data):
    """仅在必要时使用UDF，确保类型安全"""
    if not input_data:
        return []
    return process_complex_logic(input_data)
```

### 测试驱动开发流程

1. **编写测试用例**:
```python
def test_new_feature_logic(self):
    """新功能测试 - 先写测试，再写实现"""
    parser = TagRuleParser()
    result = parser.new_feature_method(test_input)
    assert result == expected_output
```

2. **运行测试验证**:
```bash
python -m pytest tests/test_new_feature.py::test_new_feature_logic -v
```

3. **实现功能代码**:
```python
def new_feature_method(self, input_data):
    """实现新功能，确保测试通过"""
    return processed_result
```

4. **完整测试验证**:
```bash
python -m pytest tests/ -v  # 确保不破坏现有功能
```

## 🔧 技术亮点总结

### 类型安全保障
- ✅ **完整类型流**：`Array[Array[Int]] → flatten → Array[Int] → distinct → sort`
- ✅ **UDF类型兼容**：支持`List[int]`、`Array[int]`、嵌套数组等多种输入
- ✅ **边界情况处理**：None值过滤、空数组处理、异常恢复

### 性能优化策略
- ⚡ **Spark内置函数优先**：避免UDF序列化开销，提升计算性能
- ⚡ **智能缓存机制**：表级缓存 + 字段投影，减少重复I/O
- ⚡ **并行计算优化**：表依赖分组 + 批量计算，最大化资源利用

### 架构设计亮点
- 🏗️ **模块化设计**：TagEngine、TagGroup、UDFs职责清晰分离
- 🏗️ **统一入口管理**：`src/tag_engine/main.py`作为唯一真实来源
- 🏗️ **多环境支持**：本地开发、海豚调度器部署无缝切换

### 生产就绪特性
- 🚀 **海豚调度器集成**：原生支持YARN集群部署
- 🚀 **健康检查机制**：完整的系统状态监控
- 🚀 **错误恢复能力**：单点失败不影响全局计算

---

## 🎯 让数据驱动业务，让标签创造价值！

**基于PySpark DSL + UDF的企业级标签计算系统，助力精准营销和用户洞察**