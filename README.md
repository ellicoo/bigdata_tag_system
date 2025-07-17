# 🏷️ 大数据标签系统

企业级的大数据标签计算系统，支持多环境部署（本地、AWS Glue开发、AWS Glue生产），通过PySpark从S3读取数据，结合MySQL中的规则进行并行标签计算，并将结果存储回MySQL。

## 🎯 系统功能

### 核心功能
- ✅ 从S3读取Hive表数据
- ✅ 从MySQL读取标签规则配置
- ✅ 基于规则引擎计算用户标签
- ✅ 支持标签合并和去重
- ✅ 将标签结果写入MySQL
- ✅ 完整的错误处理和重试机制

### 🚀 性能优化特性
- ✅ **多标签并行计算**：支持多个标签同时计算，大幅提升性能
- ✅ **智能缓存策略**：预缓存MySQL标签数据，使用 `persist(StorageLevel.MEMORY_AND_DISK)` 
- ✅ **分区优化写入**：根据数据量动态调整分区数，避免小文件问题

### 🔄 数据一致性保障
- ✅ **智能标签合并**：内存合并 + MySQL现有标签合并，确保标签一致性
- ✅ **UPSERT写入策略**：`INSERT ON DUPLICATE KEY UPDATE`，避免数据覆盖
- ✅ **时间戳管理**：自动维护 `created_time` 和 `updated_time`，不重复更新创建时间

### 📊 6种计算场景
- ✅ **场景1**: 全量用户打全量标签（`full-parallel`）
- ✅ **场景2**: 全量用户打指定标签（`tags-parallel`）- 支持标签合并
- ✅ **场景3**: 增量用户打全量标签（`incremental-parallel`）
- ✅ **场景4**: 增量用户打指定标签（`incremental-tags-parallel`）
- ✅ **场景5**: 指定用户打全量标签（`users-parallel`）
- ✅ **场景6**: 指定用户打指定标签（`user-tags-parallel`）- 支持标签合并

## 🏗️ 系统架构

```
bigdata_tag_system/
├── src/                          # 🔧 核心源码
│   ├── config/                   # 配置管理
│   ├── readers/                  # 数据读取器
│   ├── engine/                   # 标签计算引擎
│   ├── merger/                   # 数据合并器
│   ├── writers/                  # 结果写入器
│   └── scheduler/                # 主调度器
├── environments/                 # 🌍 环境配置
│   ├── local/                    # 本地Docker环境
│   ├── glue-dev/                 # AWS Glue开发环境
│   └── glue-prod/                # AWS Glue生产环境
├── tests/                        # 🧪 测试代码
├── docs/                         # 📚 文档
└── main.py                       # 📍 统一入口
```

## ⚡ 快速开始

### 🔧 环境要求

- Python 3.8+
- Docker & Docker Compose (本地环境)
- AWS CLI (Glue环境)

### 🚀 本地环境

```bash
# 1. 一键部署基础环境
cd environments/local
./setup.sh                    # 启动Docker服务 + 安装依赖

# 2. 一键初始化数据
./init_data.sh                # 初始化数据库 + 生成测试数据

# 3. 运行标签计算 - 6种并行计算场景
cd ../../
python main.py --env local --mode health                           # 健康检查
python main.py --env local --mode full-parallel                    # 场景1: 全量用户打全量标签
python main.py --env local --mode tags-parallel --tag-ids 1,3,5    # 场景2: 全量用户打指定标签
python main.py --env local --mode incremental-parallel --days 7    # 场景3: 增量用户打全量标签
python main.py --env local --mode incremental-tags-parallel --days 7 --tag-ids 1,3,5  # 场景4: 增量用户打指定标签
python main.py --env local --mode users-parallel --user-ids user_000001,user_000002    # 场景5: 指定用户打全量标签
python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,3,5  # 场景6: 指定用户打指定标签
```

**本地环境管理命令：**
```bash
# 部署管理
./setup.sh                    # 部署基础环境（默认）
./setup.sh start              # 启动已有环境
./setup.sh stop               # 停止环境
./setup.sh clean              # 清理环境

# 数据管理  
./init_data.sh                # 初始化数据（默认）
./init_data.sh reset          # 重置所有数据
./init_data.sh clean          # 清理数据
./init_data.sh db-only        # 仅初始化数据库
./init_data.sh data-only      # 仅生成测试数据
```

### ☁️ AWS Glue开发环境

```bash
# 1. 部署到Glue
cd environments/glue-dev
python deploy.py

# 2. 运行作业 - 6种并行计算场景
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=full-parallel'
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=tags-parallel,--tag-ids=1,3,5'
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=incremental-parallel,--days=7'
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=incremental-tags-parallel,--days=7,--tag-ids=1,3,5'
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=users-parallel,--user-ids=user_000001,user_000002'
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=user-tags-parallel,--user-ids=user_000001,user_000002,--tag-ids=1,3,5'
```

### 🏭 AWS Glue生产环境

```bash
# 1. 部署到Glue
cd environments/glue-prod  
python deploy.py

# 2. 运行作业 - 6种并行计算场景
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=full-parallel'
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=tags-parallel,--tag-ids=1,3,5'
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=incremental-parallel,--days=7'
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=incremental-tags-parallel,--days=7,--tag-ids=1,3,5'
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=users-parallel,--user-ids=user_000001,user_000002'
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=user-tags-parallel,--user-ids=user_000001,user_000002,--tag-ids=1,3,5'
```

## ⚙️ 配置说明

### 环境变量配置

```bash
# Spark配置
export SPARK_APP_NAME=TagComputeSystem
export SPARK_MASTER=local[4]
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MEMORY=2g

# S3配置
export S3_BUCKET=your-data-bucket
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key
export S3_ENDPOINT=http://localhost:9000  # 可选，用于minio
export S3_REGION=us-east-1

# MySQL配置
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_DATABASE=tag_system
export MYSQL_USERNAME=root
export MYSQL_PASSWORD=your-password

# 系统配置
export BATCH_SIZE=10000
export MAX_RETRIES=3
export ENABLE_CACHE=true
export LOG_LEVEL=INFO
```

### 代码配置

```python
from config.base_config import TagSystemConfig, SparkConfig, S3Config, MySQLConfig

config = TagSystemConfig(
    spark=SparkConfig(
        app_name="TagComputeSystem",
        master="local[4]",
        executor_memory="4g",
        driver_memory="2g"
    ),
    s3=S3Config(
        bucket="your-data-bucket",
        access_key="your-access-key",
        secret_key="your-secret-key"
    ),
    mysql=MySQLConfig(
        host="localhost",
        database="tag_system",
        username="root",
        password="password"
    )
)
```

## 📊 数据表结构

### MySQL标签规则表

```sql
-- 标签分类表
CREATE TABLE tag_category (
    id INT PRIMARY KEY AUTO_INCREMENT,
    category_name VARCHAR(50) NOT NULL,
    category_code VARCHAR(50) NOT NULL UNIQUE,
    status TINYINT DEFAULT 1,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 标签定义表
CREATE TABLE tag_definition (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tag_name VARCHAR(100) NOT NULL,
    tag_code VARCHAR(100) NOT NULL UNIQUE,
    category_id INT NOT NULL,
    tag_type ENUM('AUTO', 'MANUAL') NOT NULL,
    status TINYINT DEFAULT 1,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES tag_category(id)
);

-- 标签规则表
CREATE TABLE tag_rules (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tag_id INT NOT NULL,
    rule_name VARCHAR(100),
    rule_description TEXT,
    condition_logic ENUM('AND', 'OR', 'NOT') DEFAULT 'AND',
    rule_conditions JSON,
    target_table VARCHAR(100),
    target_fields TEXT,
    status TINYINT DEFAULT 1,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tag_id) REFERENCES tag_definition(id)
);

-- 用户标签结果表（重构后的一个用户一条记录设计）
CREATE TABLE user_tags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(100) NOT NULL COMMENT '用户ID',
    tag_ids JSON NOT NULL COMMENT '用户的所有标签ID数组 [1,2,3,5]',
    tag_details JSON COMMENT '标签详细信息 {"1": {"tag_name": "高净值用户"}}',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_user_id (user_id),
    INDEX idx_created_time (created_time),
    INDEX idx_updated_time (updated_time),
    UNIQUE KEY uk_user_id (user_id)
);
```

### S3 Hive表结构示例

```sql
-- 用户基础信息表 (S3: s3://bucket/warehouse/user_basic_info/)
user_basic_info:
- user_id: string
- register_time: timestamp  
- register_country: string
- kyc_status: string
- user_level: string
- updated_time: timestamp

-- 用户资产汇总表 (S3: s3://bucket/warehouse/user_asset_summary/) 
user_asset_summary:
- user_id: string
- total_asset_value: decimal
- total_deposit_amount: decimal
- total_withdraw_amount: decimal
- updated_time: timestamp

-- 用户活动汇总表 (S3: s3://bucket/warehouse/user_activity_summary/)
user_activity_summary:
- user_id: string
- last_login_time: timestamp
- login_count_7d: int
- trading_count_30d: int
- last_trading_time: timestamp
- updated_time: timestamp
```

## 🔧 标签规则配置

### 规则JSON格式

```json
{
  "logic": "AND",
  "conditions": [
    {
      "field": "total_asset_value",
      "operator": ">=",
      "value": 100000,
      "type": "number"
    },
    {
      "field": "kyc_status", 
      "operator": "=",
      "value": "verified",
      "type": "string"
    }
  ]
}
```

### 支持的操作符

| 操作符 | 说明 | 示例 |
|-------|------|------|
| `=` | 等于 | `{"field": "status", "operator": "=", "value": "active"}` |
| `!=` | 不等于 | `{"field": "status", "operator": "!=", "value": "inactive"}` |
| `>`, `<`, `>=`, `<=` | 数值比较 | `{"field": "amount", "operator": ">=", "value": 1000}` |
| `in` | 包含 | `{"field": "level", "operator": "in", "value": ["VIP1", "VIP2"]}` |
| `not_in` | 不包含 | `{"field": "country", "operator": "not_in", "value": ["US", "UK"]}` |
| `in_range` | 范围内 | `{"field": "age", "operator": "in_range", "value": [18, 65]}` |
| `contains` | 字符串包含 | `{"field": "email", "operator": "contains", "value": "@gmail"}` |
| `recent_days` | 最近N天 | `{"field": "login_time", "operator": "recent_days", "value": 7}` |
| `is_null` | 为空 | `{"field": "phone", "operator": "is_null"}` |
| `is_not_null` | 不为空 | `{"field": "phone", "operator": "is_not_null"}` |

## 📝 使用示例

### 示例1：高净值用户标签（全量用户计算）

```bash
# 运行场景2：全量用户打指定标签（支持与现有标签合并）
python main.py --env local --mode tags-parallel --tag-ids 1

# 查看结果
mysql -h 127.0.0.1 -P 3307 -u root -proot123 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '1') 
LIMIT 5;"
```

### 示例2：活跃用户标签（增量用户计算）

```bash
# 运行场景4：增量用户打指定标签
python main.py --env local --mode incremental-tags-parallel --days 7 --tag-ids 2

# 查看结果：活跃用户标签
mysql -h 127.0.0.1 -P 3307 -u root -proot123 -e "
USE tag_system;
SELECT user_id, 
       tag_ids, 
       JSON_EXTRACT(tag_details, '$.\"2\".tag_name') as tag_name,
       created_time, 
       updated_time 
FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '2') 
ORDER BY updated_time DESC 
LIMIT 5;"
```

### 示例3：指定用户多标签计算

```bash
# 运行场景6：指定用户打指定标签（支持与现有标签合并）  
python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,2,3

# 查看特定用户的标签变化
mysql -h 127.0.0.1 -P 3307 -u root -proot123 -e "
USE tag_system;
SELECT user_id, 
       tag_ids,
       JSON_LENGTH(tag_ids) as tag_count,
       created_time,
       updated_time,
       TIMESTAMPDIFF(SECOND, created_time, updated_time) as seconds_since_creation
FROM user_tags 
WHERE user_id IN ('user_000001', 'user_000002');"
```

## 🧪 测试

```bash
# 运行单元测试
python -m pytest tests/test_basic.py -v

# 运行示例测试
python run_examples.py validation

# 运行所有示例
python run_examples.py all
```

## 📈 性能优化

### Spark优化

```python
# 调整Spark参数
spark_config = SparkConfig(
    executor_memory="8g",
    driver_memory="4g", 
    shuffle_partitions=200,
    max_result_size="4g"
)
```

### 数据读取优化

```python
# 使用分区过滤
partition_filter = "updated_time >= '2024-01-01'"
data = hive_reader.read_table_data('user_asset_summary', partition_filter=partition_filter)

# 字段裁剪
required_fields = "user_id,total_asset_value,updated_time"
data = hive_reader.read_table_data('user_asset_summary', required_fields=required_fields)
```

### 缓存策略

```python
# 缓存热点数据
if table_name in ['user_basic_info', 'user_asset_summary']:
    df = df.cache()
```

## 🔍 监控和日志

### 日志配置

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('tag_system.log')
    ]
)
```

### 关键指标

- 标签计算执行时间
- 数据读取量和处理速度
- 标签命中率和覆盖率
- 系统资源使用情况

## ⚠️ 注意事项

1. **数据一致性**：确保S3数据和MySQL规则的一致性
2. **资源管理**：合理设置Spark资源参数，避免OOM
3. **错误处理**：重要操作都有重试机制和错误恢复
4. **数据备份**：写入前自动备份现有数据
5. **权限控制**：确保对S3和MySQL有足够的访问权限

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 📄 许可证

MIT License