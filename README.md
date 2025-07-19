# 🏷️ 大数据标签系统

企业级的分布式标签计算系统，支持任务化架构和RESTful API接口。通过PySpark从S3读取数据，结合MySQL中的规则进行并行标签计算，并将结果存储回MySQL。支持多环境部署（本地、AWS Glue开发、AWS Glue生产）。

## 🎯 系统特色

### 🚀 任务化架构
- ✅ **任务抽象化**：每个标签都是独立的任务类，支持分布式开发
- ✅ **任务工厂模式**：自动注册和管理所有标签任务
- ✅ **任务并行引擎**：支持多任务并行执行，提升计算效率
- ✅ **MySQL规则驱动**：任务类从MySQL读取规则，不需要硬编码业务逻辑

### 🌐 RESTful API接口
- ✅ **异步任务触发**：后端可通过HTTP请求触发标签计算，立即返回不阻塞
- ✅ **任务状态跟踪**：完整的任务生命周期管理（submitted/running/completed/failed）
- ✅ **标签ID映射**：支持指定标签ID列表触发对应任务类
- ✅ **并发控制**：支持多任务并发执行，线程池管理

### 🔧 核心功能
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
- ✅ **UPSERT时间戳机制**：`created_time` 永远不变，`updated_time` 只在标签内容实际变化时更新
- ✅ **幂等性保证**：相同操作重复执行不会触发不必要的时间戳更新

### 🎯 任务化架构执行模式
- ✅ **健康检查**：`health`，系统健康检查
- ✅ **任务列表**：`list-tasks`，列出所有可用任务类
- ✅ **任务化全量标签**：`task-all`，执行所有注册的任务类
- ✅ **任务化指定标签**：`task-tags`，执行指定标签对应的任务类
- ✅ **任务化指定用户标签**：`task-users`，执行指定用户指定标签的任务类
- ✅ **API触发场景**：通过RESTful API触发指定标签任务

## 🏗️ 系统架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        大数据标签系统                              │
├─────────────────────────────────────────────────────────────────┤
│                     RESTful API 层                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  标签任务触发    │  │  任务状态查询    │  │  标签管理接口    │  │
│  │   API接口       │  │   API接口       │  │   API接口       │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                     任务化架构层                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   任务工厂       │  │  任务并行引擎    │  │  异步任务管理    │  │
│  │   TaskFactory   │  │ TaskParallel    │  │  TaskManager    │  │
│  └─────────────────┘  │    Engine       │  └─────────────────┘  │
│                       └─────────────────┘                       │
├─────────────────────────────────────────────────────────────────┤
│                     业务任务层                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   财富管理       │  │   行为分析       │  │   风险管理       │  │
│  │  WealthTasks    │  │ BehaviorTasks   │  │  RiskTasks      │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   生命周期       │  │   人口特征       │  │   价值管理       │  │
│  │ LifecycleTasks  │  │DemographicTasks │  │  ValueTasks     │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                     计算引擎层                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   规则引擎       │  │   标签合并       │  │   并行计算       │  │
│  │  RuleEngine     │  │  TagMerger      │  │ ParallelEngine  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                     数据访问层                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   S3数据读取     │  │  MySQL规则读取   │  │  MySQL结果写入   │  │
│  │   HiveReader    │  │   RuleReader    │  │  MySQLWriter    │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                     基础设施层                                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   Apache Spark  │  │   Amazon S3     │  │    MySQL        │  │
│  │   计算引擎       │  │   数据湖        │  │   规则&结果     │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 项目结构

```
bigdata_tag_system/
├── src/                          # 🔧 核心源码
│   ├── api/                      # RESTful API接口
│   │   ├── tag_trigger_api.py    # 标签任务触发API
│   │   └── task_manager.py       # 异步任务管理器
│   ├── config/                   # 配置管理
│   ├── readers/                  # 数据读取器
│   ├── engine/                   # 标签计算引擎
│   │   └── task_parallel_engine.py  # 任务并行引擎
│   ├── tasks/                    # 任务类架构
│   │   ├── base_tag_task.py      # 任务抽象基类
│   │   ├── task_factory.py       # 任务工厂
│   │   ├── wealth/               # 财富相关任务
│   │   ├── behavior/             # 行为相关任务
│   │   ├── risk/                 # 风险相关任务
│   │   ├── demographic/          # 人口特征任务
│   │   ├── lifecycle/            # 生命周期任务
│   │   └── value/                # 价值相关任务
│   ├── merger/                   # 数据合并器
│   ├── writers/                  # 结果写入器
│   └── scheduler/                # 主调度器
├── environments/                 # 🌍 环境配置
│   ├── local/                    # 本地Docker环境
│   ├── glue-dev/                 # AWS Glue开发环境
│   └── glue-prod/                # AWS Glue生产环境
├── tests/                        # 🧪 测试代码
├── docs/                         # 📚 文档
│   ├── API_USAGE.md              # API使用文档
│   ├── TASK_ARCHITECTURE.md      # 任务架构设计文档
│   └── 标准需求文档.md            # 业务需求文档
├── main.py                       # 📍 统一入口
├── api_server.py                 # 🌐 API服务器启动脚本
└── CLAUDE.md                     # 🤖 AI助手项目说明
```

## ⚡ 快速开始

### 🔧 环境要求

- Python 3.8+
- Docker & Docker Compose (本地环境)
- AWS CLI (Glue环境)
- Flask (API服务器)

### 🚀 本地环境

```bash
# 1. 一键部署基础环境
cd environments/local
./setup.sh                    # 启动Docker服务 + 安装依赖

# 2. 一键初始化数据
./init_data.sh                # 初始化数据库 + 生成测试数据

# 3. 运行标签计算 - 任务化架构
cd ../../
python main.py --env local --mode health                           # 健康检查
python main.py --env local --mode list-tasks                       # 列出所有可用任务
python main.py --env local --mode task-all                         # 任务化全量用户全量标签计算（执行所有任务）
python main.py --env local --mode task-tags --tag-ids 1,3,5        # 任务化全量用户指定标签计算（执行指定任务）
python main.py --env local --mode task-users --user-ids user_000001,user_000002 --tag-ids 1,3,5  # 任务化指定用户指定标签计算
```

### 📋 本地服务信息

部署完成后，以下服务将可用：

| 服务 | 地址 | 用户名 | 密码 | 说明 |
|------|------|--------|------|------|
| **MySQL** | `localhost:3307` | `root` | `root123` | 数据库服务 |
| **MySQL** | `localhost:3307` | `tag_user` | `tag_pass` | 应用用户 |
| **MinIO** | `http://localhost:9000` | `minioadmin` | `minioadmin` | S3存储服务 |
| **MinIO Console** | `http://localhost:9001` | `minioadmin` | `minioadmin` | MinIO管理界面 |
| **Spark Master** | `http://localhost:8080` | - | - | Spark主节点UI |
| **Jupyter** | `http://localhost:8888` | - | `tag_system_2024` | 开发环境 |

### 🔧 服务连接配置

```bash
# MySQL数据库连接
mysql -h 127.0.0.1 -P 3307 -u root -proot123
mysql -h 127.0.0.1 -P 3307 -u tag_user -ptag_pass

# MinIO S3 API配置
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY=minioadmin
export S3_SECRET_KEY=minioadmin

# Spark集群配置
export SPARK_MASTER_URL=spark://localhost:7077
```

### 🌐 API服务器

```bash
# 启动API服务器
python api_server.py --env local --host 0.0.0.0 --port 5000

# 触发标签任务
curl -X POST http://localhost:5000/api/v1/tags/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "tag_ids": [1, 2, 3],
    "mode": "full"
  }'

# 查询任务状态
curl -X GET http://localhost:5000/api/v1/tasks/{task_id}/status

# 获取可用标签
curl -X GET http://localhost:5000/api/v1/tags/available
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

# 2. 运行作业 - 任务化架构
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=health'                    # 健康检查
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=list-tasks'               # 列出任务
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=task-all'                 # 全量任务
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=task-tags,--tag-ids=1,3,5'  # 指定任务
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=task-users,--user-ids=user_000001,user_000002,--tag-ids=1,3,5'  # 指定用户任务
```

### 🏭 AWS Glue生产环境

```bash
# 1. 部署到Glue
cd environments/glue-prod  
python deploy.py

# 2. 运行作业 - 任务化架构
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=health'                    # 健康检查
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=list-tasks'               # 列出任务
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=task-all'                 # 全量任务
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=task-tags,--tag-ids=1,3,5'  # 指定任务
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=task-users,--user-ids=user_000001,user_000002,--tag-ids=1,3,5'  # 指定用户任务
```

## 🎯 任务化架构详解

### 任务类结构

```python
# 抽象基类
class BaseTagTask:
    """标签任务抽象基类"""
    
    def get_required_fields(self) -> List[str]:
        """获取任务所需的数据字段"""
        pass
    
    def get_data_sources(self) -> Dict[str, str]:
        """获取数据源配置"""
        pass
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """数据预处理"""
        pass
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """结果后处理"""
        pass
```

### 任务类示例

```python
class HighNetWorthUserTask(BaseTagTask):
    """高净值用户标签任务 - 标签ID: 1"""
    
    def get_required_fields(self) -> List[str]:
        return ['user_id', 'total_asset_value', 'cash_balance']
    
    def get_data_sources(self) -> Dict[str, str]:
        return {
            'primary': 'user_asset_summary',
            'secondary': None
        }
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        return raw_data.filter(
            col('total_asset_value').isNotNull() & 
            (col('total_asset_value') >= 0)
        )
```

### 已注册的任务类

| 标签ID | 任务类 | 描述 | 业务域 | 模块路径 |
|-------|--------|------|--------|----------|
| 1 | HighNetWorthUserTask | 高净值用户 | 财富管理 | src.tasks.wealth |
| 2 | ActiveTraderTask | 活跃交易者 | 行为分析 | src.tasks.behavior |
| 3 | LowRiskUserTask | 低风险用户 | 风险管理 | src.tasks.risk |
| 4 | NewUserTask | 新注册用户 | 生命周期 | src.tasks.lifecycle |
| 5 | VIPUserTask | VIP客户 | 价值管理 | src.tasks.lifecycle |
| 6 | CashRichUserTask | 现金充足用户 | 财富管理 | src.tasks.wealth |
| 7 | YoungUserTask | 年轻用户 | 人口特征 | src.tasks.demographic |
| 8 | RecentActiveUserTask | 最近活跃用户 | 行为分析 | src.tasks.behavior |

## 🌐 API接口详解

### 接口概览

| 方法 | 路径 | 描述 |
|------|------|------|
| GET | `/health` | 健康检查 |
| POST | `/api/v1/tags/trigger` | 触发标签任务 |
| GET | `/api/v1/tasks/{task_id}/status` | 查询任务状态 |
| GET | `/api/v1/tasks` | 列出所有任务 |
| GET | `/api/v1/tags/available` | 获取可用标签 |

### 触发标签任务

```json
POST /api/v1/tags/trigger
{
    "tag_ids": [1, 2, 3],                    // 必需: 标签ID列表
    "user_ids": ["user_000001", "user_000002"], // 可选: 指定用户列表
    "mode": "full"                           // 可选: 执行模式
}
```

**响应示例：**
```json
{
    "success": true,
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "message": "标签任务已成功提交",
    "data": {
        "tag_ids": [1, 2, 3],
        "user_ids": ["user_000001", "user_000002"],
        "mode": "full",
        "environment": "local",
        "submitted_at": "2024-01-20T10:30:00"
    }
}
```

### 任务状态查询

```json
GET /api/v1/tasks/{task_id}/status
{
    "success": true,
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": {
        "task_id": "550e8400-e29b-41d4-a716-446655440000",
        "tag_ids": [1, 2, 3],
        "status": "completed",
        "submitted_at": "2024-01-20T10:30:00",
        "started_at": "2024-01-20T10:30:05",
        "completed_at": "2024-01-20T10:32:15",
        "result": {
            "total_users": 285,
            "message": "Successfully processed 285 users"
        }
    }
}
```

## 📊 数据表结构

### MySQL标签规则表

```sql
-- 标签分类表
CREATE TABLE tag_category (
    category_id INT PRIMARY KEY AUTO_INCREMENT,
    category_name VARCHAR(100) NOT NULL COMMENT '分类名称',
    description TEXT COMMENT '分类描述',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 标签定义表
CREATE TABLE tag_definition (
    tag_id INT PRIMARY KEY AUTO_INCREMENT,
    tag_name VARCHAR(200) NOT NULL COMMENT '标签名称',
    tag_category VARCHAR(100) NOT NULL COMMENT '标签分类',
    description TEXT COMMENT '标签描述',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 标签规则表
CREATE TABLE tag_rules (
    rule_id INT PRIMARY KEY AUTO_INCREMENT,
    tag_id INT NOT NULL COMMENT '标签ID',
    rule_conditions JSON NOT NULL COMMENT '规则条件（JSON格式）',
    is_active BOOLEAN DEFAULT TRUE COMMENT '是否激活',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tag_id) REFERENCES tag_definition(tag_id)
);

-- 用户标签结果表（一个用户一条记录设计）
CREATE TABLE user_tags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(100) NOT NULL COMMENT '用户ID',
    tag_ids JSON NOT NULL COMMENT '用户的所有标签ID数组 [1,2,3,5]',
    tag_details JSON COMMENT '标签详细信息 {"1": {"tag_name": "高净值用户"}}',
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间（永远不变）',
    updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间（由UPSERT逻辑控制）',
    UNIQUE KEY uk_user_id (user_id)
);
```

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

## 🔧 配置说明

### 环境变量配置

```bash
# 环境配置
export TAG_SYSTEM_ENV=local              # 环境标识

# 本地环境配置
export MYSQL_HOST=localhost
export MYSQL_PORT=3307
export MYSQL_USER=root
export MYSQL_PASSWORD=root123
export MYSQL_DATABASE=tag_system

# MinIO S3配置
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY=minioadmin
export S3_SECRET_KEY=minioadmin

# Spark配置
export SPARK_MASTER_URL=spark://localhost:7077

# API服务器配置
export API_HOST=0.0.0.0
export API_PORT=5000

# Glue环境配置
export DEV_S3_BUCKET=tag-system-dev-data-lake
export DEV_MYSQL_HOST=tag-system-dev.cluster-xxx.rds.amazonaws.com
export PROD_S3_BUCKET=tag-system-prod-data-lake
export PROD_MYSQL_HOST=tag-system-prod.cluster-xxx.rds.amazonaws.com
```

## 📝 使用示例

### 命令行使用

```bash
# 任务化全量标签计算
python main.py --env local --mode task-all

# 任务化指定标签计算
python main.py --env local --mode task-tags --tag-ids 1,3,5

# 任务化指定用户标签计算
python main.py --env local --mode task-users --user-ids user_000001,user_000002 --tag-ids 1,3,5
```

### API使用

```python
import requests
import time

# 1. 获取可用标签
available_response = requests.get('http://localhost:5000/api/v1/tags/available')
print("可用标签:", available_response.json())

# 2. 触发标签任务
response = requests.post('http://localhost:5000/api/v1/tags/trigger', json={
    "tag_ids": [1, 2, 3],  # 高净值用户、活跃交易者、低风险用户
    "mode": "full"
})

if response.status_code == 202:
    task_id = response.json()['task_id']
    print(f"任务已提交: {task_id}")
    
    # 3. 查询任务状态
    while True:
        status_response = requests.get(f'http://localhost:5000/api/v1/tasks/{task_id}/status')
        status = status_response.json()['status']['status']
        print(f"任务状态: {status}")
        
        if status in ['completed', 'failed']:
            print("任务完成:", status_response.json())
            break
        
        time.sleep(5)
else:
    print("任务提交失败:", response.json())
```

### 查询结果

```sql
-- 查询具有特定标签的用户
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '1') 
LIMIT 5;

-- 查询用户的所有标签
SELECT user_id, 
       tag_ids,
       JSON_LENGTH(tag_ids) as tag_count,
       created_time,
       updated_time
FROM user_tags 
WHERE user_id = 'user_000001';

-- 查询具有多个标签的用户
SELECT user_id, tag_ids 
FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '1') 
  AND JSON_CONTAINS(tag_ids, '2');

-- 查询标签统计
SELECT 
    tag_id,
    COUNT(*) as user_count
FROM user_tags 
CROSS JOIN JSON_TABLE(tag_ids, '$[*]' COLUMNS (tag_id INT PATH '$')) AS jt
GROUP BY tag_id
ORDER BY user_count DESC;
```

## 🧪 测试

```bash
# 运行单元测试
python -m pytest tests/unit/ -v

# 运行集成测试
python -m pytest tests/integration/ -v

# 运行任务化架构测试
python -m pytest tests/integration/test_end_to_end.py::TestEndToEndIntegration::test_full_tag_compute_workflow -v

# 测试任务化架构
python main.py --env local --mode list-tasks              # 列出所有可用任务
python main.py --env local --mode task-all                # 测试所有任务类执行
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

### 缓存策略

```python
# 预缓存MySQL规则数据
mysql_rules = rule_reader.read_active_rules()
mysql_rules.persist(StorageLevel.MEMORY_AND_DISK)

# 缓存热点数据
if table_name in ['user_basic_info', 'user_asset_summary']:
    df = df.cache()
```

### API性能

- 异步任务处理，不阻塞调用方
- 线程池管理并发任务（默认最大3个并发）
- 任务状态内存缓存，快速查询
- 自动清理过期任务，避免内存泄漏

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
- 任务成功率和失败率
- 数据读取量和处理速度
- 标签命中率和覆盖率
- API请求响应时间
- 系统资源使用情况

## 📚 文档

- [API使用文档](docs/API_USAGE.md) - 详细的API接口说明
- [业务需求文档](docs/标准需求文档.md) - 系统业务需求和规范
- [技术实施方案](docs/大数据侧标签系统实施方案.md) - 详细的技术实施方案
- [项目说明](CLAUDE.md) - AI助手项目开发说明

## ⚠️ 注意事项

1. **数据一致性**：确保S3数据和MySQL规则的一致性
2. **资源管理**：合理设置Spark资源参数，避免OOM
3. **错误处理**：重要操作都有重试机制和错误恢复
4. **数据备份**：写入前自动备份现有数据
5. **权限控制**：确保对S3和MySQL有足够的访问权限
6. **API安全**：生产环境请添加认证和授权机制

### 🔧 常见问题

**1. 服务启动失败**
```bash
# 检查端口占用
lsof -i :3307 -i :9000 -i :8080 -i :5000

# 重新部署
cd environments/local
./setup.sh stop
./setup.sh clean
./setup.sh
```

**2. 数据库连接失败**
```bash
# 检查MySQL服务状态
docker ps | grep mysql

# 重新初始化数据库
./init_data.sh reset
```

**3. API请求失败**
```bash
# 检查API服务器状态
curl http://localhost:5000/health

# 启动API服务器
python api_server.py --env local --host 0.0.0.0 --port 5000
```

**4. 任务执行失败**
```bash
# 检查任务注册状态
python -c "from src.tasks.task_registry import TagTaskFactory; print(TagTaskFactory.get_all_available_tasks())"

# 查看详细日志
python main.py --env local --mode health --log-level DEBUG
```

## 🛠️ 开发指南

### 🎯 新增标签任务开发步骤

当需要新增一个标签时，按照以下步骤进行开发：

#### 第一步：在MySQL中添加标签定义和规则

```sql
-- 1. 添加标签定义
INSERT INTO tag_definition (tag_id, tag_name, tag_category, description, is_active) 
VALUES (9, '高频交易用户', '行为分析', '30天内交易次数超过50次的用户', 1);

-- 2. 添加标签规则（JSON格式）
INSERT INTO tag_rules (tag_id, rule_conditions, is_active) 
VALUES (9, '{
  "logic": "AND",
  "conditions": [
    {
      "field": "trade_count_30d",
      "operator": ">=", 
      "value": 50,
      "type": "number"
    }
  ]
}', 1);
```

#### 第二步：创建任务类文件

根据业务域创建对应的任务类文件：

```bash
# 创建新的任务类文件（以行为分析域为例）
touch src/tasks/behavior/high_frequency_trader_task.py
```

#### 第三步：实现任务类

```python
# src/tasks/behavior/high_frequency_trader_task.py
from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.tasks.base_tag_task import BaseTagTask

class HighFrequencyTraderTask(BaseTagTask):
    """高频交易用户标签任务 - 标签ID: 9"""
    
    def __init__(self, tag_config: Dict):
        super().__init__(
            tag_id=9,
            tag_name="高频交易用户",
            tag_category="行为分析",
            task_config=tag_config
        )
    
    def get_required_fields(self) -> List[str]:
        """获取任务所需的数据字段"""
        return [
            'user_id', 
            'trade_count_30d'  # 30天交易次数
        ]
    
    def get_data_sources(self) -> Dict[str, str]:
        """获取数据源配置"""
        return {
            'primary': 'user_activity_summary',  # 主数据源
            'secondary': None  # 辅助数据源（如果需要）
        }
    
    def validate_data(self, data: DataFrame) -> bool:
        """验证数据完整性"""
        required_cols = ['user_id', 'trade_count_30d']
        missing_cols = [col for col in required_cols if col not in data.columns]
        
        if missing_cols:
            self.logger.error(f"缺少必需字段: {missing_cols}")
            return False
        
        return True
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """数据预处理"""
        # 过滤空值和异常数据
        cleaned_data = raw_data.filter(
            col('trade_count_30d').isNotNull() & 
            (col('trade_count_30d') >= 0)
        )
        
        self.logger.info(f"数据预处理完成：{raw_data.count()} → {cleaned_data.count()}")
        return cleaned_data
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """结果后处理"""
        # 可以添加额外的业务逻辑
        # 例如：添加标签权重、有效期等
        return tagged_users.select('user_id', 'tag_id', 'tag_detail')
```

#### 第四步：注册任务类

在 `src/tasks/task_registry.py` 中注册新任务：

```python
# src/tasks/task_registry.py
def register_all_tasks():
    """注册所有标签任务类"""
    # ... 现有注册代码 ...
    
    # 新增：注册高频交易用户任务
    from src.tasks.behavior.high_frequency_trader_task import HighFrequencyTraderTask
    TagTaskFactory.register_task(9, HighFrequencyTraderTask)
```

#### 第五步：更新任务映射

在 `src/engine/task_parallel_engine.py` 中更新映射关系：

```python
# src/engine/task_parallel_engine.py
def _get_tag_to_task_mapping(self) -> Dict[int, type]:
    """获取标签ID到任务类的映射"""
    # 添加新的映射
    from src.tasks.behavior.high_frequency_trader_task import HighFrequencyTraderTask
    
    return {
        # ... 现有映射 ...
        9: HighFrequencyTraderTask,  # 新增
    }
```

#### 第六步：测试新任务

```bash
# 1. 测试单个标签任务
python main.py --env local --mode task-tags --tag-ids 9

# 2. 测试包含新标签的多标签任务
python main.py --env local --mode task-tags --tag-ids 1,2,9

# 3. 验证结果
mysql -h 127.0.0.1 -P 3307 -u root -proot123 tag_system -e "
SELECT user_id, tag_ids 
FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '9') 
LIMIT 5;
"
```

### 📋 任务类开发最佳实践

#### 🔧 数据字段映射

确保你的任务类字段与数据源字段匹配：

```python
# 数据源字段映射表
FIELD_MAPPING = {
    # 用户基本信息表 (user_basic_info)
    'user_id': '用户ID',
    'age': '年龄', 
    'user_level': '用户等级',
    'kyc_status': 'KYC状态',
    'registration_date': '注册日期',
    'risk_score': '风险评分',
    
    # 用户资产汇总表 (user_asset_summary)  
    'total_asset_value': '总资产价值',
    'cash_balance': '现金余额',
    
    # 用户活动汇总表 (user_activity_summary)
    'trade_count_30d': '30天交易次数', 
    'last_login_date': '最后登录日期'
}
```

#### 🛡️ 错误处理模式

```python
class YourTagTask(BaseTagTask):
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        try:
            # 数据清洗逻辑
            cleaned = raw_data.filter(/* 过滤条件 */)
            
            # 验证数据量
            if cleaned.count() == 0:
                self.logger.warning("预处理后无数据，请检查过滤条件")
                
            return cleaned
            
        except Exception as e:
            self.logger.error(f"数据预处理失败: {str(e)}")
            # 返回原始数据作为降级方案
            return raw_data
```

#### 🔍 调试和日志

```python
class YourTagTask(BaseTagTask):
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        # 记录数据统计
        self.logger.info(f"原始数据记录数: {raw_data.count()}")
        
        # 记录关键字段分布
        if 'your_field' in raw_data.columns:
            stats = raw_data.select('your_field').describe().collect()
            self.logger.info(f"字段统计: {stats}")
        
        # ... 处理逻辑 ...
        
        return processed_data
```

### 🧪 测试策略

#### 单元测试

```python
# tests/unit/tasks/test_your_task.py
import pytest
from pyspark.sql import SparkSession
from src.tasks.behavior.your_task import YourTask

class TestYourTask:
    def test_get_required_fields(self):
        task = YourTask({})
        fields = task.get_required_fields()
        assert 'user_id' in fields
        assert 'your_business_field' in fields
    
    def test_data_validation(self, spark_session):
        # 创建测试数据
        test_data = spark_session.createDataFrame([
            ("user_001", 100),
            ("user_002", None)
        ], ["user_id", "your_field"])
        
        task = YourTask({})
        assert task.validate_data(test_data) == True
```

#### 集成测试

```bash
# 完整流程测试
python -m pytest tests/integration/test_new_task.py -v

# 端到端测试  
python main.py --env local --mode task-tags --tag-ids 9 --log-level DEBUG
```

### 📊 性能优化建议

1. **字段选择优化**：只获取必需字段
   ```python
   def get_required_fields(self) -> List[str]:
       # 返回最小必需字段集，避免读取不必要的数据
       return ['user_id', 'essential_field1', 'essential_field2']
   ```

2. **数据过滤前置**：在preprocess中尽早过滤
   ```python
   def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
       # 尽早过滤，减少后续计算量
       return raw_data.filter(col('field') > threshold)
   ```

3. **避免重复计算**：缓存中间结果
   ```python
   def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
       processed = raw_data.transform(/* 复杂计算 */)
       processed.cache()  # 缓存结果
       return processed
   ```

## 🚀 未来规划

- [ ] 支持更多标签类型和复杂规则
- [ ] 集成外部任务队列（Redis/RabbitMQ）
- [ ] 添加Web管理界面
- [ ] 支持实时标签计算
- [ ] 集成监控告警系统
- [ ] 支持标签AB测试
- [ ] 任务类代码生成器
- [ ] 可视化标签规则编辑器

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 📄 许可证

MIT License