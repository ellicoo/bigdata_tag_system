# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个大数据侧标签系统，通过PySpark从S3 Hive表读取用户数据，结合MySQL中的规则进行标签计算，并将结果写回MySQL。系统支持全量、增量和指定标签计算模式。

## 项目架构（重构后）

系统采用三环境架构，项目结构如下：

```
大数据标签系统
├── src/                    # 核心业务逻辑
│   ├── config/            # 统一配置管理
│   │   ├── base.py        # 基础配置类
│   │   └── manager.py     # 配置管理器
│   ├── readers/           # 数据读取器
│   │   ├── hive_reader.py # S3 Hive表读取
│   │   └── rule_reader.py # MySQL规则读取
│   ├── engine/            # 标签计算引擎
│   │   ├── rule_parser.py # JSON规则解析器
│   │   └── tag_computer.py# 分布式标签计算
│   ├── merger/            # 标签合并和去重
│   │   └── tag_merger.py  # 标签结果合并
│   ├── writers/           # 数据写入器
│   │   └── mysql_writer.py# MySQL批量写入
│   └── scheduler/         # 主调度器
│       └── main_scheduler.py# 工作流编排
├── environments/          # 三环境配置
│   ├── local/            # 本地Docker环境
│   │   ├── config.py     # 本地配置
│   │   ├── docker-compose.yml# Docker服务
│   │   └── setup.sh      # 一键环境搭建
│   ├── glue-dev/         # AWS Glue开发环境
│   │   ├── config.py     # 开发环境配置
│   │   ├── glue_job.py   # Glue ETL作业
│   │   └── deploy.py     # 自动化部署
│   └── glue-prod/        # AWS Glue生产环境
│       ├── config.py     # 生产环境配置
│       ├── glue_job.py   # 生产Glue作业
│       └── deploy.py     # 生产部署脚本
├── tests/                # 测试框架
│   ├── unit/            # 单元测试
│   ├── integration/     # 集成测试
│   ├── fixtures/        # 测试数据
│   └── test_basic.py    # 基础测试
├── docs/                # 项目文档
└── main.py             # 统一入口
```

## 常用命令

### 本地环境管理详细指南

#### 🚀 快速开始
```bash
# 进入本地环境目录
cd environments/local

# 一键部署所有服务
./setup.sh

# 初始化数据库和测试数据
./init_data.sh

# 测试系统运行
cd ../../
python main.py --env local --mode health    # 健康检查
python main.py --env local --mode full      # 全量标签计算
python main.py --env local --mode incremental --days 7  # 增量计算
```

#### 📦 环境部署命令
```bash
# 基础服务管理
cd environments/local
./setup.sh                    # 启动所有服务（默认）
./setup.sh start              # 启动服务
./setup.sh stop               # 停止服务
./setup.sh clean              # 清理数据卷和网络
./setup.sh status             # 检查服务状态
```

#### 🗄️ 数据初始化命令
```bash
# 数据库和测试数据管理
cd environments/local
./init_data.sh                # 初始化数据库和测试数据（默认）
./init_data.sh clean          # 清理所有数据
./init_data.sh reset          # 清理并重新初始化
./init_data.sh db-only        # 仅初始化数据库表结构
./init_data.sh data-only      # 仅生成测试数据
```

#### 🔧 完整重新部署流程
```bash
# 完全重新部署（解决服务问题或配置更新）
cd environments/local

# 1. 停止所有服务
./setup.sh stop

# 2. 清理数据卷（会清空所有数据）
./setup.sh clean

# 3. 重新启动服务
./setup.sh

# 4. 重新初始化数据
./init_data.sh

# 5. 验证部署
cd ../../
python main.py --env local --mode health
```

#### 🐛 常见问题解决
```bash
# 问题1: MySQL连接超时或锁死
cd environments/local
./setup.sh stop
./setup.sh clean  # 清理数据卷
./setup.sh        # 重新部署

# 问题2: 中文字符乱码
# 确保使用正确的字符集初始化
./init_data.sh reset  # 重置数据库

# 问题3: 服务端口冲突
docker ps -a  # 检查端口占用
./setup.sh stop
./setup.sh clean
./setup.sh

# 问题4: 数据不一致
./init_data.sh clean   # 清理数据
./init_data.sh         # 重新初始化
```

### 运行系统（三环境支持）
```bash
# 本地环境
python main.py --env local --mode full                    # 全量计算
python main.py --env local --mode incremental --days 3    # 增量计算
python main.py --env local --mode tags --tag-ids 1,3,5    # 指定标签
python main.py --env local --mode health                  # 健康检查

# Glue开发环境
python main.py --env glue-dev --mode full

# Glue生产环境  
python main.py --env glue-prod --mode full
```

### 部署管理
```bash
# 部署到Glue开发环境
cd environments/glue-dev && python deploy.py

# 部署到Glue生产环境
cd environments/glue-prod && python deploy.py

# 运行Glue作业（通过AWS CLI）
aws glue start-job-run --job-name tag-compute-dev --arguments='--mode=health'
aws glue start-job-run --job-name tag-compute-prod --arguments='--mode=full'
```

### 测试
```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行单元测试
python -m pytest tests/unit/ -v

# 运行集成测试
python -m pytest tests/integration/ -v

# 运行基础测试
python -m pytest tests/test_basic.py -v

# 测试覆盖率
python -m pytest tests/ --cov=src --cov-report=html

# 运行特定测试用例
python -m pytest tests/integration/test_end_to_end.py::TestEndToEndIntegration::test_full_tag_compute_workflow -v     # 全量标签测试
python -m pytest tests/integration/test_end_to_end.py::TestEndToEndIntegration::test_specific_tags_workflow -v      # 特定标签测试
python -m pytest tests/integration/test_end_to_end.py::TestEndToEndIntegration::test_incremental_compute_workflow -v # 增量计算测试
python -m pytest tests/integration/test_end_to_end.py::TestEndToEndIntegration::test_health_check_workflow -v       # 健康检查测试
```

## 配置管理

系统采用统一的三环境配置管理，支持以下环境：

### 环境类型
- **local**: 本地Docker环境（开发测试）
- **glue-dev**: AWS Glue开发环境
- **glue-prod**: AWS Glue生产环境

### 配置结构
```python
# 基础配置类
class BaseConfig:
    environment: str        # 环境标识
    spark: SparkConfig     # Spark配置
    s3: S3Config          # S3/MinIO配置  
    mysql: MySQLConfig    # MySQL配置

# 配置管理器
ConfigManager.load_config('local')      # 加载本地配置
ConfigManager.load_config('glue-dev')   # 加载开发配置
ConfigManager.load_config('glue-prod')  # 加载生产配置
```

### 环境变量
```bash
# 通用配置
TAG_SYSTEM_ENV=local              # 环境标识

# 本地环境（MinIO + MySQL容器）
MYSQL_HOST=localhost
MYSQL_PORT=3307
S3_ENDPOINT=http://localhost:9000

# Glue开发环境
DEV_S3_BUCKET=tag-system-dev-data-lake
DEV_MYSQL_HOST=tag-system-dev.cluster-xxx.rds.amazonaws.com
DEV_GLUE_ROLE_ARN=arn:aws:iam::xxx:role/GlueServiceRole-dev

# Glue生产环境
PROD_S3_BUCKET=tag-system-prod-data-lake
PROD_MYSQL_HOST=tag-system-prod.cluster-xxx.rds.amazonaws.com
PROD_GLUE_ROLE_ARN=arn:aws:iam::xxx:role/GlueServiceRole-prod
```

配置示例位于 `.env.example`，包含所有环境的变量模板。

## 数据架构

### 输入数据源
- **S3 Hive表**: 来自数据湖的用户数据（user_basic_info, user_asset_summary, user_activity_summary）
- **MySQL规则表**: 以JSON格式存储的标签定义和规则条件

### 输出结果（重构后的数据模型）
- **MySQL user_tags表**: 采用**一个用户一条记录**的设计
  - `tag_ids`: JSON数组，存储用户的所有标签ID `[1,2,3,5]`
  - `tag_details`: JSON对象，存储标签详细信息
  - **核心优势**: 真正的标签合并逻辑，支持复杂查询，符合业务需求

### 标签合并机制
系统实现了真正的标签合并逻辑：
- **新计算标签** + **已有标签** → **数组合并去重**
- 支持增量更新，历史标签自动保留
- MySQL JSON类型支持高效的标签查询：
  ```sql
  -- 查询具有特定标签的用户
  SELECT user_id FROM user_tags WHERE JSON_CONTAINS(tag_ids, '1');
  
  -- 查询具有多个标签的用户
  SELECT user_id FROM user_tags 
  WHERE JSON_CONTAINS(tag_ids, '1') AND JSON_CONTAINS(tag_ids, '2');
  ```

### 规则系统
规则以JSON格式存储，支持：
- 逻辑操作符: AND, OR, NOT
- 比较操作符: =, !=, >, <, >=, <=, in, not_in, in_range, contains, recent_days, is_null, is_not_null
- 字段类型: string, number, date

## 开发注意事项

### 代码组织
- 所有业务逻辑位于 `src/` 目录下
- 使用标准的Python包导入: `from src.config.manager import ConfigManager`
- 遵循模块化设计，各组件职责清晰分离

### 开发规范
- 使用PySpark进行分布式处理，支持大数据量计算
- 统一配置管理，通过 `--env` 参数切换环境
- 完整的错误处理，包含重试机制和优雅降级
- 支持中文日志记录，方便问题排查
- 主入口支持多种执行模式（full/incremental/tags/health）

### 🔧 重要技术修复说明

#### 中文字符编码问题解决
**问题**: 数据库中中文字符显示为乱码
**根本原因**: 数据库初始化时MySQL客户端字符集配置不正确
**解决方案**: 
1. 修改 `init_data.sh` 中的MySQL命令，添加 `--default-character-set=utf8mb4`
2. 移除JDBC连接中的 `characterEncoding=utf8mb4` 参数（该参数不被支持）
3. 保留 `connectionCollation=utf8mb4_unicode_ci` 确保字符集正确

#### 写入验证逻辑优化
**问题**: 验证逻辑错误地比较数据库总用户数和标签用户数
**根本原因**: 数据库用户数不等于标签用户数，用户标签匹配会动态变化
**解决方案**: 
1. 只验证当前计算出标签的用户是否成功写入数据库
2. 使用用户ID集合精确匹配，而不是总数比较
3. 支持空标签用户的正常处理

#### 标签去重机制完善
**问题**: 用户可能获得重复的标签ID
**解决方案**: 
1. 在 `tag_merger.py` 中使用 `dropDuplicates(["user_id", "tag_id"])` 
2. 在数组聚合时使用 `array_distinct()` 确保标签ID唯一
3. 多层级去重保证数据一致性

#### 增量模式逻辑改进
**实现**: 采用方案2 - 独立内存处理
1. 通过 `left_anti` join 识别真正的新用户（Hive中有但MySQL中没有的用户）
2. 对新用户计算所有现有标签规则
3. 直接追加到数据库，无需与现有标签合并
4. 避免了复杂的标签合并逻辑，提高性能

### 测试数据生成
本地环境支持生产级模拟数据生成，确保标签规则能够正确匹配：

#### 数据生成策略
- **高净值用户**: 50个用户，总资产 ≥ 150,000，现金余额 ≥ 60,000
- **VIP客户**: 20个用户，等级为 VIP2/VIP3，KYC状态已验证
- **年轻用户**: 30个用户，年龄在 18-30 岁之间
- **活跃交易者**: 80个用户，30天交易次数 > 15次
- **低风险用户**: 25个用户，风险评分 ≤ 30
- **新注册用户**: 15个用户，注册时间在最近30天内
- **最近活跃用户**: 15个用户，最近7天内有登录

#### 测试数据生成位置
- **完整生成器**: `environments/local/test_data_generator.py`
- **内置生成器**: `src/scheduler/main_scheduler.py:_generate_production_like_data()`
- **简化生成器**: `src/scheduler/main_scheduler.py:_generate_test_user_data()`

#### 本地测试特殊说明
由于本地环境的S3AFileSystem依赖问题，系统在本地模式下会自动使用内置数据生成器，而非从MinIO读取Parquet文件。这确保了本地测试的稳定性和完整性。

### 测试要求
- 新功能必须包含单元测试
- 重要流程需要集成测试
- 使用 `tests/fixtures/` 中的测试数据
- 运行测试前确保本地环境正常

### 部署流程
1. **本地开发**: 使用 `environments/local/` 环境
2. **开发测试**: 部署到 `glue-dev` 环境验证
3. **生产发布**: 通过 `glue-prod` 环境上线

## 数据库表结构

系统需要特定的MySQL表结构：

### 规则存储表
- `tag_category`: 标签分类
- `tag_definition`: 标签定义  
- `tag_rules`: 标签规则（JSON格式）

### 结果存储表（重构后）
- `user_tags`: 用户标签结果表，采用**一个用户一条记录**的新设计
  ```sql
  CREATE TABLE user_tags (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      user_id VARCHAR(100) NOT NULL,
      tag_ids JSON NOT NULL COMMENT '用户的所有标签ID数组 [1,2,3,5]',
      tag_details JSON COMMENT '标签详细信息 {"1": {"tag_name": "高净值用户"}}',
      computed_date DATE NOT NULL,
      created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE KEY uk_user_date (user_id, computed_date)
  );
  ```

### 数据模型重构说明
**重构前**：一个标签一条记录 → 违背标签合并需求
**重构后**：一个用户一条记录 → 标签ID存储为JSON数组

**核心改进**：
1. 真正的标签合并：新老标签数组合并去重
2. 查询高效：支持 `JSON_CONTAINS(tag_ids, '1')` 查询
3. 存储紧凑：减少数据冗余，提高性能
4. 业务友好：符合"用户拥有多个标签"的业务模型

具体表结构和示例数据详见 `docs/` 目录下的相关文档。

## 文档索引

项目文档已整理到 `docs/` 目录：
- **标准需求文档.md**: 业务需求和规范
- **大数据侧标签系统实施方案.md**: 详细技术实施方案
- **标签架构演进方案.md**: 架构设计和演进规划
- **MIGRATION_GUIDE.md**: 系统迁移指南
- **RESTRUCTURE_PLAN.md**: 架构重构计划