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

### 环境管理
```bash
# 本地环境一键搭建
cd environments/local && ./setup.sh

# 启动本地服务
cd environments/local && ./setup.sh start

# 停止本地服务
cd environments/local && ./setup.sh stop

# 清理本地环境
cd environments/local && ./setup.sh clean
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

### 输出结果
- **MySQL user_tags表**: 计算后的标签结果，包含user_id和标签元数据

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

### 结果存储表
- `user_tags`: 用户标签结果，包含user_id、tag_id、tag_detail等字段

具体表结构和示例数据详见 `docs/` 目录下的相关文档。

## 文档索引

项目文档已整理到 `docs/` 目录：
- **标准需求文档.md**: 业务需求和规范
- **大数据侧标签系统实施方案.md**: 详细技术实施方案
- **标签架构演进方案.md**: 架构设计和演进规划
- **MIGRATION_GUIDE.md**: 系统迁移指南
- **RESTRUCTURE_PLAN.md**: 架构重构计划