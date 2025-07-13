# 🏗️ 项目重构计划

## 📋 当前问题
- ❌ 配置文件分散：`config/`, `aws_glue/`, `test_env/`
- ❌ 测试重复：`tests/` vs `test_env/`
- ❌ 环境混乱：本地、开发、生产环境配置混在一起
- ❌ 部署脚本散乱：Docker和Glue配置没有清晰分离

## 🎯 重构目标
1. **清晰的三环境分离**：本地 + Glue开发 + Glue生产
2. **统一的配置管理**：一套代码，多环境配置
3. **简化的部署流程**：每个环境独立的部署脚本
4. **干净的项目结构**：消除冗余和重复

## 🏗️ 新架构设计

```
bigdata_tag_system/
├── src/                          # 🔧 核心源码（环境无关）
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── base.py              # 基础配置类
│   │   └── manager.py           # 配置管理器
│   ├── readers/                 # 数据读取器
│   ├── engine/                  # 标签计算引擎
│   ├── merger/                  # 数据合并器
│   ├── writers/                 # 结果写入器
│   └── scheduler/               # 主调度器
│
├── environments/                 # 🌍 环境配置
│   ├── local/                   # 本地Docker环境
│   │   ├── config.py           # 本地配置
│   │   ├── docker-compose.yml  # Docker服务
│   │   ├── setup.sh            # 环境设置脚本
│   │   ├── test_data/          # 测试数据
│   │   └── notebooks/          # Jupyter测试
│   │
│   ├── glue-dev/               # Glue开发环境
│   │   ├── config.py           # 开发环境配置
│   │   ├── glue_job.py         # Glue作业脚本
│   │   ├── deploy.py           # 部署脚本
│   │   └── resources/          # CloudFormation等
│   │
│   └── glue-prod/              # Glue生产环境
│       ├── config.py           # 生产环境配置
│       ├── glue_job.py         # Glue作业脚本
│       ├── deploy.py           # 部署脚本
│       └── resources/          # CloudFormation等
│
├── tests/                       # 🧪 测试代码
│   ├── unit/                   # 单元测试
│   ├── integration/            # 集成测试
│   └── fixtures/               # 测试数据
│
├── docs/                        # 📚 文档
│   ├── architecture.md
│   ├── deployment.md
│   └── user-guide.md
│
├── scripts/                     # 🔧 工具脚本
│   ├── build.py               # 打包脚本
│   └── utils.py               # 工具函数
│
├── main.py                      # 📍 统一入口
├── requirements.txt             # Python依赖
├── README.md                    # 项目说明
└── .env.example                # 环境变量模板
```

## 🚀 使用方式

### 本地开发
```bash
# 设置本地环境
cd environments/local
./setup.sh

# 运行测试
python ../../main.py --env local --mode full
```

### Glue开发环境
```bash
# 部署到Glue开发环境
cd environments/glue-dev
python deploy.py

# 运行作业
aws glue start-job-run --job-name tag-compute-dev
```

### Glue生产环境
```bash
# 部署到Glue生产环境
cd environments/glue-prod
python deploy.py

# 运行作业
aws glue start-job-run --job-name tag-compute-prod
```

## 📝 迁移步骤
1. ✅ 分析当前问题
2. 🔄 创建新目录结构
3. 🔄 重构配置管理
4. 🔄 迁移核心代码
5. 🔄 重写部署脚本
6. 🔄 更新文档
7. 🔄 清理旧文件

## 🎯 迁移后的优势
- ✅ **环境隔离**：每个环境独立配置和部署
- ✅ **代码复用**：核心代码在所有环境通用
- ✅ **部署简化**：每个环境一个命令完成部署
- ✅ **维护性**：清晰的文件组织，易于维护
- ✅ **扩展性**：新增环境或功能很容易