# 项目文档索引

## 📚 文档列表

### 核心文档
- **[标准需求文档.md](./标准需求文档.md)** - 项目需求和业务规范
- **[大数据侧标签系统实施方案.md](./大数据侧标签系统实施方案.md)** - 详细技术实施方案  
- **[标签架构演进方案.md](./标签架构演进方案.md)** - 架构设计和演进规划

### 开发文档  
- **[MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)** - 系统迁移指南
- **[RESTRUCTURE_PLAN.md](./RESTRUCTURE_PLAN.md)** - 架构重构计划

## 🏗️ 项目架构

当前项目采用三环境架构：

```
大数据标签系统
├── src/                    # 核心业务逻辑
├── environments/           # 三环境配置
│   ├── local/             # 本地Docker环境
│   ├── glue-dev/          # AWS Glue开发环境  
│   └── glue-prod/         # AWS Glue生产环境
├── tests/                 # 测试框架
├── docs/                  # 项目文档 (当前目录)
└── main.py               # 统一入口
```

## 🚀 快速开始

```bash
# 1. 本地环境搭建
cd environments/local && ./setup.sh

# 2. 运行健康检查
python main.py --env local --mode health

# 3. 运行标签计算
python main.py --env local --mode full

# 4. 部署到开发环境
cd environments/glue-dev && python deploy.py
```

## 📖 阅读建议

### 新用户
1. 先阅读 [标准需求文档.md](./标准需求文档.md) 了解业务背景
2. 再阅读 [大数据侧标签系统实施方案.md](./大数据侧标签系统实施方案.md) 了解技术方案

### 架构设计师  
1. 重点阅读 [标签架构演进方案.md](./标签架构演进方案.md) 了解架构设计思路
2. 参考 [RESTRUCTURE_PLAN.md](./RESTRUCTURE_PLAN.md) 了解重构历程

### 运维开发
1. 查看 [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) 了解部署和迁移步骤
2. 参考根目录 [README.md](../README.md) 了解日常操作命令

---

更新时间：2025年7月