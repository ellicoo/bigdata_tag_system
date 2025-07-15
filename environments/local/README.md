# 本地环境部署和测试指南

## 概述

本地环境使用Docker容器化部署，提供完整的大数据标签系统测试环境，包括：
- **MySQL 8.0**: 数据库服务（端口3307）
- **MinIO**: S3兼容的对象存储（端口9000-9001）
- **Apache Spark**: 分布式计算引擎（端口8080）
- **Jupyter Notebook**: 交互式开发环境（端口8888）

## 🚀 快速开始

### 1. 一键部署
```bash
# 进入本地环境目录
cd environments/local

# 部署所有服务
./setup.sh

# 初始化数据库和测试数据
./init_data.sh

# 验证部署
cd ../../
python main.py --env local --mode health
```

### 2. 测试标签计算
```bash
# 全量标签计算
python main.py --env local --mode full

# 增量标签计算（最近7天新用户）
python main.py --env local --mode incremental --days 7

# 指定标签计算
python main.py --env local --mode tags --tag-ids 1,2,3
```

## 📦 环境部署详解

### 服务管理命令
```bash
cd environments/local

# 启动服务
./setup.sh                    # 启动所有服务（默认）
./setup.sh start              # 启动服务（显式）
./setup.sh status             # 检查服务状态

# 停止服务
./setup.sh stop               # 停止所有服务

# 清理环境
./setup.sh clean              # 清理数据卷和网络
```

### 数据初始化命令
```bash
cd environments/local

# 基础操作
./init_data.sh                # 初始化数据库和测试数据
./init_data.sh db-only        # 仅初始化数据库表结构
./init_data.sh data-only      # 仅生成测试数据

# 清理和重置
./init_data.sh clean          # 清理所有数据
./init_data.sh reset          # 清理并重新初始化
```

## 🔗 服务访问信息

### 1. Spark 分布式计算

**Spark Master Web UI**
- 访问地址: http://localhost:8080
- 用途: 监控Spark集群状态、查看作业执行情况
- 登录: 无需登录
- 功能:
  - 查看Worker节点状态
  - 监控正在运行的应用程序
  - 查看作业执行历史

**Spark Worker**
- 自动连接到Master节点
- 在Master UI中可以看到Worker状态

### 2. MinIO 对象存储 (S3兼容)

**MinIO Console (Web管理界面)**
- 访问地址: http://localhost:9001
- 用户名: `minioadmin`
- 密码: `minioadmin`
- 用途: 管理S3存储桶和对象
- 功能:
  - 创建和管理存储桶(Bucket)
  - 上传/下载文件
  - 查看存储使用情况
  - 管理访问权限

**MinIO S3 API**
- 访问地址: http://localhost:9000
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- 用途: 程序化访问，模拟AWS S3

### 3. MySQL 数据库

**数据库连接信息**
- 主机: `localhost`
- 端口: `3307` (避免与本机MySQL冲突)
- 数据库: `tag_system`
- 用户名: `root`
- 密码: `root123`

**连接方式**

命令行连接:
```bash
mysql -h 127.0.0.1 -P 3307 -u root -proot123
```

图形化工具连接 (MySQL Workbench, Navicat等):
```
Host: 127.0.0.1
Port: 3307
Username: root
Password: root123
```

Python连接示例:
```python
import pymysql

connection = pymysql.connect(
    host='localhost',
    port=3307,
    user='root',
    password='root123',
    database='tag_system'
)
```

### 4. Jupyter Notebook (可选)

**Jupyter Lab**
- 访问地址: http://localhost:8888
- Token: `tag_system_2024`
- 用途: 交互式数据分析和测试
- 预装: PySpark, pandas, matplotlib等

登录后可以:
- 创建Python笔记本进行数据探索
- 测试PySpark代码
- 查看数据处理结果

## 📊 系统运行测试

部署完成后，可以运行以下命令测试系统:

```bash
# 回到项目根目录
cd ../../

# 健康检查 - 验证所有组件连接正常
python main.py --env local --mode health

# 查看帮助信息
python main.py --help
```

## 🗄️ 数据库初始化

系统首次运行时，需要初始化MySQL数据库表结构:

```sql
-- 连接数据库后执行以下SQL

-- 1. 标签分类表
CREATE TABLE IF NOT EXISTS tag_category (
    category_id INT PRIMARY KEY AUTO_INCREMENT,
    category_name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 2. 标签定义表
CREATE TABLE IF NOT EXISTS tag_definition (
    tag_id INT PRIMARY KEY AUTO_INCREMENT,
    tag_name VARCHAR(200) NOT NULL,
    tag_category VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (tag_category),
    INDEX idx_active (is_active)
);

-- 3. 标签规则表
CREATE TABLE IF NOT EXISTS tag_rules (
    rule_id INT PRIMARY KEY AUTO_INCREMENT,
    tag_id INT NOT NULL,
    rule_conditions JSON NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (tag_id) REFERENCES tag_definition(tag_id),
    INDEX idx_tag_id (tag_id),
    INDEX idx_active (is_active)
);

-- 4. 用户标签结果表
CREATE TABLE IF NOT EXISTS user_tags (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(100) NOT NULL,
    tag_id INT NOT NULL,
    tag_name VARCHAR(200) NOT NULL,
    tag_category VARCHAR(100) NOT NULL,
    tag_detail JSON,
    computed_date DATE NOT NULL,
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_tag_id (tag_id),
    INDEX idx_computed_date (computed_date),
    UNIQUE KEY uk_user_tag_date (user_id, tag_id, computed_date)
);
```

## 🧪 测试数据示例

插入一些测试数据用于验证系统功能:

```sql
-- 插入标签分类
INSERT INTO tag_category (category_name, description) VALUES 
('用户价值', '基于用户资产和行为的价值分类'),
('行为特征', '基于用户行为模式的特征标签'),
('风险等级', '基于用户风险评估的等级标签');

-- 插入标签定义
INSERT INTO tag_definition (tag_name, tag_category, description) VALUES 
('高净值用户', '用户价值', '总资产价值超过10万的用户'),
('活跃交易者', '行为特征', '近30天交易次数超过10次的用户'),
('低风险用户', '风险等级', '风险评估为低风险的用户');

-- 插入标签规则
INSERT INTO tag_rules (tag_id, rule_conditions) VALUES 
(1, '{"logic": "AND", "conditions": [{"field": "total_asset_value", "operator": ">=", "value": 100000, "type": "number"}]}'),
(2, '{"logic": "AND", "conditions": [{"field": "trade_count_30d", "operator": ">", "value": 10, "type": "number"}]}'),
(3, '{"logic": "AND", "conditions": [{"field": "risk_score", "operator": "<=", "value": 30, "type": "number"}]}');
```

## 📁 MinIO存储桶设置

在MinIO Console中创建必要的存储桶:

1. 访问 http://localhost:9001
2. 使用 `minioadmin/minioadmin` 登录
3. 创建以下存储桶:
   - `test-data-lake` - 存储测试数据
   - `hive-warehouse` - Hive数据仓库
   - `tag-results` - 标签计算结果

## 🔧 完整重新部署流程

### 场景1: 解决服务问题
```bash
cd environments/local

# 1. 停止所有服务
./setup.sh stop

# 2. 清理资源（保留数据）
docker system prune -f

# 3. 重新启动
./setup.sh

# 4. 验证
python ../../main.py --env local --mode health
```

### 场景2: 配置更新后重新部署
```bash
cd environments/local

# 1. 完全清理
./setup.sh stop
./setup.sh clean

# 2. 重新部署
./setup.sh

# 3. 重新初始化数据
./init_data.sh

# 4. 验证
cd ../../
python main.py --env local --mode health
python main.py --env local --mode full
```

### 场景3: 数据重置
```bash
cd environments/local

# 仅重置数据库数据
./init_data.sh reset

# 或者清理并重新初始化
./init_data.sh clean
./init_data.sh
```

## 🐛 常见问题解决

### 问题1: MySQL连接失败
```bash
# 症状
ERROR: Can't connect to MySQL server on 'localhost:3307'

# 解决方案
cd environments/local
./setup.sh stop
./setup.sh clean
./setup.sh
# 等待MySQL服务完全启动（约30秒）
./init_data.sh
```

### 问题2: 中文字符乱码
```bash
# 症状
数据库中中文显示为乱码字符

# 解决方案
cd environments/local
./init_data.sh reset  # 重置数据库，使用正确的字符集

# 验证修复
mysql -h 127.0.0.1 -P 3307 -u root -proot123 --default-character-set=utf8mb4 tag_system \
  -e "SELECT tag_name FROM tag_definition WHERE tag_id = 1;"
```

### 问题3: 端口冲突
```bash
# 症状
ERROR: Port 3307 is already in use

# 解决方案
# 检查端口占用
sudo lsof -i :3307
sudo lsof -i :9000
sudo lsof -i :8080

# 停止冲突服务或修改docker-compose.yml端口配置
./setup.sh stop
./setup.sh clean
./setup.sh
```

### 问题4: 服务启动超时
```bash
# 症状
服务健康检查失败，容器反复重启

# 解决方案
# 检查系统资源
docker system df
docker system prune -f  # 清理不用的镜像和容器

# 重新部署
./setup.sh stop
./setup.sh clean
./setup.sh
```

### 问题5: 数据不一致
```bash
# 症状
标签计算结果不符合预期

# 解决方案
# 重新生成测试数据
./init_data.sh data-only

# 或者完全重置
./init_data.sh reset

# 验证数据
cd ../../
python main.py --env local --mode health
```

### 问题6: JDBC连接字符集错误
```bash
# 症状
Unsupported character encoding 'utf8mb4'

# 解决方案
# 这是系统已修复的问题，如果遇到：
# 1. 确保使用最新的配置文件
# 2. 重新部署环境
./setup.sh stop
./setup.sh clean
./setup.sh
./init_data.sh
```

## 🧪 测试数据说明

### 数据规模
- **总用户数**: 1,000个模拟用户
- **数据表**: 3个Hive表（用户基础信息、资产汇总、活动汇总）
- **标签规则**: 8个预定义标签规则

### 标签分布（典型情况）
- **高净值用户**: ~300个用户（30%）
- **活跃交易者**: ~800个用户（80%）
- **低风险用户**: ~250个用户（25%）
- **VIP客户**: ~100个用户（10%）
- **年轻用户**: ~300个用户（30%）
- **新注册用户**: ~150个用户（15%）
- **现金充足用户**: ~200个用户（20%）
- **最近活跃用户**: ~400个用户（40%）

### 验证标签计算
```bash
# 验证全量计算
python main.py --env local --mode full

# 验证增量计算
python main.py --env local --mode incremental --days 7

# 查看结果
mysql -h 127.0.0.1 -P 3307 -u root -proot123 --default-character-set=utf8mb4 tag_system \
  -e "SELECT user_id, tag_ids, computed_date FROM user_tags LIMIT 10;"
```

## 💡 最佳实践

### 开发流程
1. **启动环境**: `./setup.sh`
2. **初始化数据**: `./init_data.sh`
3. **验证健康**: `python main.py --env local --mode health`
4. **开发测试**: 修改代码并测试
5. **功能验证**: 运行完整的标签计算流程

### 数据一致性
- 每次重新部署后都要重新初始化数据
- 标签规则变更后需要重新计算所有标签
- 使用 `./init_data.sh reset` 确保数据一致性

## 📞 技术支持

如果遇到问题，请检查:
1. Docker和Docker Compose是否正确安装
2. 所需端口(8080, 8888, 9000, 9001, 3307)是否被占用
3. 系统内存是否充足(建议8GB+)

---

🎉 **部署完成后，您就拥有了一个完整的本地大数据标签系统！**

### 🏆 修复成果总结

经过完整的问题排查和修复，本地环境现已解决：

1. ✅ **中文字符乱码问题**: 通过正确配置MySQL客户端字符集
2. ✅ **标签重复问题**: 实现多层级去重机制
3. ✅ **写入验证逻辑**: 优化为只验证目标用户写入成功
4. ✅ **增量模式优化**: 采用方案2独立内存处理，避免复杂合并
5. ✅ **JDBC字符集**: 移除不支持的characterEncoding参数
6. ✅ **完整部署流程**: 提供详细的重新部署和故障排除指南

现在系统稳定运行，支持全量、增量和指定标签计算模式！