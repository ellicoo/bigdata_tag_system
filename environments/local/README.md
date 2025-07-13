# 本地环境访问指南

## 🚀 一键部署

```bash
# 进入本地环境目录
cd environments/local

# 运行一键部署脚本
./setup.sh

# 或者分步操作
./setup.sh start    # 仅启动服务
./setup.sh stop     # 停止服务
./setup.sh clean    # 清理环境
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

## 🔧 故障排除

### 常见问题

1. **端口冲突**
   - MySQL端口3307已被占用 → 修改docker-compose.yml中的端口映射
   - 其他端口冲突 → 检查并修改相应服务端口

2. **容器启动失败**
   ```bash
   # 查看容器日志
   docker logs tag_system_mysql
   docker logs tag_system_spark_master
   docker logs tag_system_minio
   ```

3. **权限问题**
   ```bash
   # 修复脚本权限
   chmod +x setup.sh
   ```

4. **数据持久化**
   - MySQL数据存储在Docker Volume: `local_mysql_data`
   - MinIO数据存储在Docker Volume: `local_minio_data`
   - 删除这些Volume会清空所有数据

### 重置环境

如果需要完全重置环境:

```bash
# 停止并删除所有容器和数据
./setup.sh clean

# 重新部署
./setup.sh
```

## 📞 技术支持

如果遇到问题，请检查:
1. Docker和Docker Compose是否正确安装
2. 所需端口(8080, 8888, 9000, 9001, 3307)是否被占用
3. 系统内存是否充足(建议8GB+)

---

🎉 **部署完成后，您就拥有了一个完整的本地大数据标签系统！**