# 本地部署快速指南

本指南帮助您快速在本地环境中部署和运行大数据标签系统。

## 🚀 快速开始（3分钟部署）

### 第一步：环境准备
```bash
# 确保已安装 Docker 和 Docker Compose
docker --version
docker-compose --version

# 克隆项目（如果还没有）
git clone <repository-url>
cd bigdata_tag_system
```

### 第二步：一键部署
```bash
# 进入本地环境目录
cd environments/local

# 启动所有服务（MySQL、MinIO、Spark、Jupyter）
./setup.sh

# 初始化数据库和测试数据
./init_data.sh
```

### 第三步：验证部署
```bash
# 回到项目根目录
cd ../../

# 运行健康检查
python main.py --env local --mode health

# 如果看到 "🎉 任务执行成功！" 说明部署成功
```

## 📋 详细部署步骤

### 1. 环境检查
```bash
# 检查 Docker 运行状态
docker info

# 检查端口占用情况（确保以下端口未被占用）
# - 3307: MySQL
# - 9000: MinIO API
# - 9001: MinIO Console
# - 8080: Spark Master Web UI
# - 8888: Jupyter Notebook
lsof -i :3307 -i :9000 -i :9001 -i :8080 -i :8888
```

### 2. 服务启动
```bash
cd environments/local

# 完整启动流程
./setup.sh

# 等待所有服务启动（大约30秒）
# 检查服务状态
docker-compose ps
```

**预期输出**：
```
        Name                      Command              State                    Ports                  
-------------------------------------------------------------------------------------------------------
tag_system_jupyter     start-notebook.sh            Up      0.0.0.0:8888->8888/tcp                   
tag_system_minio       /usr/bin/docker-entrypoint ...   Up      0.0.0.0:9000->9000/tcp, 0.0.0.0:9001->9001/tcp
tag_system_mysql       docker-entrypoint.sh mysqld     Up      0.0.0.0:3307->3306/tcp, 33060/tcp       
tag_system_spark       /opt/spark/bin/spark-class ...   Up      0.0.0.0:8080->8080/tcp, 7077/tcp
```

### 3. 数据初始化
```bash
# 在 environments/local 目录下
./init_data.sh

# 等待初始化完成，应该看到以下输出：
# ✅ 数据库表结构初始化完成
# ✅ 测试数据生成完成
# 🎉 数据初始化完成！
```

### 4. 系统验证
```bash
# 回到项目根目录
cd ../../

# 安装 Python 依赖（如果还没有）
pip install -r requirements.txt

# 运行健康检查
python main.py --env local --mode health
```

**成功输出示例**：
```
2025-07-17 14:00:00,000 - __main__ - INFO - 🚀 启动标签系统
2025-07-17 14:00:00,000 - __main__ - INFO - 📋 环境: local
2025-07-17 14:00:00,000 - __main__ - INFO - 🎯 模式: health
...
2025-07-17 14:00:30,000 - __main__ - INFO - 🎉 任务执行成功！
```

## 🎯 快速测试

### 基础功能测试
```bash
# 1. 全量标签计算（推荐：并行优化版）
python main.py --env local --mode full-parallel

# 2. 指定标签计算
python main.py --env local --mode tags-parallel --tag-ids 1,2,3

# 3. 指定用户标签计算
python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,2,3

# 4. 增量计算
python main.py --env local --mode incremental-parallel --days 7
```

### 验证数据结果
```bash
# 查看标签计算结果
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
LIMIT 5;
"

# 查看标签定义
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT tag_id, tag_name, tag_category 
FROM tag_definition 
WHERE is_active = 1;
"
```

## 🔗 服务访问信息

部署成功后，您可以访问以下服务：

| 服务 | 访问地址 | 用户名/密码 | 说明 |
|------|---------|------------|------|
| **Spark Master Web UI** | http://localhost:8080 | - | 监控Spark集群状态和作业执行 |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin | 管理S3存储桶和文件 |
| **Jupyter Notebook** | http://localhost:8888 | Token: tag_system_2024 | 交互式数据分析 |
| **MySQL 数据库** | localhost:3307 | root/root123 | 标签系统数据库 |

## ⚙️ 高级配置

### 自定义配置
```bash
# 编辑本地配置文件
vi environments/local/config.py

# 修改 Docker Compose 配置
vi environments/local/docker-compose.yml

# 编辑数据库初始化脚本
vi environments/local/init_database.sql
```

### 性能调优
```bash
# 根据机器配置调整内存设置
# 编辑 environments/local/docker-compose.yml 中的：
# - Spark driver/executor 内存
# - MySQL 内存配置
# - JVM 堆大小
```

### 数据定制
```bash
# 修改测试数据生成
vi environments/local/test_data_generator.py

# 重新生成测试数据
cd environments/local
./init_data.sh data-only
```

## 🐛 常见问题解决

### 问题1：端口冲突
**现象**：服务启动失败，提示端口被占用
```bash
# 解决方法：
# 1. 检查端口占用
lsof -i :3307

# 2. 停止占用端口的进程或修改配置
# 3. 重新启动服务
cd environments/local
./setup.sh stop
./setup.sh
```

### 问题2：MySQL连接失败
**现象**：健康检查失败，MySQL连接超时
```bash
# 解决方法：
cd environments/local

# 1. 检查MySQL容器状态
docker-compose logs tag_system_mysql

# 2. 重启MySQL服务
./setup.sh stop
./setup.sh clean
./setup.sh

# 3. 重新初始化数据
./init_data.sh
```

### 问题3：中文字符乱码
**现象**：数据库中中文显示为 `???`
```bash
# 解决方法：
cd environments/local

# 重置数据库，确保使用正确字符集
./init_data.sh reset
```

### 问题4：Spark任务失败
**现象**：标签计算任务异常终止
```bash
# 解决方法：
# 1. 检查Spark日志
docker-compose logs tag_system_spark

# 2. 检查系统资源
docker stats

# 3. 调整内存配置后重试
vi environments/local/docker-compose.yml
# 增加 Spark 内存配置
```

### 问题5：数据不一致
**现象**：标签计算结果异常
```bash
# 解决方法：
cd environments/local

# 1. 清理所有数据重新开始
./init_data.sh clean
./init_data.sh

# 2. 或者只清理计算结果
docker exec tag_system_mysql mysql -u root -proot123 -e "
USE tag_system;
DELETE FROM user_tags;
"
```

## 🧹 环境清理

### 停止服务
```bash
cd environments/local
./setup.sh stop
```

### 完全清理
```bash
cd environments/local

# 停止服务并清理数据卷
./setup.sh clean

# 清理Docker镜像（可选）
docker system prune -f
```

### 重新部署
```bash
cd environments/local

# 一键重新部署
./setup.sh clean
./setup.sh
./init_data.sh

# 验证部署
cd ../../
python main.py --env local --mode health
```

## 📚 下一步

部署成功后，您可以：

1. **查看测试指南**：`docs/TESTING_GUIDE.md`
2. **了解故障排除**：`docs/TROUBLESHOOTING_GUIDE.md`
3. **学习开发流程**：`docs/DEVELOPMENT_GUIDE.md`
4. **部署到AWS**：`environments/glue-dev/` 和 `environments/glue-prod/`

## 🤝 获取帮助

如果遇到问题：
1. 查看项目 `CLAUDE.md` 获得详细技术说明
2. 检查 `docs/` 目录下的专项指南
3. 查看 Docker 容器日志：`docker-compose logs <service_name>`
4. 提交 Issue 到项目仓库