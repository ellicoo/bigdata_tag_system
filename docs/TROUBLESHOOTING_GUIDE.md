# 故障排除指南

本指南帮助您快速诊断和解决大数据标签系统的常见问题。

## 🆘 快速问题诊断

### 诊断脚本
在遇到问题时，首先运行以下诊断脚本：

```bash
#!/bin/bash
echo "🔍 标签系统故障诊断报告"
echo "======================================="
echo "时间: $(date)"
echo ""

# 1. Docker 服务状态
echo "1. Docker 服务状态:"
cd environments/local
docker-compose ps
echo ""

# 2. 端口占用检查
echo "2. 端口占用检查:"
echo "MySQL (3307): $(lsof -i :3307 | wc -l) 进程"
echo "MinIO API (9000): $(lsof -i :9000 | wc -l) 进程"
echo "MinIO Console (9001): $(lsof -i :9001 | wc -l) 进程"
echo "Spark Web UI (8080): $(lsof -i :8080 | wc -l) 进程"
echo "Jupyter (8888): $(lsof -i :8888 | wc -l) 进程"
echo ""

# 3. 容器资源使用
echo "3. 容器资源使用:"
docker stats --no-stream
echo ""

# 4. 数据库连接测试
echo "4. 数据库连接测试:"
docker exec tag_system_mysql mysql -u root -proot123 -e "SELECT 'MySQL连接正常' as status;" 2>/dev/null && echo "✅ MySQL连接正常" || echo "❌ MySQL连接失败"
echo ""

# 5. 数据完整性检查
echo "5. 数据完整性检查:"
table_count=$(docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "USE tag_system; SHOW TABLES;" 2>/dev/null | wc -l)
echo "数据库表数量: $((table_count-1))"

user_count=$(docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "USE tag_system; SELECT COUNT(*) FROM user_tags;" 2>/dev/null | tail -n 1)
echo "用户标签记录数: $user_count"
echo ""

# 6. 最近错误日志
echo "6. 最近错误日志:"
echo "MySQL错误:"
docker logs tag_system_mysql --tail=5 2>/dev/null | grep -i error || echo "无错误"
echo ""
echo "Spark错误:"
docker logs tag_system_spark --tail=5 2>/dev/null | grep -i error || echo "无错误"
echo ""

cd ../../
echo "======================================="
echo "诊断完成。请根据以上信息进行问题分析。"
```

保存为 `diagnose.sh` 并执行：
```bash
chmod +x diagnose.sh
./diagnose.sh
```

## 🔥 常见问题解决方案

### 1. 系统启动问题

#### 问题1.1: Docker 服务启动失败
**症状**: `./setup.sh` 执行后服务状态显示 "Exit" 或 "Unhealthy"

**诊断**:
```bash
# 查看具体服务状态
cd environments/local
docker-compose ps

# 查看失败服务的日志
docker-compose logs [service_name]
```

**解决方案**:
```bash
# 方案A: 重启服务
./setup.sh stop
./setup.sh clean
./setup.sh

# 方案B: 检查端口冲突
lsof -i :3307 -i :9000 -i :9001 -i :8080 -i :8888
# 如有冲突，停止占用进程或修改配置

# 方案C: 检查磁盘空间
df -h
# 如空间不足，清理空间或调整配置
```

#### 问题1.2: 端口被占用
**症状**: 容器启动时报端口已被使用

**诊断**:
```bash
# 查找占用端口的进程
sudo lsof -i :3307
sudo lsof -i :9000
```

**解决方案**:
```bash
# 方案A: 停止占用进程
sudo kill -9 [PID]

# 方案B: 修改端口配置
vi environments/local/docker-compose.yml
# 修改端口映射，如 "3308:3306" 而不是 "3307:3306"

# 同时修改配置文件
vi environments/local/config.py
# 更新对应的端口配置
```

### 2. 数据库相关问题

#### 问题2.1: MySQL 连接超时/拒绝连接
**症状**: `JDBC Connection Failed` 或 `Connection refused`

**诊断**:
```bash
# 检查MySQL容器状态
docker exec tag_system_mysql mysql -u root -proot123 -e "SELECT 1;"

# 查看MySQL日志
docker logs tag_system_mysql --tail=20
```

**解决方案**:
```bash
# 方案A: 重启MySQL
docker-compose restart tag_system_mysql
sleep 30  # 等待启动完成

# 方案B: 重建MySQL容器
docker-compose stop tag_system_mysql
docker-compose rm -f tag_system_mysql
docker-compose up -d tag_system_mysql
sleep 60
./init_data.sh

# 方案C: 检查网络连接
docker network ls
docker network inspect local_tag_system_network
```

#### 问题2.2: 中文字符乱码
**症状**: 数据库中中文显示为 `???` 或乱码

**诊断**:
```bash
# 检查数据库字符集
docker exec tag_system_mysql mysql -u root -proot123 -e "
SHOW VARIABLES LIKE 'character_set%';
SHOW VARIABLES LIKE 'collation%';
"

# 检查表字符集
docker exec tag_system_mysql mysql -u root -proot123 -e "
USE tag_system;
SHOW CREATE TABLE tag_definition;
"
```

**解决方案**:
```bash
# 重置数据库，确保正确字符集
cd environments/local
./init_data.sh reset

# 验证字符集
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT tag_name FROM tag_definition WHERE tag_id = 1;
"
```

#### 问题2.3: 数据库锁死/死锁
**症状**: 操作长时间挂起，无法完成

**诊断**:
```bash
# 查看当前连接和锁
docker exec tag_system_mysql mysql -u root -proot123 -e "
SHOW PROCESSLIST;
SHOW ENGINE INNODB STATUS\G
" | grep -A 20 "LATEST DETECTED DEADLOCK"
```

**解决方案**:
```bash
# 方案A: 杀死长时间运行的查询
docker exec tag_system_mysql mysql -u root -proot123 -e "
KILL [CONNECTION_ID];
"

# 方案B: 重启MySQL服务
docker-compose restart tag_system_mysql

# 方案C: 完全重建
cd environments/local
./setup.sh clean
./setup.sh
./init_data.sh
```

### 3. Spark 相关问题

#### 问题3.1: Spark 任务失败
**症状**: 标签计算过程中出现 Spark 异常

**诊断**:
```bash
# 查看Spark日志
docker logs tag_system_spark --tail=50

# 检查Spark Web UI
curl -s http://localhost:8080 | grep -i error

# 查看系统资源
docker stats --no-stream
free -h
```

**解决方案**:
```bash
# 方案A: 调整内存配置
vi environments/local/docker-compose.yml
# 增加 Spark 容器内存限制和JVM参数

# 方案B: 重启Spark服务
docker-compose restart tag_system_spark

# 方案C: 使用更小的数据集测试
python main.py --env local --mode users-parallel --user-ids user_000001,user_000002
```

#### 问题3.2: 内存不足 (OOM)
**症状**: `java.lang.OutOfMemoryError` 或容器被杀死

**诊断**:
```bash
# 检查系统内存
free -h
cat /proc/meminfo | grep MemAvailable

# 查看Docker内存限制
docker stats
```

**解决方案**:
```bash
# 方案A: 增加Docker内存限制
vi environments/local/docker-compose.yml
# 修改 mem_limit 配置

# 方案B: 调整Spark配置
# 编辑 environments/local/config.py
# 减少 executor_memory 和 driver_memory

# 方案C: 分批处理数据
# 使用更小的批次大小进行测试
```

### 4. 应用程序问题

#### 问题4.1: 标签计算结果异常
**症状**: 用户标签数量异常或逻辑错误

**诊断**:
```bash
# 检查标签规则配置
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT tag_id, JSON_PRETTY(rule_conditions) FROM tag_rules WHERE is_active = 1;
"

# 检查测试数据
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT COUNT(*) as total_users FROM user_tags;
SELECT tag_id, COUNT(*) as user_count 
FROM (
    SELECT tag_id 
    FROM user_tags 
    CROSS JOIN JSON_TABLE(tag_ids, '$[*]' COLUMNS(tag_id INT PATH '$')) AS jt
) AS tag_counts 
GROUP BY tag_id;
"
```

**解决方案**:
```bash
# 方案A: 重新生成测试数据
cd environments/local
./init_data.sh data-only

# 方案B: 验证单个用户标签计算
python main.py --env local --mode users-parallel --user-ids user_000001

# 方案C: 检查规则逻辑
# 手动验证规则条件是否符合测试数据
```

#### 问题4.2: 时间戳逻辑异常
**症状**: `updated_time` 未按预期更新或 `created_time` 发生变化

**诊断**:
```bash
# 执行时间戳测试
python main.py --env local --mode user-tags-parallel --user-ids user_000001 --tag-ids 1,2

# 记录时间戳
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE user_id = 'user_000001';
" > before.txt

# 执行相同操作
python main.py --env local --mode user-tags-parallel --user-ids user_000001 --tag-ids 1,2

# 再次记录
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE user_id = 'user_000001';
" > after.txt

# 比较差异
diff before.txt after.txt
```

**解决方案**:
```bash
# 如果时间戳逻辑异常，检查UPSERT SQL
# 确保 src/writers/optimized_mysql_writer.py 中的逻辑正确
grep -A 10 "ON DUPLICATE KEY UPDATE" src/writers/optimized_mysql_writer.py

# 重新初始化表结构
cd environments/local
./init_data.sh db-only
```

### 5. 网络和连接问题

#### 问题5.1: S3/MinIO 连接失败
**症状**: `S3AFileSystem` 相关错误

**诊断**:
```bash
# 检查MinIO服务状态
curl -s http://localhost:9000/minio/health/live

# 检查MinIO日志
docker logs tag_system_minio --tail=20
```

**解决方案**:
```bash
# 方案A: 重启MinIO
docker-compose restart tag_system_minio

# 方案B: 使用内置数据生成器（本地环境推荐）
# 系统会自动切换到内置数据生成器，无需额外配置

# 方案C: 手动创建存储桶
docker exec -it tag_system_minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker exec -it tag_system_minio mc mb myminio/test-data-lake
```

#### 问题5.2: 网络隔离问题
**症状**: 容器间无法通信

**诊断**:
```bash
# 检查Docker网络
docker network ls
docker network inspect environments_local_tag_system_network

# 测试容器间连通性
docker exec tag_system_spark ping tag_system_mysql
```

**解决方案**:
```bash
# 重建网络
cd environments/local
./setup.sh stop
docker network prune -f
./setup.sh
```

### 6. 性能问题

#### 问题6.1: 执行速度过慢
**症状**: 标签计算耗时异常长

**诊断**:
```bash
# 监控资源使用
top
htop  # 如果可用

# 检查磁盘I/O
iostat -x 1 5

# 查看Spark任务进度
curl -s http://localhost:8080/json/ | jq .
```

**解决方案**:
```bash
# 方案A: 调整并行度
# 编辑配置减少数据量或增加资源

# 方案B: 使用SSD磁盘
# 确保Docker数据目录在SSD上

# 方案C: 优化内存配置
vi environments/local/docker-compose.yml
# 调整内存和CPU限制
```

#### 问题6.2: 内存泄漏
**症状**: 内存使用持续增长

**诊断**:
```bash
# 持续监控内存使用
watch -n 5 'docker stats --no-stream'

# 检查垃圾回收
docker logs tag_system_spark | grep -i "gc"
```

**解决方案**:
```bash
# 方案A: 调整JVM参数
vi environments/local/docker-compose.yml
# 添加GC调优参数

# 方案B: 定期重启服务
# 在大批量处理后重启Spark
docker-compose restart tag_system_spark
```

## 🔧 高级诊断技巧

### 1. 启用详细日志
```bash
# 修改日志级别为DEBUG
python main.py --env local --mode health --log-level DEBUG

# 查看详细的Spark执行计划
python main.py --env local --mode tags-parallel --tag-ids 1 --log-level DEBUG 2>&1 | grep -A 20 "Physical Plan"
```

### 2. 数据库性能分析
```bash
# 启用MySQL慢查询日志
docker exec tag_system_mysql mysql -u root -proot123 -e "
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL slow_query_log_file = '/var/lib/mysql/slow.log';
SET GLOBAL long_query_time = 1;
"

# 分析查询性能
docker exec tag_system_mysql mysql -u root -proot123 -e "
USE tag_system;
EXPLAIN SELECT * FROM user_tags WHERE JSON_CONTAINS(tag_ids, '1');
"
```

### 3. 容器级别监控
```bash
# 持续监控容器状态
watch -n 2 'docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"'

# 检查容器健康状态
docker inspect tag_system_mysql | grep -A 5 '"Health"'
```

## 📞 获取帮助的最佳实践

### 1. 收集诊断信息
```bash
# 生成完整诊断报告
./diagnose.sh > diagnostic_report.txt
docker-compose logs > container_logs.txt

# 收集系统信息
uname -a > system_info.txt
df -h >> system_info.txt
free -h >> system_info.txt
```

### 2. 问题描述模板
```
**问题描述**: [简要描述问题]
**重现步骤**: 
1. [步骤1]
2. [步骤2]
3. [步骤3]

**预期结果**: [描述预期结果]
**实际结果**: [描述实际结果]

**环境信息**:
- 操作系统: [OS版本]
- Docker版本: [Docker版本]
- 可用内存: [内存信息]

**诊断信息**: [附加诊断报告]
**错误日志**: [相关错误日志]
```

### 3. 常用的调试命令
```bash
# 快速重置环境
cd environments/local && ./setup.sh clean && ./setup.sh && ./init_data.sh

# 测试基础功能
python main.py --env local --mode health

# 检查数据库状态
docker exec tag_system_mysql mysql -u root -proot123 -e "SHOW PROCESSLIST; SHOW ENGINE INNODB STATUS\G"

# 监控资源使用
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
```

## 📈 预防性维护

### 1. 定期检查
```bash
# 每周运行的健康检查脚本
#!/bin/bash
echo "=== 每周健康检查 $(date) ==="

# 检查磁盘空间
df -h | grep -E '9[0-9]%|100%' && echo "⚠️ 磁盘空间不足"

# 检查容器状态
unhealthy=$(docker ps --filter "health=unhealthy" -q | wc -l)
[ $unhealthy -gt 0 ] && echo "⚠️ 发现 $unhealthy 个不健康容器"

# 运行系统测试
cd /path/to/project
python main.py --env local --mode health || echo "❌ 健康检查失败"

echo "=== 健康检查完成 ==="
```

### 2. 日志轮转
```bash
# 清理旧日志
docker system prune -f
docker volume prune -f

# 限制日志大小
vi environments/local/docker-compose.yml
# 添加 logging 配置
```

### 3. 备份重要配置
```bash
# 备份配置文件
tar -czf config_backup_$(date +%Y%m%d).tar.gz environments/local/
```

通过遵循本指南，您应该能够快速诊断和解决大多数常见问题。如果问题仍然存在，请收集完整的诊断信息并寻求进一步的技术支持。