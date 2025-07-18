# 测试指南

本指南详细介绍如何对大数据标签系统进行全面测试，包括功能测试、性能测试和集成测试。

## 📋 测试概述

### 测试层级
1. **健康检查**: 验证系统基础功能
2. **功能测试**: 验证核心业务逻辑
3. **集成测试**: 验证端到端流程
4. **性能测试**: 验证系统性能指标
5. **时间戳测试**: 验证UPSERT时间戳逻辑

### 测试环境
- **本地环境**: Docker容器化测试环境
- **测试数据**: 1000个模拟用户，8个标签规则
- **并行模式**: 支持6种并行计算场景

## 🚀 快速测试（5分钟验证）

### 前置条件
```bash
# 确保本地环境已部署
cd environments/local
./setup.sh
./init_data.sh

# 回到项目根目录
cd ../../
```

### 基础验证
```bash
# 1. 健康检查
python main.py --env local --mode health

# 2. 简单标签计算
python main.py --env local --mode tags-parallel --tag-ids 1,2

# 3. 检查结果
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT COUNT(*) as total_users, 
       COUNT(CASE WHEN JSON_CONTAINS(tag_ids, '1') THEN 1 END) as tag_1_users,
       COUNT(CASE WHEN JSON_CONTAINS(tag_ids, '2') THEN 1 END) as tag_2_users
FROM user_tags;
"
```

**预期结果**：
- 健康检查成功
- 标签计算成功
- 数据库中有用户标签数据

## 🎯 功能测试详解

### 1. 全量标签计算测试

#### 测试场景1：全量用户打全量标签
```bash
# 执行全量并行计算
python main.py --env local --mode full-parallel

# 验证结果
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT 
    COUNT(*) as total_users,
    AVG(JSON_LENGTH(tag_ids)) as avg_tags_per_user,
    MAX(JSON_LENGTH(tag_ids)) as max_tags_per_user
FROM user_tags;
"
```

**预期结果**：
- `total_users`: ~1000（所有用户）
- `avg_tags_per_user`: 2-5（平均每用户标签数）
- `max_tags_per_user`: ≤8（最大标签数）

#### 测试场景2：全量用户打指定标签（支持标签合并）
```bash
# 先执行基础标签
python main.py --env local --mode tags-parallel --tag-ids 1,2,3

# 记录初始状态
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, updated_time 
FROM user_tags 
WHERE user_id = 'user_000001';
" > before_merge.txt

# 执行标签合并
python main.py --env local --mode tags-parallel --tag-ids 4,5,6

# 验证合并结果
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, updated_time 
FROM user_tags 
WHERE user_id = 'user_000001';
"
```

**预期结果**：
- 标签数组包含新旧标签的合并
- 时间戳根据内容变化正确更新

### 2. 增量计算测试

#### 测试场景3：增量用户打全量标签
```bash
# 模拟增量场景：删除部分用户记录
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
DELETE FROM user_tags WHERE user_id IN ('user_000001', 'user_000002', 'user_000003');
"

# 执行增量计算
python main.py --env local --mode incremental-parallel --days 30

# 验证增量用户被正确处理
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE user_id IN ('user_000001', 'user_000002', 'user_000003')
ORDER BY user_id;
"
```

**预期结果**：
- 被删除的用户重新计算并插入
- `created_time` 为新的时间戳
- 标签数据正确

### 3. 指定用户/标签测试

#### 测试场景5：指定用户打全量标签
```bash
# 指定2个用户计算全量标签
python main.py --env local --mode users-parallel --user-ids user_000001,user_000002

# 验证只有指定用户被更新
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, updated_time 
FROM user_tags 
WHERE user_id IN ('user_000001', 'user_000002', 'user_000003')
ORDER BY user_id;
"
```

#### 测试场景6：指定用户打指定标签（支持标签合并）
```bash
# 指定用户指定标签（测试标签合并）
python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,2,3

# 验证结果
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, updated_time 
FROM user_tags 
WHERE user_id IN ('user_000001', 'user_000002');
"
```

## 🕒 时间戳逻辑测试（重要）

这是验证UPSERT时间戳机制的关键测试。

### 测试1：时间戳不变性测试
```bash
# 1. 执行初始标签计算
python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,2,3

# 2. 记录时间戳
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE user_id IN ('user_000001', 'user_000002')
ORDER BY user_id;
" > timestamps_before.txt

# 3. 执行相同操作
python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,2,3

# 4. 验证时间戳未变化
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE user_id IN ('user_000001', 'user_000002')
ORDER BY user_id;
" > timestamps_after.txt

# 5. 比较结果
diff timestamps_before.txt timestamps_after.txt
```

**预期结果**: 两个文件内容完全相同

### 测试2：时间戳更新测试
```bash
# 1. 记录初始时间戳
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE user_id = 'user_000001';
" > before_change.txt

# 2. 执行标签变化操作
python main.py --env local --mode user-tags-parallel --user-ids user_000001 --tag-ids 4,5,6

# 3. 验证时间戳更新
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT user_id, tag_ids, created_time, updated_time 
FROM user_tags 
WHERE user_id = 'user_000001';
" > after_change.txt

# 4. 比较结果
echo "=== Before ==="
cat before_change.txt
echo "=== After ==="
cat after_change.txt
```

**预期结果**:
- `created_time` 保持不变
- `updated_time` 更新为新时间
- `tag_ids` 包含新标签

### 测试3：幂等性验证
```bash
# 1. 执行标签变化
python main.py --env local --mode user-tags-parallel --user-ids user_000001 --tag-ids 7,8

# 2. 记录时间戳
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT updated_time FROM user_tags WHERE user_id = 'user_000001';
" > timestamp_1.txt

# 3. 立即执行相同操作
python main.py --env local --mode user-tags-parallel --user-ids user_000001 --tag-ids 7,8

# 4. 再次记录时间戳
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT updated_time FROM user_tags WHERE user_id = 'user_000001';
" > timestamp_2.txt

# 5. 验证时间戳未变化
diff timestamp_1.txt timestamp_2.txt
```

**预期结果**: 两次时间戳完全相同

## 📊 性能测试

### 1. 批量处理性能测试
```bash
# 测试不同数据量的处理性能
echo "测试开始时间: $(date)"

# 小规模测试（10个用户）
time python main.py --env local --mode users-parallel --user-ids user_000001,user_000002,user_000003,user_000004,user_000005,user_000006,user_000007,user_000008,user_000009,user_000010

# 中等规模测试（100个用户）
user_list=""
for i in {1..100}; do
    user_id=$(printf "user_%06d" $i)
    user_list="$user_list,$user_id"
done
user_list=${user_list#,}  # 移除开头的逗号

time python main.py --env local --mode users-parallel --user-ids "$user_list"

# 大规模测试（全量用户）
time python main.py --env local --mode full-parallel

echo "测试结束时间: $(date)"
```

### 2. 并发性能对比
```bash
# 对比单线程vs并行模式性能
echo "=== 单线程模式 ==="
time python main.py --env local --mode full

echo "=== 并行模式 ==="
time python main.py --env local --mode full-parallel
```

### 3. 内存使用监控
```bash
# 在后台监控Docker容器资源使用
docker stats --no-stream > stats_before.txt

# 执行大规模计算
python main.py --env local --mode full-parallel

# 记录资源使用
docker stats --no-stream > stats_after.txt

# 比较资源使用情况
echo "=== 计算前资源使用 ==="
cat stats_before.txt
echo "=== 计算后资源使用 ==="
cat stats_after.txt
```

## 🧪 集成测试

### 端到端业务流程测试
```bash
# 完整业务流程测试脚本
#!/bin/bash

echo "🚀 开始端到端集成测试"

# 1. 清理环境
echo "1. 清理测试环境..."
cd environments/local
./init_data.sh clean
./init_data.sh
cd ../../

# 2. 健康检查
echo "2. 系统健康检查..."
python main.py --env local --mode health
if [ $? -ne 0 ]; then
    echo "❌ 健康检查失败"
    exit 1
fi

# 3. 全量计算
echo "3. 执行全量标签计算..."
python main.py --env local --mode full-parallel
if [ $? -ne 0 ]; then
    echo "❌ 全量计算失败"
    exit 1
fi

# 4. 验证数据完整性
echo "4. 验证数据完整性..."
result=$(docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT COUNT(*) FROM user_tags;
" | tail -n 1)

if [ "$result" -lt 100 ]; then
    echo "❌ 数据完整性检查失败，用户数量: $result"
    exit 1
fi

# 5. 增量计算测试
echo "5. 测试增量计算..."
# 删除部分数据模拟增量场景
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
DELETE FROM user_tags WHERE user_id LIKE 'user_0000%' LIMIT 10;
"

python main.py --env local --mode incremental-parallel --days 30
if [ $? -ne 0 ]; then
    echo "❌ 增量计算失败"
    exit 1
fi

# 6. 标签合并测试
echo "6. 测试标签合并功能..."
python main.py --env local --mode tags-parallel --tag-ids 1,2
python main.py --env local --mode tags-parallel --tag-ids 3,4

# 验证合并结果
merge_result=$(docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT COUNT(*) FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '1') AND JSON_CONTAINS(tag_ids, '3');
" | tail -n 1)

if [ "$merge_result" -lt 1 ]; then
    echo "❌ 标签合并测试失败"
    exit 1
fi

echo "✅ 所有集成测试通过！"
```

### 保存为可执行脚本
```bash
# 保存上述脚本
cat > integration_test.sh << 'EOF'
[上述脚本内容]
EOF

# 设置执行权限
chmod +x integration_test.sh

# 运行集成测试
./integration_test.sh
```

## 🔍 数据验证测试

### 1. 标签规则验证
```bash
# 验证每个标签规则的执行结果
echo "=== 标签1: 高净值用户验证 ==="
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT COUNT(*) as high_value_users 
FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '1');
"

echo "=== 标签2: 活跃交易者验证 ==="
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT COUNT(*) as active_traders 
FROM user_tags 
WHERE JSON_CONTAINS(tag_ids, '2');
"

# 验证标签逻辑正确性（需要与生成的测试数据对应）
echo "=== 验证高净值用户标签逻辑 ==="
echo "应该有大约50个高净值用户（总资产≥150000且现金余额≥60000）"
```

### 2. 数据一致性检查
```bash
# 检查标签ID的完整性
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
-- 检查是否有无效的标签ID
SELECT user_id, tag_ids 
FROM user_tags 
WHERE JSON_EXTRACT(tag_ids, '$[0]') > 8 
   OR JSON_EXTRACT(tag_ids, '$[0]') < 1
LIMIT 5;
"

# 检查JSON格式完整性
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
-- 检查JSON格式是否正确
SELECT COUNT(*) as invalid_json_count
FROM user_tags 
WHERE JSON_VALID(tag_ids) = 0 OR JSON_VALID(tag_details) = 0;
"
```

## 📈 测试报告生成

### 自动化测试报告
```bash
# 生成测试报告
cat > generate_test_report.sh << 'EOF'
#!/bin/bash

echo "# 标签系统测试报告" > test_report.md
echo "生成时间: $(date)" >> test_report.md
echo "" >> test_report.md

echo "## 系统概况" >> test_report.md
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT 
    COUNT(*) as total_users,
    COUNT(DISTINCT JSON_EXTRACT(tag_ids, '$[*]')) as unique_tags,
    AVG(JSON_LENGTH(tag_ids)) as avg_tags_per_user,
    MAX(JSON_LENGTH(tag_ids)) as max_tags_per_user,
    MIN(created_time) as earliest_created,
    MAX(updated_time) as latest_updated
FROM user_tags;
" >> test_report.md

echo "" >> test_report.md
echo "## 各标签用户数量分布" >> test_report.md
for tag_id in {1..8}; do
    count=$(docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
    USE tag_system;
    SELECT COUNT(*) FROM user_tags WHERE JSON_CONTAINS(tag_ids, '$tag_id');
    " | tail -n 1)
    
    tag_name=$(docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
    USE tag_system;
    SELECT tag_name FROM tag_definition WHERE tag_id = $tag_id;
    " | tail -n 1)
    
    echo "- 标签$tag_id ($tag_name): $count 用户" >> test_report.md
done

echo "" >> test_report.md
echo "## 时间戳分析" >> test_report.md
docker exec tag_system_mysql mysql -u root -proot123 --default-character-set=utf8mb4 -e "
USE tag_system;
SELECT 
    COUNT(CASE WHEN created_time = updated_time THEN 1 END) as never_updated_users,
    COUNT(CASE WHEN created_time != updated_time THEN 1 END) as updated_users,
    TIMESTAMPDIFF(SECOND, MIN(created_time), MAX(updated_time)) as time_span_seconds
FROM user_tags;
" >> test_report.md

echo "测试报告已生成: test_report.md"
EOF

chmod +x generate_test_report.sh
./generate_test_report.sh
```

## 🚨 错误场景测试

### 1. 异常输入测试
```bash
# 测试无效参数
echo "测试无效标签ID..."
python main.py --env local --mode tags-parallel --tag-ids 999,1000
echo "退出码: $?"

echo "测试无效用户ID..."
python main.py --env local --mode users-parallel --user-ids invalid_user_123
echo "退出码: $?"

# 测试空参数
echo "测试空标签列表..."
python main.py --env local --mode tags-parallel --tag-ids ""
echo "退出码: $?"
```

### 2. 数据库连接中断测试
```bash
# 模拟数据库连接问题
echo "停止MySQL服务..."
docker-compose -f environments/local/docker-compose.yml stop tag_system_mysql

echo "尝试执行标签计算（应该失败）..."
python main.py --env local --mode health
echo "退出码: $?"

echo "重启MySQL服务..."
docker-compose -f environments/local/docker-compose.yml start tag_system_mysql
sleep 30

echo "重新尝试（应该成功）..."
python main.py --env local --mode health
echo "退出码: $?"
```

## 📋 测试检查清单

使用以下检查清单确保测试完整性：

### ✅ 基础功能测试
- [ ] 健康检查通过
- [ ] 6种并行计算场景都能正常执行
- [ ] 数据库连接正常
- [ ] 测试数据生成正确

### ✅ 时间戳逻辑测试
- [ ] 相同操作不更新时间戳
- [ ] 标签变化正确更新 `updated_time`
- [ ] `created_time` 永远不变
- [ ] 幂等性验证通过

### ✅ 标签合并测试
- [ ] 新旧标签正确合并
- [ ] 标签去重正确
- [ ] 标签合并触发时间戳更新

### ✅ 性能测试
- [ ] 并行模式比单线程模式快
- [ ] 大规模数据处理正常
- [ ] 内存使用在合理范围

### ✅ 数据验证测试
- [ ] 标签规则逻辑正确
- [ ] 数据一致性检查通过
- [ ] JSON格式验证正确

### ✅ 异常处理测试
- [ ] 无效参数处理正确
- [ ] 数据库连接异常恢复正常
- [ ] 错误信息清晰易懂

## 🎯 测试最佳实践

1. **测试前准备**：始终从干净环境开始
2. **数据验证**：每次测试后验证数据正确性
3. **时间戳检查**：重点关注UPSERT时间戳逻辑
4. **性能监控**：记录执行时间和资源使用
5. **错误处理**：测试各种异常场景
6. **文档记录**：记录测试结果和发现的问题

通过完成本指南中的所有测试，您可以确信标签系统的各个功能模块都能正常工作，特别是关键的UPSERT时间戳机制。