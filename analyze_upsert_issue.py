#!/usr/bin/env python3
"""
分析UPSERT逻辑问题
"""

# 分析当前的UPSERT逻辑
current_upsert_sql = """
INSERT INTO user_tags (user_id, tag_ids, tag_details) 
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
    tag_ids = CASE 
        WHEN tag_ids != VALUES(tag_ids) OR tag_details != VALUES(tag_details) 
        THEN VALUES(tag_ids) 
        ELSE tag_ids 
    END,
    tag_details = CASE 
        WHEN tag_ids != VALUES(tag_ids) OR tag_details != VALUES(tag_details) 
        THEN VALUES(tag_details) 
        ELSE tag_details 
    END,
    updated_time = CASE 
        WHEN tag_ids != VALUES(tag_ids) OR tag_details != VALUES(tag_details) 
        THEN CURRENT_TIMESTAMP 
        ELSE updated_time 
    END
"""

print("=== 当前UPSERT逻辑分析 ===")
print("问题分析:")
print("1. JSON比较问题:")
print("   - MySQL中的JSON比较可能不是按字节对比")
print("   - 相同的JSON内容可能有不同的格式（空格、顺序等）")
print("   - 导致 tag_ids != VALUES(tag_ids) 始终为true")
print()

print("2. 具体场景:")
print("   - 第一次插入: [1,2,3] -> 正常插入")
print("   - 第二次插入: [1,2,3] -> 但JSON字符串可能格式不同")
print("   - 结果: MySQL认为不同，触发updated_time更新")
print()

print("3. 解决方案:")
print("   - 方案1: 使用JSON_EXTRACT和JSON_ARRAY_CONTAINS比较")
print("   - 方案2: 使用MD5或SHA256哈希值比较")
print("   - 方案3: 标准化JSON格式后比较")
print()

# 建议的修复方案
improved_upsert_sql = """
INSERT INTO user_tags (user_id, tag_ids, tag_details) 
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
    tag_ids = CASE 
        WHEN JSON_EXTRACT(tag_ids, '$') != JSON_EXTRACT(VALUES(tag_ids), '$') 
        THEN VALUES(tag_ids) 
        ELSE tag_ids 
    END,
    tag_details = CASE 
        WHEN JSON_EXTRACT(tag_ids, '$') != JSON_EXTRACT(VALUES(tag_ids), '$')
        THEN VALUES(tag_details) 
        ELSE tag_details 
    END,
    updated_time = CASE 
        WHEN JSON_EXTRACT(tag_ids, '$') != JSON_EXTRACT(VALUES(tag_ids), '$')
        THEN CURRENT_TIMESTAMP 
        ELSE updated_time 
    END
"""

print("=== 改进的UPSERT逻辑 ===")
print(improved_upsert_sql)
print()

print("=== 更简单的解决方案 ===")
print("由于我们只关心tag_ids数组，可以使用更直接的方法:")

simple_upsert_sql = """
INSERT INTO user_tags (user_id, tag_ids, tag_details) 
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
    tag_ids = VALUES(tag_ids),
    tag_details = VALUES(tag_details),
    updated_time = CASE 
        WHEN tag_ids <> VALUES(tag_ids) 
        THEN CURRENT_TIMESTAMP 
        ELSE updated_time 
    END
"""

print(simple_upsert_sql)
print()
print("注意: 使用 <> 操作符而不是 != 可能在某些情况下更稳定")