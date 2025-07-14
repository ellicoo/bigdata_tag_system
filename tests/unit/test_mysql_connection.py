#!/usr/bin/env python3
"""
简单的MySQL连接测试脚本
"""

import pymysql
import time

def test_mysql_connection():
    """测试MySQL连接"""
    try:
        print("开始测试MySQL连接...")
        
        connection = pymysql.connect(
            host='localhost',
            port=3307,
            user='root',
            password='root123',
            database='tag_system',
            charset='utf8mb4',
            autocommit=False,
            connect_timeout=30,
            read_timeout=60,
            write_timeout=60
        )
        
        print("✅ MySQL连接成功")
        
        cursor = connection.cursor()
        
        # 测试简单查询
        cursor.execute("SELECT COUNT(*) FROM tag_definition")
        count = cursor.fetchone()[0]
        print(f"✅ 查询成功，tag_definition表有 {count} 条记录")
        
        # 测试简单插入
        test_sql = """
        INSERT INTO user_tags (user_id, tag_ids, tag_details, computed_date) 
        VALUES (%s, %s, %s, %s)
        """
        
        test_data = [
            ('test_user_001', '[1,2,3]', '{"1": {"tag_name": "测试标签"}}', '2025-07-14'),
            ('test_user_002', '[2,3,4]', '{"2": {"tag_name": "测试标签2"}}', '2025-07-14')
        ]
        
        print("开始测试批量插入...")
        start_time = time.time()
        
        cursor.executemany(test_sql, test_data)
        connection.commit()
        
        end_time = time.time()
        print(f"✅ 批量插入成功，耗时: {end_time - start_time:.2f}秒")
        
        # 清理测试数据
        cursor.execute("DELETE FROM user_tags WHERE user_id LIKE 'test_user_%'")
        connection.commit()
        print("✅ 测试数据清理完成")
        
        connection.close()
        print("✅ 连接关闭成功")
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_mysql_connection()
    if success:
        print("🎉 MySQL连接测试通过")
    else:
        print("💥 MySQL连接测试失败")