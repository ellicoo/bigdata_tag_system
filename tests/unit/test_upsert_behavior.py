#!/usr/bin/env python3
"""
测试UPSERT行为验证脚本
验证是否符合预期：相同数据再次写入不应该更新updated_time
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.common.config.manager import ConfigManager
from src.batch.orchestrator.batch_orchestrator import BatchOrchestrator
import time
import pymysql

def check_database_state():
    """检查数据库状态"""
    config = ConfigManager.load_config('local')
    
    connection = pymysql.connect(
        host=config.mysql.host,
        port=config.mysql.port,
        user=config.mysql.username,
        password=config.mysql.password,
        database=config.mysql.database,
        charset='utf8mb4'
    )
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM user_tags")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT user_id, tag_ids, created_time, updated_time FROM user_tags ORDER BY user_id LIMIT 3")
        sample_records = cursor.fetchall()
        
        print(f"总记录数: {total_records}")
        print("样本记录:")
        for record in sample_records:
            print(f"  用户: {record[0]}, 标签: {record[1]}, 创建时间: {record[2]}, 更新时间: {record[3]}")
        
        return total_records, sample_records
        
    finally:
        connection.close()

def test_upsert_behavior():
    """测试UPSERT行为"""
    print("=== UPSERT行为测试 ===")
    
    config = ConfigManager.load_config('local')
    scheduler = TagScheduler(config, max_workers=2)
    
    try:
        scheduler.initialize()
        
        print("\n1. 第一次运行全量并行场景...")
        success1 = scheduler.scenario_1_full_users_full_tags()
        print(f"第一次运行结果: {'成功' if success1 else '失败'}")
        
        if success1:
            print("\n第一次运行后的数据库状态:")
            total1, sample1 = check_database_state()
            
            print("\n等待5秒...")
            time.sleep(5)
            
            print("\n2. 第二次运行相同的全量并行场景...")
            success2 = scheduler.scenario_1_full_users_full_tags()
            print(f"第二次运行结果: {'成功' if success2 else '失败'}")
            
            if success2:
                print("\n第二次运行后的数据库状态:")
                total2, sample2 = check_database_state()
                
                print("\n=== 验证结果 ===")
                print(f"记录数变化: {total1} -> {total2}")
                
                if total1 == total2:
                    print("✅ 记录数没有变化，符合预期")
                else:
                    print("❌ 记录数发生了变化，不符合预期")
                
                print("\n时间戳变化检查:")
                for i, (r1, r2) in enumerate(zip(sample1, sample2)):
                    user_id = r1[0]
                    updated_time_1 = r1[3]
                    updated_time_2 = r2[3]
                    
                    if updated_time_1 == updated_time_2:
                        print(f"  用户 {user_id}: 时间戳未变化 ✅")
                    else:
                        print(f"  用户 {user_id}: 时间戳变化了 {updated_time_1} -> {updated_time_2} ❌")
                
                return total1 == total2 and all(r1[3] == r2[3] for r1, r2 in zip(sample1, sample2))
    
    except Exception as e:
        print(f"测试失败: {e}")
        return False
    
    finally:
        scheduler.cleanup()

if __name__ == "__main__":
    success = test_upsert_behavior()
    sys.exit(0 if success else 1)