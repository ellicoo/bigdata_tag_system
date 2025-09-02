#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证小数点字符串字段的数值比较是否正确
基于model/demo.sql中的实际规则格式进行测试
"""

import sys
sys.path.append('/Users/otis/PycharmProjects/bigdata_tag_system')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.tag_engine.parser.TagRuleParser import TagRuleParser
import json


def create_spark():
    """创建Spark会话"""
    return SparkSession.builder \
        .appName("DecimalComparisonTest") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


def test_key_numeric_rules():
    """测试关键的数值比较规则"""
    print("🔍 测试关键数值比较规则 (基于model/demo.sql)")
    
    spark = create_spark()
    parser = TagRuleParser()
    
    # 从model/demo.sql中选取关键的数值比较规则
    test_rules = [
        {
            "tag_id": 5,
            "rule": '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "available_balance", "operator": ">=", "value": "50000", "type": "number"}]}}]}',
            "description": "可用余额 >= 50000"
        },
        {
            "tag_id": 12, 
            "rule": '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_trading_df", "field": "spot_trading_volume", "operator": ">=", "value": "100000", "type": "number"}]}}]}',
            "description": "现货交易量 >= 100000"
        },
        {
            "tag_id": 57,
            "rule": '{"logic": "AND", "conditions": [{"condition": {"logic": "OR", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "current_total_position_value", "operator": ">=", "value": "100000", "type": "number"}, {"table": "dws_user.dws_user_trading_df", "field": "contract_trading_volume", "operator": ">=", "value": "500000", "type": "number"}]}}]}',
            "description": "高价值用户: 总持仓 >= 100000 OR 合约交易量 >= 500000"
        }
    ]
    
    # 创建问题用户数据 - 字符串类型（模拟Hive表）
    problem_user_data = {
        "user_id": "11248360576",
        "available_balance": "21.330918335534",
        "spot_trading_volume": "51.742852600000000000", 
        "current_total_position_value": "25000.50",
        "contract_trading_volume": "75000.123456"
    }
    
    # 创建其他测试用户
    test_users_data = [
        {
            "user_id": "test_user_1",
            "available_balance": "75000.0",
            "spot_trading_volume": "150000.0",
            "current_total_position_value": "120000.0", 
            "contract_trading_volume": "600000.0"
        },
        {
            "user_id": "test_user_2", 
            "available_balance": "25000.0",
            "spot_trading_volume": "50000.0",
            "current_total_position_value": "80000.0",
            "contract_trading_volume": "300000.0" 
        }
    ]
    
    all_users = [problem_user_data] + test_users_data
    
    for rule_info in test_rules:
        print(f"\n{'='*60}")
        print(f"🏷️  标签 {rule_info['tag_id']}: {rule_info['description']}")
        print(f"{'='*60}")
        
        # 解析规则生成SQL
        sql_condition = parser.parseRuleToSql(rule_info['rule'], None)
        print(f"📝 生成的SQL: {sql_condition}")
        
        # 根据规则创建对应的测试数据
        if rule_info['tag_id'] == 5:
            # available_balance >= 50000
            df_data = [(user['user_id'], user['available_balance']) for user in all_users]
            df = spark.createDataFrame(df_data, ["user_id", "available_balance"]).alias("dws_user_asset_df")
            
        elif rule_info['tag_id'] == 12:
            # spot_trading_volume >= 100000  
            df_data = [(user['user_id'], user['spot_trading_volume']) for user in all_users]
            df = spark.createDataFrame(df_data, ["user_id", "spot_trading_volume"]).alias("dws_user_trading_df")
            
        elif rule_info['tag_id'] == 57:
            # 复杂OR条件，需要JOIN
            asset_data = [(user['user_id'], user['current_total_position_value']) for user in all_users]
            trading_data = [(user['user_id'], user['contract_trading_volume']) for user in all_users]
            
            asset_df = spark.createDataFrame(asset_data, ["user_id", "current_total_position_value"]).alias("dws_user_asset_df")
            trading_df = spark.createDataFrame(trading_data, ["user_id", "contract_trading_volume"]).alias("dws_user_trading_df")
            
            df = asset_df.join(trading_df, "user_id", "inner")
        
        print(f"\n📊 测试数据:")
        df.show(truncate=False)
        
        try:
            # 应用SQL条件过滤
            result_df = df.filter(expr(sql_condition))
            matched_count = result_df.count()
            total_count = df.count()
            
            print(f"\n🎯 匹配结果: {matched_count}/{total_count} 个用户")
            
            if matched_count > 0:
                print("匹配的用户:")
                result_df.show(truncate=False)
                
                matched_users = [row.user_id for row in result_df.collect()]
                
                # 关键检查：问题用户是否被错误匹配
                if "11248360576" in matched_users:
                    print("❌ 问题发现: 用户11248360576被匹配了!")
                    print("   根据规则，该用户不应该匹配:")
                    
                    if rule_info['tag_id'] == 5:
                        print(f"   - available_balance: {problem_user_data['available_balance']} < 50000")
                    elif rule_info['tag_id'] == 12:
                        print(f"   - spot_trading_volume: {problem_user_data['spot_trading_volume']} < 100000")
                    elif rule_info['tag_id'] == 57:
                        print(f"   - current_total_position_value: {problem_user_data['current_total_position_value']} < 100000")
                        print(f"   - contract_trading_volume: {problem_user_data['contract_trading_volume']} < 500000")
                        print(f"   - OR条件: False OR False = False")
                    
                    print("   🔍 可能原因: 字符串字典序比较问题")
                else:
                    print("✅ 正确: 用户11248360576没有被错误匹配")
            else:
                print("✅ 没有用户匹配 (可能正确，需要检查数据)")
                
        except Exception as e:
            print(f"❌ SQL执行失败: {e}")
            import traceback
            traceback.print_exc()
    
    spark.stop()


def test_decimal_edge_cases():
    """测试小数点边缘情况"""
    print(f"\n{'='*60}")
    print("🔍 小数点边缘情况测试")
    print(f"{'='*60}")
    
    spark = create_spark()
    parser = TagRuleParser()
    
    # 简单的 >= 规则
    simple_rule = '{"logic": "AND", "conditions": [{"condition": {"logic": "None", "fields": [{"table": "dws_user.dws_user_asset_df", "field": "balance", "operator": ">=", "value": "100000", "type": "number"}]}}]}'
    
    sql_condition = parser.parseRuleToSql(simple_rule, ["dws_user.dws_user_asset_df"])
    print(f"📝 SQL: {sql_condition}")
    
    # 边缘测试数据
    edge_test_data = [
        ("user1", "21.330918335534"),        # 小数 < 100000 (应该不匹配)
        ("user2", "51.742852600000000000"),  # 小数 < 100000 (应该不匹配)  
        ("user3", "99999.99"),               # 接近但小于 (应该不匹配)
        ("user4", "100000.0"),               # 等于 (应该匹配)
        ("user5", "100000.01"),              # 略大于 (应该匹配)
        ("user6", "150000.123456"),          # 明显大于 (应该匹配)
        ("user7", "999999.999999"),          # 很大的数 (应该匹配)
    ]
    
    df = spark.createDataFrame(edge_test_data, ["user_id", "balance"]).alias("dws_user_asset_df")
    
    print(f"\n📊 边缘测试数据:")
    df.show(truncate=False)
    
    # 应用条件
    result_df = df.filter(expr(sql_condition))
    matched_count = result_df.count()
    total_count = df.count()
    
    print(f"\n🎯 匹配结果: {matched_count}/{total_count}")
    
    if matched_count > 0:
        result_df.show(truncate=False)
        matched_users = [row.user_id for row in result_df.collect()]
        
        # 分析每个用户
        for user_data in edge_test_data:
            user_id, balance = user_data
            should_match = float(balance) >= 100000
            actually_matched = user_id in matched_users
            
            status = "✅" if should_match == actually_matched else "❌"
            print(f"   {status} {user_id}: {balance} >= 100000 -> 预期:{should_match}, 实际:{actually_matched}")
            
            if should_match != actually_matched:
                print(f"      🚨 字符串比较问题: '{balance}' >= '100000' = {balance >= '100000'}")
    
    spark.stop()


def main():
    """主测试函数"""
    print("🚀 验证小数点字符串字段的数值比较")
    print("基于model/demo.sql中的实际规则格式")
    print("="*80)
    
    try:
        # 测试1: 关键数值规则
        test_key_numeric_rules()
        
        # 测试2: 小数点边缘情况
        test_decimal_edge_cases()
        
        print("\n" + "="*80)
        print("🎉 测试完成!")
        print("\n📋 结论:")
        print("1. 如果用户11248360576被错误匹配，说明存在字符串字典序比较问题")
        print("2. 如果小数点边缘测试中出现❌，说明需要在数值操作符中添加CAST转换") 
        print("3. 正确的修复方法是在 >= > < <= 操作符中使用 CAST(field AS DECIMAL)")
        
    except Exception as e:
        print(f"❌ 测试执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()