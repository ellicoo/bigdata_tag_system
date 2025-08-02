#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签质量测试
测试标签打标逻辑的正确性，包含简单条件和复杂条件的全面验证
"""
import pytest
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.tag_engine.parser.TagRuleParser import TagRuleParser
from src.tag_engine.utils.tagExpressionUtils import buildParallelTagExpression


class TestTagQuality:
    """标签质量测试类"""
    
    def test_simple_age_tag_quality(self, spark):
        """测试简单年龄标签质量 - 成年用户标签"""
        print("\n🧪 测试简单条件：成年用户标签(age >= 18)")
        
        # 标签规则：成年用户
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [{
                "condition": {
                    "logic": "None",
                    "fields": [{
                        "table": "user_basic_info",
                        "field": "age", 
                        "operator": ">=",
                        "value": "18",
                        "type": "number"
                    }]
                }
            }]
        })
        
        # 创建测试用户数据
        test_data = [
            ("user001", 25),    # ✅ 应该有标签
            ("user002", 17),    # ❌ 不应该有标签 
            ("user003", 18),    # ✅ 应该有标签(边界值)
            ("user004", 30),    # ✅ 应该有标签
            ("user005", 16)     # ❌ 不应该有标签
        ]
        
        testDF = spark.createDataFrame(test_data, ["user_id", "age"])
        
        # 解析规则为SQL条件
        parser = TagRuleParser()
        sql_condition = parser.parseRuleToSql(rule_json, ["user_basic_info"])
        print(f"   SQL条件: {sql_condition}")
        
        # 构建标签表达式 - 需要将表前缀替换为实际的列名格式
        simple_condition = sql_condition.replace("user_basic_info.", "")
        tag_conditions = [{'tag_id': 1, 'condition': simple_condition}]
        tag_expr = buildParallelTagExpression(tag_conditions)
        
        # 应用标签逻辑
        resultDF = testDF.withColumn("tag_ids_array", tag_expr)
        results = resultDF.collect()
        
        # 验证结果
        expected_results = {
            "user001": [1],  # age=25 >= 18 ✅
            "user002": [],   # age=17 < 18 ❌  
            "user003": [1],  # age=18 >= 18 ✅
            "user004": [1],  # age=30 >= 18 ✅
            "user005": []    # age=16 < 18 ❌
        }
        
        for row in results:
            user_id = row.user_id
            actual_tags = row.tag_ids_array
            expected_tags = expected_results[user_id]
            print(f"   {user_id}: age={row.age} → 预期{expected_tags}, 实际{actual_tags}")
            assert actual_tags == expected_tags, f"用户{user_id}标签不匹配"
        
        print("   ✅ 简单年龄标签测试通过")
    
    def test_vip_level_tag_quality(self, spark):
        """测试VIP等级标签质量 - 枚举条件"""
        print("\n🧪 测试枚举条件：高级VIP标签(VIP2,VIP3,VIP4)")
        
        # 标签规则：高级VIP用户
        rule_json = json.dumps({
            "logic": "AND", 
            "conditions": [{
                "condition": {
                    "logic": "None",
                    "fields": [{
                        "table": "user_basic_info",
                        "field": "user_level",
                        "operator": "belongs_to", 
                        "value": ["VIP2", "VIP3", "VIP4"],
                        "type": "enum"
                    }]
                }
            }]
        })
        
        # 创建测试用户数据
        test_data = [
            ("user001", "VIP1"),     # ❌ 不应该有标签
            ("user002", "VIP2"),     # ✅ 应该有标签
            ("user003", "VIP3"),     # ✅ 应该有标签
            ("user004", "VIP4"),     # ✅ 应该有标签
            ("user005", "NORMAL"),   # ❌ 不应该有标签
            ("user006", "VIP5")      # ❌ 不应该有标签(不在列表中)
        ]
        
        testDF = spark.createDataFrame(test_data, ["user_id", "user_level"])
        
        # 解析规则并应用
        parser = TagRuleParser()
        sql_condition = parser.parseRuleToSql(rule_json, ["user_basic_info"])
        print(f"   SQL条件: {sql_condition}")
        
        # 构建标签表达式 - 需要将表前缀替换为实际的列名格式
        simple_condition = sql_condition.replace("user_basic_info.", "")
        tag_conditions = [{'tag_id': 2, 'condition': simple_condition}]
        tag_expr = buildParallelTagExpression(tag_conditions)
        
        resultDF = testDF.withColumn("tag_ids_array", tag_expr)
        results = resultDF.collect()
        
        # 验证结果
        expected_results = {
            "user001": [],   # VIP1 不在[VIP2,VIP3,VIP4] ❌
            "user002": [2],  # VIP2 在列表中 ✅
            "user003": [2],  # VIP3 在列表中 ✅ 
            "user004": [2],  # VIP4 在列表中 ✅
            "user005": [],   # NORMAL 不在列表中 ❌
            "user006": []    # VIP5 不在列表中 ❌
        }
        
        for row in results:
            user_id = row.user_id
            actual_tags = row.tag_ids_array
            expected_tags = expected_results[user_id]
            print(f"   {user_id}: level={row.user_level} → 预期{expected_tags}, 实际{actual_tags}")
            assert actual_tags == expected_tags, f"用户{user_id}标签不匹配"
            
        print("   ✅ VIP等级标签测试通过")
    
    def test_complex_high_value_user_tag_quality(self, spark):
        """测试复杂条件：高净值用户标签 - 多表AND逻辑"""
        print("\n🧪 测试复杂AND条件：高净值用户标签")
        print("   规则：(age >= 30 AND assets >= 100000) AND (kyc_status = 'verified')")
        
        # 复杂标签规则：高净值用户
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "AND",
                        "fields": [
                            {
                                "table": "user_basic_info",
                                "field": "age",
                                "operator": ">=",
                                "value": "30", 
                                "type": "number"
                            },
                            {
                                "table": "user_asset_summary",
                                "field": "total_asset_value",
                                "operator": ">=",
                                "value": "100000",
                                "type": "number"
                            }
                        ]
                    }
                },
                {
                    "condition": {
                        "logic": "None",
                        "fields": [{
                            "table": "user_basic_info", 
                            "field": "kyc_status",
                            "operator": "=",
                            "value": "verified",
                            "type": "string"
                        }]
                    }
                }
            ]
        })
        
        # 创建多表测试数据
        test_data = [
            # user_id, age, total_asset_value, kyc_status
            ("user001", 35, 150000, "verified"),    # ✅ 全部满足
            ("user002", 25, 200000, "verified"),    # ❌ 年龄不够
            ("user003", 40, 50000, "verified"),     # ❌ 资产不够
            ("user004", 35, 150000, "pending"),     # ❌ KYC未验证
            ("user005", 45, 500000, "verified"),    # ✅ 全部满足
            ("user006", 28, 80000, "pending"),      # ❌ 全部不满足
            ("user007", 30, 100000, "verified")     # ✅ 边界值全部满足
        ]
        
        # 创建DataFrame（模拟多表JOIN后的结果）
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("age", IntegerType(), False), 
            StructField("total_asset_value", IntegerType(), False),
            StructField("kyc_status", StringType(), False)
        ])
        
        testDF = spark.createDataFrame(test_data, schema)
        
        # 解析规则并应用
        parser = TagRuleParser()
        sql_condition = parser.parseRuleToSql(rule_json, ["user_basic_info", "user_asset_summary"])
        print(f"   SQL条件: {sql_condition}")
        
        # 构建标签表达式 - 移除表前缀
        simple_condition = sql_condition.replace("user_basic_info.", "").replace("user_asset_summary.", "")
        tag_conditions = [{'tag_id': 3, 'condition': simple_condition}]
        tag_expr = buildParallelTagExpression(tag_conditions)
        
        resultDF = testDF.withColumn("tag_ids_array", tag_expr)
        results = resultDF.collect()
        
        # 验证结果
        expected_results = {
            "user001": [3],  # 35>=30 ✅, 150000>=100000 ✅, verified ✅
            "user002": [],   # 25>=30 ❌, 200000>=100000 ✅, verified ✅
            "user003": [],   # 40>=30 ✅, 50000>=100000 ❌, verified ✅
            "user004": [],   # 35>=30 ✅, 150000>=100000 ✅, pending≠verified ❌
            "user005": [3],  # 45>=30 ✅, 500000>=100000 ✅, verified ✅
            "user006": [],   # 28>=30 ❌, 80000>=100000 ❌, pending≠verified ❌
            "user007": [3]   # 30>=30 ✅, 100000>=100000 ✅, verified ✅
        }
        
        for row in results:
            user_id = row.user_id
            actual_tags = row.tag_ids_array
            expected_tags = expected_results[user_id]
            
            # 详细验证逻辑
            age_ok = row.age >= 30
            asset_ok = row.total_asset_value >= 100000
            kyc_ok = row.kyc_status == "verified"
            should_have_tag = age_ok and asset_ok and kyc_ok
            
            print(f"   {user_id}: age={row.age}({age_ok}), assets={row.total_asset_value}({asset_ok}), kyc={row.kyc_status}({kyc_ok}) → 预期{expected_tags}, 实际{actual_tags}")
            
            assert actual_tags == expected_tags, f"用户{user_id}标签不匹配"
            assert (len(actual_tags) > 0) == should_have_tag, f"用户{user_id}逻辑判断错误"
            
        print("   ✅ 复杂高净值用户标签测试通过")
    
    def test_complex_or_logic_tag_quality(self, spark):
        """测试复杂OR条件：活跃用户标签"""
        print("\n🧪 测试复杂OR条件：活跃用户标签")
        print("   规则：(trade_count > 10) OR (last_login within 7 days) OR (cash_balance > 50000)")
        
        # 复杂OR逻辑标签规则
        rule_json = json.dumps({
            "logic": "OR",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [{
                            "table": "user_activity_summary",
                            "field": "trade_count_30d", 
                            "operator": ">",
                            "value": "10",
                            "type": "number"
                        }]
                    }
                },
                {
                    "condition": {
                        "logic": "None", 
                        "fields": [{
                            "table": "user_activity_summary",
                            "field": "days_since_login",
                            "operator": "<=",
                            "value": "7",
                            "type": "number"
                        }]
                    }
                },
                {
                    "condition": {
                        "logic": "None",
                        "fields": [{
                            "table": "user_asset_summary",
                            "field": "cash_balance",
                            "operator": ">", 
                            "value": "50000",
                            "type": "number"
                        }]
                    }
                }
            ]
        })
        
        # 创建测试数据
        test_data = [
            # user_id, trade_count_30d, days_since_login, cash_balance
            ("user001", 15, 3, 30000),    # ✅ 交易多
            ("user002", 5, 2, 20000),     # ✅ 最近登录
            ("user003", 3, 15, 80000),    # ✅ 现金多
            ("user004", 20, 1, 100000),   # ✅ 全部满足
            ("user005", 2, 30, 10000),    # ❌ 全部不满足
            ("user006", 11, 10, 60000),   # ✅ 交易多+现金多
            ("user007", 10, 7, 50000)     # ❌ 都是边界值但不满足
        ]
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("trade_count_30d", IntegerType(), False),
            StructField("days_since_login", IntegerType(), False), 
            StructField("cash_balance", IntegerType(), False)
        ])
        
        testDF = spark.createDataFrame(test_data, schema)
        
        # 解析规则并应用
        parser = TagRuleParser()
        sql_condition = parser.parseRuleToSql(rule_json, ["user_activity_summary", "user_asset_summary"])
        print(f"   SQL条件: {sql_condition}")
        
        # 构建标签表达式 - 移除表前缀
        simple_condition = sql_condition.replace("user_activity_summary.", "").replace("user_asset_summary.", "")
        tag_conditions = [{'tag_id': 4, 'condition': simple_condition}]
        tag_expr = buildParallelTagExpression(tag_conditions)
        
        resultDF = testDF.withColumn("tag_ids_array", tag_expr)
        results = resultDF.collect()
        
        # 验证结果
        for row in results:
            user_id = row.user_id
            actual_tags = row.tag_ids_array
            
            # OR逻辑验证
            trade_ok = row.trade_count_30d > 10
            login_ok = row.days_since_login <= 7  
            cash_ok = row.cash_balance > 50000
            should_have_tag = trade_ok or login_ok or cash_ok
            
            expected_tags = [4] if should_have_tag else []
            
            print(f"   {user_id}: trade={row.trade_count_30d}({trade_ok}), login_days={row.days_since_login}({login_ok}), cash={row.cash_balance}({cash_ok}) → 预期{expected_tags}, 实际{actual_tags}")
            
            assert actual_tags == expected_tags, f"用户{user_id}标签不匹配"
            
        print("   ✅ 复杂OR逻辑标签测试通过")
    
    def test_complex_nested_logic_tag_quality(self, spark):
        """测试嵌套逻辑：超级用户标签"""
        print("\n🧪 测试嵌套条件：超级用户标签")
        print("   规则：(age >= 25 AND assets >= 50000) AND ((vip_level IN [VIP3,VIP4]) OR (trade_count > 20))")
        
        # 复杂嵌套逻辑标签规则
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "AND",
                        "fields": [
                            {
                                "table": "user_basic_info",
                                "field": "age",
                                "operator": ">=", 
                                "value": "25",
                                "type": "number"
                            },
                            {
                                "table": "user_asset_summary", 
                                "field": "total_asset_value",
                                "operator": ">=",
                                "value": "50000",
                                "type": "number"
                            }
                        ]
                    }
                },
                {
                    "condition": {
                        "logic": "OR",
                        "conditions": [
                            {
                                "condition": {
                                    "logic": "None",
                                    "fields": [{
                                        "table": "user_basic_info",
                                        "field": "user_level", 
                                        "operator": "belongs_to",
                                        "value": ["VIP3", "VIP4"],
                                        "type": "enum"
                                    }]
                                }
                            },
                            {
                                "condition": {
                                    "logic": "None",
                                    "fields": [{
                                        "table": "user_activity_summary",
                                        "field": "trade_count_30d",
                                        "operator": ">",
                                        "value": "20", 
                                        "type": "number"
                                    }]
                                }
                            }
                        ]
                    }
                }
            ]
        })
        
        # 创建测试数据
        test_data = [
            # user_id, age, total_asset_value, user_level, trade_count_30d
            ("user001", 30, 80000, "VIP3", 15),    # ✅ age+assets ✅, VIP3 ✅
            ("user002", 28, 60000, "VIP2", 25),    # ✅ age+assets ✅, trade_count ✅
            ("user003", 20, 100000, "VIP4", 5),    # ❌ age不够
            ("user004", 35, 30000, "VIP3", 10),    # ❌ assets不够
            ("user005", 40, 120000, "VIP2", 15),   # ❌ 基础条件满足，但VIP级别和交易都不够
            ("user006", 25, 50000, "VIP4", 21),    # ✅ 全部满足(边界值)
            ("user007", 35, 200000, "VIP1", 50)    # ✅ age+assets ✅, trade_count ✅
        ]
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("age", IntegerType(), False),
            StructField("total_asset_value", IntegerType(), False),
            StructField("user_level", StringType(), False), 
            StructField("trade_count_30d", IntegerType(), False)
        ])
        
        testDF = spark.createDataFrame(test_data, schema)
        
        # 解析规则并应用
        parser = TagRuleParser()
        sql_condition = parser.parseRuleToSql(rule_json, ["user_basic_info", "user_asset_summary", "user_activity_summary"])
        print(f"   SQL条件: {sql_condition}")
        
        # 构建标签表达式 - 移除表前缀
        simple_condition = sql_condition.replace("user_basic_info.", "").replace("user_asset_summary.", "").replace("user_activity_summary.", "")
        tag_conditions = [{'tag_id': 5, 'condition': simple_condition}]
        tag_expr = buildParallelTagExpression(tag_conditions)
        
        resultDF = testDF.withColumn("tag_ids_array", tag_expr)
        results = resultDF.collect()
        
        # 验证结果
        for row in results:
            user_id = row.user_id
            actual_tags = row.tag_ids_array
            
            # 嵌套逻辑验证
            # 第一层AND：基础条件
            age_ok = row.age >= 25
            asset_ok = row.total_asset_value >= 50000
            basic_ok = age_ok and asset_ok
            
            # 第二层OR：VIP或活跃
            vip_ok = row.user_level in ["VIP3", "VIP4"]
            trade_ok = row.trade_count_30d > 20
            advanced_ok = vip_ok or trade_ok
            
            # 最终结果
            should_have_tag = basic_ok and advanced_ok
            expected_tags = [5] if should_have_tag else []
            
            print(f"   {user_id}: 基础({age_ok}&{asset_ok}={basic_ok}) AND 高级({vip_ok}|{trade_ok}={advanced_ok}) = {should_have_tag} → 预期{expected_tags}, 实际{actual_tags}")
            
            assert actual_tags == expected_tags, f"用户{user_id}标签不匹配"
            
        print("   ✅ 复杂嵌套逻辑标签测试通过")
    
    def test_multi_tag_parallel_quality(self, spark):
        """测试多标签并行计算质量"""
        print("\n🧪 测试多标签并行计算质量")
        print("   同时计算：成年标签(1) + VIP标签(2) + 高净值标签(3)")
        
        # 准备多个标签的条件
        tag_conditions = [
            # 标签1：成年用户 (age >= 18)  
            {'tag_id': 1, 'condition': 'age >= 18'},
            # 标签2：VIP用户 (user_level IN ('VIP2','VIP3','VIP4'))
            {'tag_id': 2, 'condition': "user_level IN ('VIP2','VIP3','VIP4')"},
            # 标签3：高资产用户 (total_asset_value >= 100000)
            {'tag_id': 3, 'condition': 'total_asset_value >= 100000'}
        ]
        
        # 创建测试数据
        test_data = [
            # user_id, age, user_level, total_asset_value
            ("user001", 25, "VIP3", 150000),   # ✅ 应该有标签 [1,2,3]
            ("user002", 17, "VIP2", 200000),   # ✅ 应该有标签 [2,3] (未成年)
            ("user003", 30, "VIP1", 50000),    # ✅ 应该有标签 [1] (普通VIP,低资产)
            ("user004", 16, "NORMAL", 20000),  # ❌ 应该有标签 [] (全部不满足)
            ("user005", 35, "VIP4", 500000),   # ✅ 应该有标签 [1,2,3] (全部满足)
            ("user006", 18, "VIP2", 100000)    # ✅ 应该有标签 [1,2,3] (边界值全部满足)
        ]
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("age", IntegerType(), False),
            StructField("user_level", StringType(), False),
            StructField("total_asset_value", IntegerType(), False)
        ])
        
        testDF = spark.createDataFrame(test_data, schema)
        
        # 构建并行标签表达式
        tag_expr = buildParallelTagExpression(tag_conditions)
        resultDF = testDF.withColumn("tag_ids_array", tag_expr)
        results = resultDF.collect()
        
        # 验证结果
        expected_results = {
            "user001": [1, 2, 3],  # 25>=18 ✅, VIP3∈[VIP2,VIP3,VIP4] ✅, 150000>=100000 ✅
            "user002": [2, 3],     # 17>=18 ❌, VIP2∈[VIP2,VIP3,VIP4] ✅, 200000>=100000 ✅  
            "user003": [1],        # 30>=18 ✅, VIP1∈[VIP2,VIP3,VIP4] ❌, 50000>=100000 ❌
            "user004": [],         # 16>=18 ❌, NORMAL∈[VIP2,VIP3,VIP4] ❌, 20000>=100000 ❌
            "user005": [1, 2, 3],  # 35>=18 ✅, VIP4∈[VIP2,VIP3,VIP4] ✅, 500000>=100000 ✅
            "user006": [1, 2, 3]   # 18>=18 ✅, VIP2∈[VIP2,VIP3,VIP4] ✅, 100000>=100000 ✅
        }
        
        for row in results:
            user_id = row.user_id
            actual_tags = sorted(row.tag_ids_array) if row.tag_ids_array else []
            expected_tags = expected_results[user_id]
            
            # 逐个验证条件
            age_ok = row.age >= 18
            vip_ok = row.user_level in ["VIP2", "VIP3", "VIP4"] 
            asset_ok = row.total_asset_value >= 100000
            
            print(f"   {user_id}: age={row.age}({age_ok}), level={row.user_level}({vip_ok}), assets={row.total_asset_value}({asset_ok}) → 预期{expected_tags}, 实际{actual_tags}")
            
            assert actual_tags == expected_tags, f"用户{user_id}多标签并行计算结果不匹配"
            
        print("   ✅ 多标签并行计算质量测试通过")
    
    def test_edge_cases_tag_quality(self, spark):
        """测试边界情况标签质量"""
        print("\n🧪 测试边界情况标签质量")
        
        # 测试NULL值处理
        test_data = [
            ("user001", None, "VIP2", 100000),    # age为NULL
            ("user002", 25, None, 150000),        # user_level为NULL
            ("user003", 30, "VIP3", None),        # total_asset_value为NULL
            ("user004", 25, "VIP2", 100000)       # 正常数据
        ]
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("age", IntegerType(), True),      # 允许NULL
            StructField("user_level", StringType(), True), # 允许NULL
            StructField("total_asset_value", IntegerType(), True) # 允许NULL
        ])
        
        testDF = spark.createDataFrame(test_data, schema)
        
        # 测试条件：age >= 25 AND user_level = 'VIP2' AND total_asset_value >= 100000
        tag_conditions = [
            {'tag_id': 99, 'condition': "age >= 25 AND user_level = 'VIP2' AND total_asset_value >= 100000"}
        ]
        
        tag_expr = buildParallelTagExpression(tag_conditions)  
        resultDF = testDF.withColumn("tag_ids_array", tag_expr)
        results = resultDF.collect()
        
        print("   测试NULL值处理:")
        for row in results:
            user_id = row.user_id
            actual_tags = row.tag_ids_array
            
            print(f"   {user_id}: age={row.age}, level={row.user_level}, assets={row.total_asset_value} → 标签{actual_tags}")
            
            # 只有user004应该有标签，其他因为有NULL值都不应该有标签
            if user_id == "user004":
                assert actual_tags == [99], f"用户{user_id}应该有标签"
            else:
                assert actual_tags == [], f"用户{user_id}因NULL值不应该有标签"
                
        print("   ✅ 边界情况标签质量测试通过")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])