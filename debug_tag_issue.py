#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算问题调试脚本
分析标签规则解析和计算逻辑
"""
import json
import sys
import os
sys.path.append('/Users/otis/PycharmProjects/bigdata_tag_system')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.tag_engine.parser.TagRuleParser import TagRuleParser
from src.tag_engine.utils.tagExpressionUtils import buildParallelTagExpression


def create_test_spark():
    """创建测试用的Spark会话"""
    return SparkSession.builder \
        .appName("TagDebugTest") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()


def test_rule_parsing():
    """测试标签规则解析"""
    print("🔍 测试1: 标签规则解析")
    
    # 问题标签规则
    rule_json = {
        "logic": "AND", 
        "conditions": [{
            "condition": {
                "logic": "OR", 
                "fields": [
                    {
                        "type": "USDT（折U后）", 
                        "field": "available_balance", 
                        "table": "dws_user.dws_user_asset_df", 
                        "value": 100000, 
                        "operator": ">="
                    }, 
                    {
                        "type": "USDT（折U后）", 
                        "field": "spot_trading_volume", 
                        "table": "dws_user.dws_user_trading_df", 
                        "value": 500000, 
                        "operator": ">="
                    }
                ]
            }
        }]
    }
    
    parser = TagRuleParser()
    
    # 解析规则
    rule_json_str = json.dumps(rule_json)
    tables = ["dws_user.dws_user_asset_df", "dws_user.dws_user_trading_df"]
    
    try:
        sql_condition = parser.parseRuleToSql(rule_json_str, tables)
        print(f"✅ 规则解析成功")
        print(f"📄 生成的SQL条件:")
        print(f"   {sql_condition}")
        
        # 分析SQL条件是否正确
        expected_parts = [
            "`dws_user_asset_df`.`available_balance` >= 100000",
            "`dws_user_trading_df`.`spot_trading_volume` >= 500000",
            "OR"
        ]
        
        for part in expected_parts:
            if part in sql_condition:
                print(f"   ✅ 包含预期部分: {part}")
            else:
                print(f"   ❌ 缺失预期部分: {part}")
                
        return sql_condition
        
    except Exception as e:
        print(f"❌ 规则解析失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_data_evaluation(spark, sql_condition):
    """测试数据评估"""
    print("\n🔍 测试2: 数据评估（字符串类型）")
    
    if not sql_condition:
        print("❌ 没有SQL条件，跳过数据评估")
        return
    
    # 🎯 关键：使用字符串类型数据（模拟大数据场景）
    asset_data = [
        ("11248360576", "21.330918335534")  # 字符串类型
    ]
    
    trading_data = [
        ("11248360576", "51.742852600000000000")  # 字符串类型
    ]
    
    # 创建DataFrame - 字符串类型字段
    from pyspark.sql.types import StringType, StructType, StructField
    
    asset_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("available_balance", StringType(), True)
    ])
    
    trading_schema = StructType([
        StructField("user_id", StringType(), True), 
        StructField("spot_trading_volume", StringType(), True)
    ])
    
    asset_df = spark.createDataFrame(asset_data, asset_schema) \
        .alias("dws_user_asset_df")
    
    trading_df = spark.createDataFrame(trading_data, trading_schema) \
        .alias("dws_user_trading_df")
    
    # JOIN数据
    joined_df = asset_df.join(trading_df, "user_id", "inner")
    
    print(f"📊 测试数据:")
    joined_df.show()
    
    # 应用条件过滤
    print(f"📝 应用SQL条件: {sql_condition}")
    
    try:
        # 构建完整的SQL表达式进行测试
        test_expr = f"CASE WHEN {sql_condition} THEN 1 ELSE 0 END"
        
        result_df = joined_df.withColumn("tag_matched", expr(test_expr))
        
        print(f"🎯 条件评估结果:")
        result_df.select("user_id", "available_balance", "spot_trading_volume", "tag_matched").show()
        
        # 检查是否匹配
        matched_count = result_df.filter(col("tag_matched") == 1).count()
        total_count = result_df.count()
        
        print(f"📈 匹配结果: {matched_count}/{total_count} 个用户匹配标签")
        
        if matched_count > 0:
            print("❌ 错误: 用户不应该匹配此标签!")
            print("   用户数据都不满足条件:")
            print("   - available_balance: 21.33 < 100000 ❌")
            print("   - spot_trading_volume: 51.74 < 500000 ❌")
            print("   - OR条件: False OR False = False ❌")
        else:
            print("✅ 正确: 用户确实不匹配此标签")
            
        return result_df
        
    except Exception as e:
        print(f"❌ 数据评估失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_parallel_expression(spark):
    """测试并行标签表达式"""
    print("\n🔍 测试3: 并行标签表达式构建")
    
    # 模拟标签条件（已解析的SQL条件）
    tag_conditions = [
        {
            'tag_id': 2, 
            'condition': '(`dws_user_asset_df`.`available_balance` >= 100000 OR `dws_user_trading_df`.`spot_trading_volume` >= 500000)'
        }
    ]
    
    try:
        # 构建并行表达式
        parallel_expr = buildParallelTagExpression(tag_conditions)
        
        print(f"✅ 并行表达式构建成功")
        
        # 创建测试数据
        test_data = [
            ("11248360576", 21.330918335534, 51.742852600000),
            ("test_user_1", 150000.0, 30000.0),  # 应该匹配：余额满足
            ("test_user_2", 50000.0, 600000.0),  # 应该匹配：交易量满足
            ("test_user_3", 50000.0, 30000.0),   # 不应该匹配：都不满足
        ]
        
        test_df = spark.createDataFrame(
            test_data, 
            ["user_id", "available_balance", "spot_trading_volume"]
        ).alias("dws_user_asset_df").alias("dws_user_trading_df")
        
        # 应用并行表达式
        result_df = test_df.withColumn("tag_ids_array", parallel_expr)
        
        print(f"🎯 并行表达式结果:")
        result_df.select("user_id", "available_balance", "spot_trading_volume", "tag_ids_array").show(truncate=False)
        
        # 分析结果
        results = result_df.collect()
        for row in results:
            user_id = row['user_id']
            tags = row['tag_ids_array']
            balance = row['available_balance']
            volume = row['spot_trading_volume']
            
            should_match = balance >= 100000 or volume >= 500000
            actually_matched = len(tags) > 0 and 2 in tags
            
            status = "✅" if should_match == actually_matched else "❌"
            print(f"   {status} {user_id}: 预期匹配={should_match}, 实际匹配={actually_matched}, 标签={tags}")
        
        return result_df
        
    except Exception as e:
        print(f"❌ 并行表达式测试失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_edge_case_string_comparison(spark):
    """测试边缘情况：字符串比较逻辑"""
    print("\n🔍 测试4: 边缘情况 - 字符串比较逻辑")
    
    # 测试字符串比较的边缘情况
    test_cases = [
        ("21.330918335534", "100000", ">="),
        ("51.742852600000000000", "500000", ">="),
        ("100000", "100000", ">="),
        ("500000", "500000", ">="),
        ("99999", "100000", ">="),
    ]
    
    print("📊 字符串比较测试:")
    for val1, val2, op in test_cases:
        if op == ">=":
            result = val1 >= val2
            numeric_result = float(val1) >= float(val2)
            status = "✅" if result == numeric_result else "❌"
            print(f"   {status} '{val1}' >= '{val2}' = {result} (数值比较: {numeric_result})")
    
    # 🎯 重点测试：问题用户的字符串比较
    problem_balance = "21.330918335534"
    problem_volume = "51.742852600000000000"
    
    balance_match = problem_balance >= "100000"
    volume_match = problem_volume >= "500000"
    or_result = balance_match or volume_match
    
    print(f"\n🎯 问题用户字符串比较分析:")
    print(f"   available_balance: '{problem_balance}' >= '100000' = {balance_match}")
    print(f"   spot_trading_volume: '{problem_volume}' >= '500000' = {volume_match}")
    print(f"   OR结果: {balance_match} OR {volume_match} = {or_result}")
    
    if or_result:
        print(f"   ❌ 字符串比较导致误匹配！")
        print(f"   📝 原因分析：字符串按字典序比较，'21.33...' < '100000'但可能有其他逻辑")
    else:
        print(f"   ✅ 字符串比较正确，用户不应该匹配")


def test_tag_replacement_logic(spark):
    """测试标签替换逻辑 - 边缘情况"""
    print("\n🔍 测试5: 标签替换逻辑 - 边缘情况")
    
    from src.tag_engine.utils.SparkUdfs import replace_computed_tags
    
    # 🎯 你的边缘测试场景：用户原有标签[2]，本次计算标签[2,3,4]，本次结果[]（都没匹配）
    test_data = [
        ("11248360576", [2], [], [2, 3, 4])  # (user_id, existing_tags, computed_tags, computed_scope)
    ]
    
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("existing_tags", ArrayType(IntegerType())),
        StructField("computed_tags", ArrayType(IntegerType())),
        StructField("computed_scope", ArrayType(IntegerType()))
    ])
    
    test_df = spark.createDataFrame(test_data, schema)
    
    # 应用标签替换逻辑
    result_df = test_df.withColumn(
        "final_tags",
        replace_computed_tags(
            col("computed_tags"),      # 本次计算结果: []
            col("existing_tags"),      # 现有标签: [1,2,5]  
            col("computed_scope")      # 本次计算范围: [2,3,4]
        )
    )
    
    print(f"🎯 标签替换结果:")
    result_df.select("user_id", "existing_tags", "computed_tags", "computed_scope", "final_tags").show(truncate=False)
    
    # 分析结果
    result = result_df.collect()[0]
    existing = result['existing_tags']
    computed = result['computed_tags'] 
    scope = result['computed_scope']
    final = result['final_tags']
    
    print(f"📋 逻辑分析:")
    print(f"   现有标签: {existing}")
    print(f"   本次计算范围: {scope}")
    print(f"   本次计算结果: {computed}")
    print(f"   最终标签: {final}")
    
    # 🎯 边缘测试场景：用户原有[2]，计算范围[2,3,4]，结果[]，预期最终[]
    expected = []
    
    if set(final) == set(expected):
        print(f"   ✅ 标签替换逻辑正确: 标签2被正确移除，最终标签为空")
    else:
        print(f"   ❌ 标签替换逻辑错误: 预期{expected}, 实际{final}")
    
    return result_df


def main():
    """主测试函数"""
    print("🚀 开始标签计算问题调试")
    
    spark = create_test_spark()
    
    try:
        # 测试1: 规则解析
        sql_condition = test_rule_parsing()
        
        # 测试2: 数据评估
        if sql_condition:
            test_data_evaluation(spark, sql_condition)
        
        # 测试3: 并行表达式
        test_parallel_expression(spark)
        
        # 测试4: 边缘情况字符串比较
        test_edge_case_string_comparison(spark)
        
        # 测试5: 标签替换逻辑
        test_tag_replacement_logic(spark)
        
        print("\n🎉 调试测试完成")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()