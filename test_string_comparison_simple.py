#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试字符串比较逻辑
"""

def test_python_string_comparison():
    """测试Python字符串比较"""
    print("🔍 Python字符串比较测试:")
    
    test_cases = [
        ("21.330918335534", "100000"),
        ("51.742852600000000000", "500000"),
        ("100000", "100000"),
        ("99999", "100000"),
        ("500000", "500000"),
    ]
    
    for val1, val2 in test_cases:
        string_result = val1 >= val2
        numeric_result = float(val1) >= float(val2)
        print(f"  '{val1}' >= '{val2}' = {string_result} (数值: {numeric_result})")


def test_spark_string_comparison():
    """测试Spark SQL字符串比较"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    spark = SparkSession.builder \
        .appName("StringComparisonTest") \
        .master("local[1]") \
        .getOrCreate()
    
    print("\n🔍 Spark SQL字符串比较测试:")
    
    # 创建测试数据
    test_data = [
        ("user1", "21.330918335534", "51.742852600000000000"),
        ("user2", "100000", "500000"),
        ("user3", "99999", "499999"),
    ]
    
    df = spark.createDataFrame(test_data, ["user_id", "balance", "volume"])
    
    # 测试字符串比较
    result_df = df.withColumn(
        "balance_match", col("balance") >= "100000"
    ).withColumn(
        "volume_match", col("volume") >= "500000"  
    ).withColumn(
        "or_result", col("balance_match") | col("volume_match")
    )
    
    print("  Spark SQL结果:")
    result_df.select("user_id", "balance", "volume", "balance_match", "volume_match", "or_result").show()
    
    spark.stop()


if __name__ == "__main__":
    test_python_string_comparison()
    test_spark_string_comparison()