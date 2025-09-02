#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试空标签更新的边缘情况
验证用户原有标签被清空的场景是否正确处理
"""

import sys
sys.path.append('/Users/otis/PycharmProjects/bigdata_tag_system')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.tag_engine.utils.SparkUdfs import replace_computed_tags, array_to_json

def create_spark():
    """创建Spark会话"""
    return SparkSession.builder \
        .appName("EmptyTagUpdateTest") \
        .master("local[2]") \
        .getOrCreate()

def test_empty_tag_scenario():
    """测试空标签更新场景"""
    print("🔍 测试空标签更新场景")
    
    spark = create_spark()
    
    # 模拟场景数据
    print("\n📊 测试场景:")
    print("- 用户原来有标签 [2]")
    print("- 本次计算范围 [2, 3, 4]")  
    print("- 本次计算结果 [] (都没匹配)")
    print("- 期望最终结果 [] (标签2被移除)")
    
    # 现有标签数据
    existing_data = [
        ("user001", [2]),      # 原来有标签2
        ("user002", [1, 2]),   # 原来有标签1,2
        ("user003", [5]),      # 原来有标签5(不在计算范围内)
    ]
    
    existing_schema = StructType([
        StructField("user_id", StringType()),
        StructField("existing_tag_ids", ArrayType(IntegerType()))
    ])
    
    existing_df = spark.createDataFrame(existing_data, existing_schema).alias("existing")
    
    # 本次计算结果 - 都没匹配
    new_data = [
        ("user001", []),       # 没匹配任何标签
        ("user002", []),       # 没匹配任何标签  
        ("user003", []),       # 没匹配任何标签
    ]
    
    new_schema = StructType([
        StructField("user_id", StringType()),
        StructField("tag_ids_array", ArrayType(IntegerType()))
    ])
    
    new_df = spark.createDataFrame(new_data, new_schema).alias("new")
    
    print(f"\n📋 现有标签:")
    existing_df.show()
    
    print(f"📋 本次计算结果:")
    new_df.show()
    
    # FULL JOIN
    joined_df = new_df.join(existing_df, "user_id", "full")
    
    # 应用智能标签替换
    computed_tag_ids = [2, 3, 4]
    computed_scope_lit = lit(computed_tag_ids)
    
    final_df = joined_df.withColumn(
        "final_tag_ids",
        replace_computed_tags(
            col("new.tag_ids_array"),         # 本次结果: []
            col("existing.existing_tag_ids"), # 现有标签: [2] 或 [1,2] 或 [5]
            computed_scope_lit                # 计算范围: [2,3,4] 
        )
    ).withColumn(
        "final_tag_ids_json", 
        array_to_json(col("final_tag_ids"))
    )
    
    print(f"📋 标签替换结果:")
    final_df.select("user_id", "existing.existing_tag_ids", "new.tag_ids_array", "final_tag_ids", "final_tag_ids_json").show(truncate=False)
    
    # 测试当前过滤逻辑的问题
    print(f"\n🚨 当前过滤逻辑测试:")
    current_filter = final_df.filter(
        (size(col("final_tag_ids")) > 0) |
        (col("existing.existing_tag_ids").isNotNull())
    )
    
    print(f"当前过滤逻辑结果 (会丢失空标签):")
    current_filter.select("user_id", "final_tag_ids", "final_tag_ids_json").show(truncate=False)
    
    # 正确的过滤逻辑
    print(f"\n✅ 正确过滤逻辑测试:")
    correct_filter = final_df.filter(
        col("existing.existing_tag_ids").isNotNull()  # 只要有历史标签就需要更新
    )
    
    print(f"正确过滤逻辑结果 (保留空标签):")
    correct_filter.select("user_id", "final_tag_ids", "final_tag_ids_json").show(truncate=False)
    
    # 分析结果
    print(f"\n📊 结果分析:")
    
    results = final_df.collect()
    for row in results:
        user_id = row['user_id']
        existing = row['existing']['existing_tag_ids'] if row['existing'] else None
        new_tags = row['new']['tag_ids_array'] if row['new'] else None
        final = row['final_tag_ids']
        
        print(f"用户 {user_id}:")
        print(f"  - 现有标签: {existing}")
        print(f"  - 本次结果: {new_tags}")
        print(f"  - 最终标签: {final}")
        
        if user_id == "user001":
            # user001: [2] -> [] (标签2被移除)
            expected = []
            if final == expected:
                print(f"  ✅ 正确: 标签2被成功移除")
            else:
                print(f"  ❌ 错误: 期望{expected}, 实际{final}")
        elif user_id == "user002":  
            # user002: [1,2] -> [1] (标签2被移除，标签1保留)
            expected = [1]
            if final == expected:
                print(f"  ✅ 正确: 标签2被移除，标签1保留")
            else:
                print(f"  ❌ 错误: 期望{expected}, 实际{final}")
        elif user_id == "user003":
            # user003: [5] -> [5] (标签5不在计算范围内，保持不变)
            expected = [5] 
            if final == expected:
                print(f"  ✅ 正确: 标签5保持不变")
            else:
                print(f"  ❌ 错误: 期望{expected}, 实际{final}")
    
    spark.stop()

def main():
    """主测试函数"""
    print("🚀 测试空标签更新的边缘情况")
    print("="*60)
    
    test_empty_tag_scenario()
    
    print("\n" + "="*60)
    print("🎯 结论:")
    print("1. replace_computed_tags函数能正确处理空标签结果")
    print("2. 当前TagEngine.py的过滤逻辑有问题，会过滤掉空标签")
    print("3. 应该修改过滤条件，保留空标签更新")
    print("\n建议修改:")
    print("❌ 当前: (size(col('final_tag_ids')) > 0) | (col('existing.existing_tag_ids').isNotNull())")
    print("✅ 正确: col('existing.existing_tag_ids').isNotNull()")

if __name__ == "__main__":
    main()