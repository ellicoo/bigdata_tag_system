#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能标签更新机制测试脚本

测试场景：
1. 标签新增 - 新用户匹配标签
2. 标签移除 - 用户不再匹配历史标签  
3. 标签更新 - 部分标签变化
4. 标签保持 - 持续匹配的标签
5. 混合场景 - 同时包含新增、移除、保持
6. FULL JOIN验证 - 确保所有用户被正确处理
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 导入标签处理函数
from src.tag_engine.utils.SparkUdfs import replace_computed_tags, merge_with_existing_tags, array_to_json, json_to_array

def create_test_spark():
    """创建测试用Spark会话"""
    return SparkSession.builder \
        .appName("SmartTagUpdateTest") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

def test_scenario_1_tag_addition():
    """场景1：标签新增 - 新用户匹配标签"""
    print("\n" + "="*60)
    print("🧪 场景1：标签新增测试")
    print("="*60)
    
    spark = create_test_spark()
    
    # 测试数据：新用户
    new_users_data = [
        ("user_D", [2]),      # 新用户，匹配标签2
        ("user_E", [3, 4]),   # 新用户，匹配标签3,4
    ]
    
    new_users_df = spark.createDataFrame(new_users_data, ["user_id", "tag_ids_array"])
    
    # 模拟现有标签（新用户在MySQL中不存在）
    existing_users_data = []  # 空的现有用户数据
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("existing_tag_ids", ArrayType(IntegerType()), True)
    ])
    existing_users_df = spark.createDataFrame(existing_users_data, schema)
    
    # FULL JOIN
    joined_df = new_users_df.alias("new").join(
        existing_users_df.alias("existing"),
        col("new.user_id") == col("existing.user_id"),
        "full"
    )
    
    # 智能标签替换（本次计算标签2,3,4）
    computed_tag_scope = [2, 3, 4]
    result_df = joined_df.withColumn(
        "final_tag_ids",
        replace_computed_tags(
            col("new.tag_ids_array"),
            col("existing.existing_tag_ids"),
            lit(computed_tag_scope)
        )
    ).withColumn(
        "final_tag_ids_json",
        array_to_json(col("final_tag_ids"))
    ).select(
        coalesce(col("new.user_id"), col("existing.user_id")).alias("user_id"),
        col("final_tag_ids"),
        col("final_tag_ids_json")
    )
    
    print("📊 新用户标签新增结果：")
    result_df.show(truncate=False)
    
    # 验证结果
    results = result_df.collect()
    assert len(results) == 2
    assert set([row.user_id for row in results]) == {"user_D", "user_E"}
    
    for row in results:
        if row.user_id == "user_D":
            assert row.final_tag_ids == [2], f"user_D应该有标签[2]，实际：{row.final_tag_ids}"
        elif row.user_id == "user_E":
            assert row.final_tag_ids == [3, 4], f"user_E应该有标签[3,4]，实际：{row.final_tag_ids}"
    
    print("✅ 标签新增测试通过")
    return True

def test_scenario_2_tag_removal():
    """场景2：标签移除 - 用户不再匹配历史标签"""
    print("\n" + "="*60)
    print("🧪 场景2：标签移除测试")
    print("="*60)
    
    spark = create_test_spark()
    
    # 测试数据：本次计算无匹配的用户 - 明确指定schema
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("tag_ids_array", ArrayType(IntegerType()), True)
    ])
    
    computed_users_data = [
        ("user_B", []),       # 用户B本次无匹配标签
        ("user_C", []),       # 用户C本次无匹配标签
    ]
    
    computed_users_df = spark.createDataFrame(computed_users_data, schema)
    
    # 现有标签
    existing_users_data = [
        ("user_B", [1, 4, 6]),    # 用户B有历史标签，其中4在本次计算范围内
        ("user_C", [1, 2, 3, 5]), # 用户C有历史标签，其中2,3在本次计算范围内
    ]
    
    existing_users_df = spark.createDataFrame(existing_users_data, ["user_id", "existing_tag_ids"])
    
    # FULL JOIN
    joined_df = computed_users_df.alias("new").join(
        existing_users_df.alias("existing"),
        col("new.user_id") == col("existing.user_id"),
        "full"
    )
    
    # 智能标签替换（本次计算标签2,3,4）
    computed_tag_scope = [2, 3, 4]
    result_df = joined_df.withColumn(
        "final_tag_ids",
        replace_computed_tags(
            col("new.tag_ids_array"),
            col("existing.existing_tag_ids"),
            lit(computed_tag_scope)
        )
    ).select(
        coalesce(col("new.user_id"), col("existing.user_id")).alias("user_id"),
        col("existing.existing_tag_ids").alias("original_tags"),
        col("new.tag_ids_array").alias("computed_tags"),
        col("final_tag_ids")
    )
    
    print("📊 标签移除结果：")
    result_df.show(truncate=False)
    
    # 验证结果
    results = result_df.collect()
    for row in results:
        if row.user_id == "user_B":
            # 原标签[1,4,6]，本次计算[]，范围[2,3,4] -> 移除4，保留[1,6]
            expected = [1, 6]
            assert row.final_tag_ids == expected, f"user_B应该有标签{expected}，实际：{row.final_tag_ids}"
        elif row.user_id == "user_C":
            # 原标签[1,2,3,5]，本次计算[]，范围[2,3,4] -> 移除2,3，保留[1,5]
            expected = [1, 5]
            assert row.final_tag_ids == expected, f"user_C应该有标签{expected}，实际：{row.final_tag_ids}"
    
    print("✅ 标签移除测试通过")
    return True

def test_scenario_3_tag_update():
    """场景3：标签更新 - 部分标签变化"""
    print("\n" + "="*60)
    print("🧪 场景3：标签更新测试")
    print("="*60)
    
    spark = create_test_spark()
    
    # 测试数据：用户标签部分变化
    computed_users_data = [
        ("user_A", [3, 4]),   # 用户A：之前有[2,3]，现在匹配[3,4]
    ]
    
    computed_users_df = spark.createDataFrame(computed_users_data, ["user_id", "tag_ids_array"])
    
    # 现有标签
    existing_users_data = [
        ("user_A", [1, 2, 3, 5]), # 用户A的历史标签
    ]
    
    existing_users_df = spark.createDataFrame(existing_users_data, ["user_id", "existing_tag_ids"])
    
    # FULL JOIN
    joined_df = computed_users_df.alias("new").join(
        existing_users_df.alias("existing"),
        col("new.user_id") == col("existing.user_id"),
        "full"
    )
    
    # 智能标签替换（本次计算标签2,3,4）
    computed_tag_scope = [2, 3, 4]
    result_df = joined_df.withColumn(
        "final_tag_ids",
        replace_computed_tags(
            col("new.tag_ids_array"),
            col("existing.existing_tag_ids"),
            lit(computed_tag_scope)
        )
    ).select(
        col("new.user_id").alias("user_id"),
        col("existing.existing_tag_ids").alias("original_tags"),
        col("new.tag_ids_array").alias("computed_tags"),
        col("final_tag_ids")
    )
    
    print("📊 标签更新结果：")
    result_df.show(truncate=False)
    
    # 验证结果
    results = result_df.collect()
    for row in results:
        if row.user_id == "user_A":
            # 原标签[1,2,3,5]，本次计算[3,4]，范围[2,3,4]
            # -> 移除2，保留3，新增4，最终[1,3,4,5]
            expected = [1, 3, 4, 5]
            assert row.final_tag_ids == expected, f"user_A应该有标签{expected}，实际：{row.final_tag_ids}"
    
    print("✅ 标签更新测试通过")
    return True

def test_scenario_4_tag_preservation():
    """场景4：标签保持 - 不在计算范围内的标签应该保持不变"""
    print("\n" + "="*60)
    print("🧪 场景4：标签保持测试")
    print("="*60)
    
    spark = create_test_spark()
    
    # 测试数据：用户有不在计算范围内的标签
    computed_users_data = [
        ("user_X", [2, 3]),   # 用户X本次匹配标签2,3
    ]
    
    computed_users_df = spark.createDataFrame(computed_users_data, ["user_id", "tag_ids_array"])
    
    # 现有标签：包含不在本次计算范围内的标签
    existing_users_data = [
        ("user_X", [1, 7, 8, 9]), # 标签1,7,8,9不在本次计算范围[2,3,4]内
    ]
    
    existing_users_df = spark.createDataFrame(existing_users_data, ["user_id", "existing_tag_ids"])
    
    # FULL JOIN
    joined_df = computed_users_df.alias("new").join(
        existing_users_df.alias("existing"),
        col("new.user_id") == col("existing.user_id"),
        "full"
    )
    
    # 智能标签替换（本次计算标签2,3,4）
    computed_tag_scope = [2, 3, 4]
    result_df = joined_df.withColumn(
        "final_tag_ids",
        replace_computed_tags(
            col("new.tag_ids_array"),
            col("existing.existing_tag_ids"),
            lit(computed_tag_scope)
        )
    ).select(
        col("new.user_id").alias("user_id"),
        col("existing.existing_tag_ids").alias("original_tags"),
        col("new.tag_ids_array").alias("computed_tags"),
        col("final_tag_ids")
    )
    
    print("📊 标签保持结果：")
    result_df.show(truncate=False)
    
    # 验证结果
    results = result_df.collect()
    for row in results:
        if row.user_id == "user_X":
            # 原标签[1,7,8,9]，本次计算[2,3]，范围[2,3,4]
            # -> 保留[1,7,8,9]，新增[2,3]，最终[1,2,3,7,8,9]
            expected = [1, 2, 3, 7, 8, 9]
            assert row.final_tag_ids == expected, f"user_X应该有标签{expected}，实际：{row.final_tag_ids}"
    
    print("✅ 标签保持测试通过")
    return True

def test_scenario_5_comprehensive():
    """场景5：综合场景 - 包含所有类型的用户和标签变化"""
    print("\n" + "="*60)
    print("🧪 场景5：综合场景测试")
    print("="*60)
    
    spark = create_test_spark()
    
    # 本次计算结果：标签范围[2,3,4] - 指定schema以处理空数组
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("tag_ids_array", ArrayType(IntegerType()), True)
    ])
    
    computed_users_data = [
        ("user_A", [3, 4]),   # 更新：原有[2,3] -> 现在[3,4]
        ("user_B", []),       # 移除：原有[4] -> 现在[]
        ("user_C", [2]),      # 新增+保持：原有[1,7] -> 现在[1,2,7]
        ("user_D", [2, 3, 4]),# 新用户：直接添加[2,3,4]
        ("user_E", []),       # 新用户无标签，应该被过滤
    ]
    
    computed_users_df = spark.createDataFrame(computed_users_data, schema)
    
    # MySQL现有标签
    existing_users_data = [
        ("user_A", [1, 2, 3, 5]), # 有范围内标签2,3
        ("user_B", [1, 4, 6]),     # 有范围内标签4
        ("user_C", [1, 7]),        # 无范围内标签
        # user_D 和 user_E 是新用户，MySQL中不存在
    ]
    
    existing_users_df = spark.createDataFrame(existing_users_data, ["user_id", "existing_tag_ids"])
    
    # FULL JOIN
    joined_df = computed_users_df.alias("new").join(
        existing_users_df.alias("existing"),
        col("new.user_id") == col("existing.user_id"),
        "full"
    )
    
    print("📊 FULL JOIN结果：")
    joined_df.select(
        coalesce(col("new.user_id"), col("existing.user_id")).alias("user_id"),
        col("new.tag_ids_array").alias("computed_tags"),
        col("existing.existing_tag_ids").alias("existing_tags")
    ).show(truncate=False)
    
    # 智能标签替换（本次计算标签2,3,4）
    computed_tag_scope = [2, 3, 4]
    result_df = joined_df.withColumn(
        "final_tag_ids",
        replace_computed_tags(
            col("new.tag_ids_array"),
            col("existing.existing_tag_ids"),
            lit(computed_tag_scope)
        )
    ).filter(
        # 智能过滤：有最终标签 或 有历史标签
        (size(col("final_tag_ids")) > 0) |
        (col("existing.existing_tag_ids").isNotNull())
    ).select(
        coalesce(col("new.user_id"), col("existing.user_id")).alias("user_id"),
        col("existing.existing_tag_ids").alias("original_tags"),
        col("new.tag_ids_array").alias("computed_tags"),
        col("final_tag_ids"),
        array_to_json(col("final_tag_ids")).alias("final_tag_ids_json")
    )
    
    print("📊 综合场景最终结果：")
    result_df.show(truncate=False)
    
    # 验证结果
    results = {row.user_id: row for row in result_df.collect()}
    
    # user_A: [1,2,3,5] + [3,4] scope[2,3,4] = [1,5] + [3,4] = [1,3,4,5]
    assert results["user_A"].final_tag_ids == [1, 3, 4, 5], f"user_A错误: {results['user_A'].final_tag_ids}"
    
    # user_B: [1,4,6] + [] scope[2,3,4] = [1,6] + [] = [1,6]
    assert results["user_B"].final_tag_ids == [1, 6], f"user_B错误: {results['user_B'].final_tag_ids}"
    
    # user_C: [1,7] + [2] scope[2,3,4] = [1,7] + [2] = [1,2,7]
    assert results["user_C"].final_tag_ids == [1, 2, 7], f"user_C错误: {results['user_C'].final_tag_ids}"
    
    # user_D: [] + [2,3,4] scope[2,3,4] = [] + [2,3,4] = [2,3,4]
    assert results["user_D"].final_tag_ids == [2, 3, 4], f"user_D错误: {results['user_D'].final_tag_ids}"
    
    # user_E 应该被过滤掉（新用户无标签）
    assert "user_E" not in results, "user_E应该被过滤掉"
    
    print("✅ 综合场景测试通过")
    return True

def test_scenario_6_backward_compatibility():
    """场景6：向后兼容性测试 - 验证传统合并模式"""
    print("\n" + "="*60)
    print("🧪 场景6：向后兼容性测试")
    print("="*60)
    
    spark = create_test_spark()
    
    # 测试数据
    computed_users_data = [
        ("user_A", [3, 4]),
        ("user_B", [5, 6]),
    ]
    
    computed_users_df = spark.createDataFrame(computed_users_data, ["user_id", "tag_ids_array"])
    
    existing_users_data = [
        ("user_A", [1, 2]),
        ("user_B", [5, 7]),
    ]
    
    existing_users_df = spark.createDataFrame(existing_users_data, ["user_id", "existing_tag_ids"])
    
    # 使用传统合并模式
    joined_df = computed_users_df.join(existing_users_df, "user_id", "left")
    
    traditional_result_df = joined_df.withColumn(
        "final_tag_ids",
        merge_with_existing_tags(
            col("tag_ids_array"),
            col("existing_tag_ids")
        )
    ).select("user_id", "final_tag_ids")
    
    print("📊 传统合并模式结果（只增不减）：")
    traditional_result_df.show()
    
    # 验证传统模式结果
    traditional_results = {row.user_id: row.final_tag_ids for row in traditional_result_df.collect()}
    
    # user_A: [3,4] + [1,2] = [1,2,3,4]
    assert traditional_results["user_A"] == [1, 2, 3, 4], f"传统模式user_A错误: {traditional_results['user_A']}"
    
    # user_B: [5,6] + [5,7] = [5,6,7]
    assert traditional_results["user_B"] == [5, 6, 7], f"传统模式user_B错误: {traditional_results['user_B']}"
    
    print("✅ 向后兼容性测试通过")
    return True

def test_scenario_7_edge_cases():
    """场景7：边界情况测试"""
    print("\n" + "="*60)
    print("🧪 场景7：边界情况测试")
    print("="*60)
    
    spark = create_test_spark()
    
    # 边界情况测试数据
    computed_users_data = [
        ("user_null", None),      # 计算结果为None
        ("user_empty", []),       # 计算结果为空数组
    ]
    
    # 处理None值
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("tag_ids_array", ArrayType(IntegerType()), True)
    ])
    computed_users_df = spark.createDataFrame(computed_users_data, schema)
    
    existing_users_data = [
        ("user_null", [1, 2, 3]),   # 有历史标签
        ("user_empty", [4, 5]),     # 有历史标签
        ("user_missing", [6, 7]),   # 本次未计算但有历史标签
    ]
    
    existing_users_df = spark.createDataFrame(existing_users_data, ["user_id", "existing_tag_ids"])
    
    # FULL JOIN
    joined_df = computed_users_df.alias("new").join(
        existing_users_df.alias("existing"),
        col("new.user_id") == col("existing.user_id"),
        "full"
    )
    
    # 智能标签替换（与其他测试场景保持一致）
    computed_tag_scope = [2, 3, 4]
    result_df = joined_df.withColumn(
        "final_tag_ids",
        replace_computed_tags(
            col("new.tag_ids_array"),
            col("existing.existing_tag_ids"),
            lit(computed_tag_scope)
        )
    ).filter(
        (size(col("final_tag_ids")) > 0) |
        (col("existing.existing_tag_ids").isNotNull())
    ).select(
        coalesce(col("new.user_id"), col("existing.user_id")).alias("user_id"),
        col("new.tag_ids_array").alias("computed_tags"),
        col("existing.existing_tag_ids").alias("existing_tags"),
        col("final_tag_ids")
    )
    
    print("📊 边界情况测试结果：")
    result_df.show(truncate=False)
    
    results = {row.user_id: row for row in result_df.collect()}
    
    # user_null: null计算结果，相当于[]
    # 原标签[1,2,3]，计算[]，范围[2,3,4] -> 移除2,3，保留[1]
    assert results["user_null"].final_tag_ids == [1], f"user_null错误: {results['user_null'].final_tag_ids}"
    
    # user_empty: []计算结果
    # 原标签[4,5]，计算[]，范围[2,3,4] -> 移除4，保留[5]
    assert results["user_empty"].final_tag_ids == [5], f"user_empty错误: {results['user_empty'].final_tag_ids}"
    
    # user_missing: 未参与计算
    # 原标签[6,7]，计算null，范围[2,3,4] -> [6,7]都不在范围内，保持不变
    assert results["user_missing"].final_tag_ids == [6, 7], f"user_missing错误: {results['user_missing'].final_tag_ids}"
    
    print("✅ 边界情况测试通过")
    return True

def main():
    """运行所有测试场景"""
    print("🚀 开始智能标签更新机制完整测试")
    print("="*80)
    
    test_results = []
    
    try:
        # 运行所有测试场景
        test_results.append(("标签新增", test_scenario_1_tag_addition()))
        test_results.append(("标签移除", test_scenario_2_tag_removal()))
        test_results.append(("标签更新", test_scenario_3_tag_update()))
        test_results.append(("标签保持", test_scenario_4_tag_preservation()))
        test_results.append(("综合场景", test_scenario_5_comprehensive()))
        test_results.append(("向后兼容", test_scenario_6_backward_compatibility()))
        test_results.append(("边界情况", test_scenario_7_edge_cases()))
        
    except Exception as e:
        print(f"\n❌ 测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # 汇总结果
    print("\n" + "="*80)
    print("📋 测试结果汇总")
    print("="*80)
    
    all_passed = True
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name:12} : {status}")
        all_passed = all_passed and result
    
    print("="*80)
    if all_passed:
        print("🎉 所有测试通过！智能标签更新机制工作正常！")
        print("\n✨ 验证的功能：")
        print("  - ✅ 标签新增：新用户匹配标签时自动添加")
        print("  - ✅ 标签移除：用户不再匹配标签时自动移除")
        print("  - ✅ 标签更新：部分标签变化时智能更新")
        print("  - ✅ 标签保持：不在计算范围内的标签保持不变")
        print("  - ✅ FULL JOIN：确保所有相关用户都被正确处理")
        print("  - ✅ 智能过滤：只更新需要更新的用户")
        print("  - ✅ 向后兼容：传统合并模式依然正常工作")
        print("  - ✅ 边界情况：正确处理null值和空数组")
    else:
        print("❌ 部分测试失败，请检查实现逻辑")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)