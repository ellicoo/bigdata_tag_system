#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SparkUdfs集成测试
测试SparkUdfs模块与TagEngine的集成使用
"""
import pytest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.tag_engine.utils.SparkUdfs import (
    merge_user_tags, 
    merge_with_existing_tags, 
    array_to_json, 
    json_to_array
)


class TestSparkUdfsIntegration:
    """SparkUdfs集成测试类"""
    
    def test_merge_user_tags_basic(self, spark):
        """测试merge_user_tags基础功能"""
        # 创建测试数据
        testData = [
            ("user001", [3, 1, 2, 1]),  # 有重复
            ("user002", [2, 4, 3]),     # 无重复
            ("user003", [1]),           # 单个元素
            ("user004", [])             # 空数组
        ]
        
        testDF = spark.createDataFrame(testData, ["user_id", "tags"])
        
        # 应用merge_user_tags函数
        resultDF = testDF.withColumn("merged_tags", merge_user_tags(col("tags")))
        
        results = resultDF.collect()
        
        # 验证结果
        assert len(results) == 4
        
        # 验证去重和排序
        user001_tags = [row.merged_tags for row in results if row.user_id == "user001"][0]
        assert user001_tags == [1, 2, 3]  # 去重并排序
        
        user002_tags = [row.merged_tags for row in results if row.user_id == "user002"][0]
        assert user002_tags == [2, 3, 4]  # 排序
        
        user003_tags = [row.merged_tags for row in results if row.user_id == "user003"][0]
        assert user003_tags == [1]
        
        user004_tags = [row.merged_tags for row in results if row.user_id == "user004"][0]
        assert user004_tags == []
    
    def test_merge_with_existing_tags_basic(self, spark):
        """测试merge_with_existing_tags基础功能"""
        # 创建测试数据：新标签和现有标签
        testData = [
            ("user001", [1, 2, 3], [2, 3, 4]),    # 有重叠
            ("user002", [5, 6], [1, 2]),          # 无重叠
            ("user003", [1, 2], None),            # 现有标签为空
            ("user004", None, [3, 4]),            # 新标签为空
            ("user005", None, None)               # 都为空
        ]
        
        testDF = spark.createDataFrame(testData, [
            "user_id", "new_tags", "existing_tags"
        ])
        
        # 应用merge_with_existing_tags函数
        resultDF = testDF.withColumn(
            "final_tags", 
            merge_with_existing_tags(col("new_tags"), col("existing_tags"))
        )
        
        results = resultDF.collect()
        
        # 验证结果
        assert len(results) == 5
        
        # user001: [1,2,3] + [2,3,4] = [1,2,3,4]
        user001_tags = [row.final_tags for row in results if row.user_id == "user001"][0]
        assert user001_tags == [1, 2, 3, 4]
        
        # user002: [5,6] + [1,2] = [1,2,5,6]
        user002_tags = [row.final_tags for row in results if row.user_id == "user002"][0]
        assert user002_tags == [1, 2, 5, 6]
        
        # user003: [1,2] + null = [1,2]
        user003_tags = [row.final_tags for row in results if row.user_id == "user003"][0]
        assert user003_tags == [1, 2]
        
        # user004: null + [3,4] = [3,4]
        user004_tags = [row.final_tags for row in results if row.user_id == "user004"][0]
        assert user004_tags == [3, 4]
        
        # user005: null + null = []
        user005_tags = [row.final_tags for row in results if row.user_id == "user005"][0]
        assert user005_tags == []
    
    def test_array_to_json_basic(self, spark):
        """测试array_to_json基础功能"""
        # 创建测试数据
        testData = [
            ("user001", [1, 2, 3]),
            ("user002", [5]),
            ("user003", []),
            ("user004", None)
        ]
        
        testDF = spark.createDataFrame(testData, ["user_id", "tags"])
        
        # 应用array_to_json函数
        resultDF = testDF.withColumn("tags_json", array_to_json(col("tags")))
        
        results = resultDF.collect()
        
        # 验证结果
        assert len(results) == 4
        
        user001_json = [row.tags_json for row in results if row.user_id == "user001"][0]
        assert user001_json == "[1,2,3]"
        
        user002_json = [row.tags_json for row in results if row.user_id == "user002"][0]
        assert user002_json == "[5]"
        
        user003_json = [row.tags_json for row in results if row.user_id == "user003"][0]
        assert user003_json == "[]"
        
        user004_json = [row.tags_json for row in results if row.user_id == "user004"][0]
        assert user004_json == "[]"  # null转换为空数组
    
    def test_json_to_array_basic(self, spark):
        """测试json_to_array基础功能"""
        # 创建测试数据
        testData = [
            ("user001", "[1,2,3]"),
            ("user002", "[5]"),
            ("user003", "[]"),
            ("user004", None)
        ]
        
        testDF = spark.createDataFrame(testData, ["user_id", "tags_json"])
        
        # 应用json_to_array函数
        resultDF = testDF.withColumn("tags", json_to_array(col("tags_json")))
        
        results = resultDF.collect()
        
        # 验证结果
        assert len(results) == 4
        
        user001_tags = [row.tags for row in results if row.user_id == "user001"][0]
        assert user001_tags == [1, 2, 3]
        
        user002_tags = [row.tags for row in results if row.user_id == "user002"][0]
        assert user002_tags == [5]
        
        user003_tags = [row.tags for row in results if row.user_id == "user003"][0]
        assert user003_tags == []
        
        user004_tags = [row.tags for row in results if row.user_id == "user004"][0]
        assert user004_tags == []  # null转换为空数组
    
    def test_json_array_roundtrip(self, spark):
        """测试JSON和数组的往返转换"""
        # 创建测试数据
        testData = [
            ("user001", [3, 1, 4, 1, 5]),
            ("user002", [2, 7, 1, 8]),
            ("user003", []),
            ("user004", None)
        ]
        
        testDF = spark.createDataFrame(testData, ["user_id", "original_tags"])
        
        # 往返转换：数组 → JSON → 数组
        resultDF = testDF \
            .withColumn("json_tags", array_to_json(col("original_tags"))) \
            .withColumn("final_tags", json_to_array(col("json_tags")))
        
        results = resultDF.collect()
        
        # 验证往返转换后的结果
        for row in results:
            original = row.original_tags if row.original_tags else []
            final = row.final_tags if row.final_tags else []
            
            # 排序后应该相等（因为可能包含重复元素）
            assert sorted(original) == sorted(final)
    
    def test_tag_engine_integration_simulation(self, spark):
        """模拟TagEngine中的实际使用场景"""
        # 模拟TagEngine._mergeAndSaveGroup中的使用
        
        # 1. 模拟新计算的标签结果
        newTagsData = [
            ("user001", [1, 3, 5]),
            ("user002", [2, 4]),
            ("user003", [1, 2, 6])
        ]
        newTagsDF = spark.createDataFrame(newTagsData, ["user_id", "tag_ids_array"])
        
        # 2. 模拟MySQL中现有的标签
        existingTagsData = [
            ("user001", [2, 4, 6]),  # 与新标签有重叠
            ("user002", None),       # 新用户，无现有标签
            ("user004", [7, 8, 9])   # 老用户，本次无新标签
        ]
        existingTagsDF = spark.createDataFrame(existingTagsData, ["user_id", "existing_tag_ids"])
        
        # 3. 模拟LEFT JOIN合并
        joinedDF = newTagsDF.alias("new").join(
            existingTagsDF.alias("existing"),
            col("new.user_id") == col("existing.user_id"),
            "left"
        )
        
        # 4. 使用SparkUdfs合并标签
        finalDF = joinedDF.withColumn(
            "final_tag_ids",
            merge_with_existing_tags(
                col("new.tag_ids_array"),
                col("existing.existing_tag_ids")
            )
        ).withColumn(
            "final_tag_ids_json",
            array_to_json(col("final_tag_ids"))
        ).select(
            col("new.user_id").alias("user_id"),
            col("final_tag_ids"),
            col("final_tag_ids_json")
        )
        
        results = finalDF.collect()
        
        # 验证集成结果
        assert len(results) == 3
        
        # user001: 新标签[1,3,5] + 现有标签[2,4,6] = [1,2,3,4,5,6]
        user001 = [row for row in results if row.user_id == "user001"][0]
        assert user001.final_tag_ids == [1, 2, 3, 4, 5, 6]
        assert user001.final_tag_ids_json == "[1,2,3,4,5,6]"
        
        # user002: 新标签[2,4] + 无现有标签 = [2,4]
        user002 = [row for row in results if row.user_id == "user002"][0]
        assert user002.final_tag_ids == [2, 4]
        assert user002.final_tag_ids_json == "[2,4]"
        
        # user003: 新标签[1,2,6] + 无现有标签 = [1,2,6]
        user003 = [row for row in results if row.user_id == "user003"][0]
        assert user003.final_tag_ids == [1, 2, 6]
        assert user003.final_tag_ids_json == "[1,2,6]"
    
    def test_spark_udfs_type_safety(self, spark):
        """测试SparkUdfs的类型安全性"""
        # 测试各种输入类型的处理
        
        # 创建包含不同数据类型的测试数据
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
        
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("tags", ArrayType(IntegerType()), True)
        ])
        
        # 包含None、空数组、正常数组的测试数据
        testData = [
            ("user001", [1, 2, 3]),
            ("user002", []),
            ("user003", None)
        ]
        
        testDF = spark.createDataFrame(testData, schema)
        
        # 测试所有UDF函数的类型安全性
        resultDF = testDF \
            .withColumn("merged_tags", merge_user_tags(col("tags"))) \
            .withColumn("tags_json", array_to_json(col("tags"))) \
            .withColumn("parsed_tags", json_to_array(col("tags_json"))) \
            .withColumn("final_merge", merge_with_existing_tags(col("merged_tags"), col("parsed_tags")))
        
        # 收集结果（不应该抛出异常）
        results = resultDF.collect()
        
        # 验证类型安全
        assert len(results) == 3
        
        for row in results:
            # 所有数组字段都应该是list类型或None（对于输入为None的情况）
            assert row.merged_tags is None or isinstance(row.merged_tags, list)
            assert isinstance(row.parsed_tags, list)  # json_to_array应该总是返回list
            assert isinstance(row.final_merge, list)  # merge_with_existing_tags应该总是返回list
            # JSON字段应该是字符串类型
            assert isinstance(row.tags_json, str)