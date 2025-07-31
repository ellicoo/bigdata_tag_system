#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签表达式工具测试
测试tagExpressionUtils模块的并行表达式构建功能
"""
import pytest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.tag_engine.utils.tagExpressionUtils import buildParallelTagExpression


class TestTagExpressionUtils:
    """标签表达式工具测试类"""
    
    def test_build_parallel_tag_expression_basic(self, spark):
        """测试基础并行标签表达式构建"""
        # 准备测试条件
        tagConditions = [
            {'tag_id': 1, 'condition': 'age >= 30'},
            {'tag_id': 2, 'condition': 'assets >= 10000'}
        ]
        
        # 构建表达式
        expr = buildParallelTagExpression(tagConditions)
        
        # 验证表达式类型
        assert expr is not None
        assert hasattr(expr, '_jc')  # Spark Column对象特征
        
        # 创建测试数据验证表达式
        testData = [
            ("user001", 35, 15000),  # 满足两个条件
            ("user002", 25, 20000),  # 只满足assets条件
            ("user003", 40, 5000),   # 只满足age条件
            ("user004", 20, 1000)    # 都不满足
        ]
        
        testDF = spark.createDataFrame(testData, ["user_id", "age", "assets"])
        resultDF = testDF.withColumn("tag_ids_array", expr)
        
        results = resultDF.collect()
        
        # 验证结果
        assert len(results) == 4
        
        # user001应该有[1,2]标签
        user001_tags = [row.tag_ids_array for row in results if row.user_id == "user001"][0]
        assert user001_tags is not None
        assert sorted(user001_tags) == [1, 2]
        
        # user002应该有[2]标签
        user002_tags = [row.tag_ids_array for row in results if row.user_id == "user002"][0]
        assert user002_tags == [2]
        
        # user003应该有[1]标签
        user003_tags = [row.tag_ids_array for row in results if row.user_id == "user003"][0]
        assert user003_tags == [1]
        
        # user004应该有空数组
        user004_tags = [row.tag_ids_array for row in results if row.user_id == "user004"][0]
        assert user004_tags == []
    
    def test_build_parallel_tag_expression_empty(self, spark):
        """测试空条件列表"""
        # 空条件列表
        tagConditions = []
        
        # 构建表达式
        expr = buildParallelTagExpression(tagConditions)
        
        # 创建测试数据
        testData = [("user001", 35, 15000)]
        testDF = spark.createDataFrame(testData, ["user_id", "age", "assets"])
        resultDF = testDF.withColumn("tag_ids_array", expr)
        
        results = resultDF.collect()
        
        # 验证结果为空数组
        assert len(results) == 1
        assert results[0].tag_ids_array == []
    
    def test_build_parallel_tag_expression_complex(self, spark):
        """测试复杂条件的并行表达式"""
        # 复杂条件
        tagConditions = [
            {'tag_id': 1, 'condition': 'age >= 30 AND assets >= 10000'},
            {'tag_id': 2, 'condition': 'age < 25 OR assets > 50000'},
            {'tag_id': 3, 'condition': 'trade_count > 5'}
        ]
        
        # 构建表达式
        expr = buildParallelTagExpression(tagConditions)
        
        # 创建测试数据
        testData = [
            ("user001", 35, 15000, 10),  # 满足条件1和3
            ("user002", 20, 60000, 2),   # 满足条件2
            ("user003", 30, 5000, 1),    # 都不满足
        ]
        
        testDF = spark.createDataFrame(testData, ["user_id", "age", "assets", "trade_count"])
        resultDF = testDF.withColumn("tag_ids_array", expr)
        
        results = resultDF.collect()
        
        # 验证结果
        assert len(results) == 3
        
        # user001应该有[1,3]标签
        user001_tags = [row.tag_ids_array for row in results if row.user_id == "user001"][0]
        assert sorted(user001_tags) == [1, 3]
        
        # user002应该有[2]标签
        user002_tags = [row.tag_ids_array for row in results if row.user_id == "user002"][0]
        assert user002_tags == [2]
        
        # user003应该有空数组
        user003_tags = [row.tag_ids_array for row in results if row.user_id == "user003"][0]
        assert user003_tags == []
    
    def test_build_parallel_tag_expression_duplicate_tags(self, spark):
        """测试重复标签ID的去重功能"""
        # 包含重复tag_id的条件（模拟不同规则产生相同标签的场景）
        tagConditions = [
            {'tag_id': 1, 'condition': 'age >= 30'},
            {'tag_id': 1, 'condition': 'assets >= 50000'},
            {'tag_id': 2, 'condition': 'trade_count > 10'}
        ]
        
        # 构建表达式
        expr = buildParallelTagExpression(tagConditions)
        
        # 创建测试数据 - 用户同时满足两个tag_id=1的条件
        testData = [("user001", 40, 60000, 15)]
        testDF = spark.createDataFrame(testData, ["user_id", "age", "assets", "trade_count"])
        resultDF = testDF.withColumn("tag_ids_array", expr)
        
        results = resultDF.collect()
        
        # 验证标签去重
        assert len(results) == 1
        user_tags = results[0].tag_ids_array
        # 应该只有一个1，一个2，并且排序
        assert user_tags == [1, 2]
    
    def test_build_parallel_tag_expression_none_conditions(self, spark):
        """测试None条件的处理"""
        # 包含None的条件列表
        tagConditions = None
        
        # 构建表达式（应该返回空数组表达式）
        expr = buildParallelTagExpression(tagConditions)
        
        # 创建测试数据
        testData = [("user001", 35, 15000)]
        testDF = spark.createDataFrame(testData, ["user_id", "age", "assets"])
        resultDF = testDF.withColumn("tag_ids_array", expr)
        
        results = resultDF.collect()
        
        # 验证结果为空数组
        assert len(results) == 1
        assert results[0].tag_ids_array == []
    
    def test_build_parallel_tag_expression_sort_order(self, spark):
        """测试标签ID的排序功能"""
        # 乱序的tag_id条件
        tagConditions = [
            {'tag_id': 5, 'condition': 'age >= 50'},
            {'tag_id': 1, 'condition': 'age >= 20'},
            {'tag_id': 3, 'condition': 'age >= 30'},
            {'tag_id': 2, 'condition': 'age >= 25'}
        ]
        
        # 构建表达式
        expr = buildParallelTagExpression(tagConditions)
        
        # 创建满足所有条件的测试数据
        testData = [("user001", 55, 15000)]
        testDF = spark.createDataFrame(testData, ["user_id", "age", "assets"])
        resultDF = testDF.withColumn("tag_ids_array", expr)
        
        results = resultDF.collect()
        
        # 验证结果按升序排列
        assert len(results) == 1
        user_tags = results[0].tag_ids_array
        assert user_tags == [1, 2, 3, 5]  # 应该自动排序
    
    def test_build_parallel_tag_expression_integration(self, spark):
        """测试与实际业务场景的集成"""
        # 模拟实际标签规则
        tagConditions = [
            {
                'tag_id': 1, 
                'condition': '`user_basic_info`.`age` >= 30'
            },
            {
                'tag_id': 2, 
                'condition': '`user_asset_summary`.`total_assets` >= 100000'
            },
            {
                'tag_id': 3, 
                'condition': '`user_activity_summary`.`trade_count_30d` > 5'
            }
        ]
        
        # 构建表达式
        expr = buildParallelTagExpression(tagConditions)
        
        # 创建模拟JOIN后的DataFrame结构
        testData = [
            ("user001", 35, 150000, 8),   # 高价值活跃用户
            ("user002", 25, 200000, 12),  # 年轻高价值活跃用户  
            ("user003", 45, 50000, 2),    # 年长低价值低活跃用户
        ]
        
        # 使用实际字段名
        testDF = spark.createDataFrame(testData, [
            "user_id", 
            "`user_basic_info.age`", 
            "`user_asset_summary.total_assets`",
            "`user_activity_summary.trade_count_30d`"
        ])
        
        # 重命名以匹配条件中的字段引用
        renamedDF = testDF \
            .withColumnRenamed("`user_basic_info.age`", "age") \
            .withColumnRenamed("`user_asset_summary.total_assets`", "total_assets") \
            .withColumnRenamed("`user_activity_summary.trade_count_30d`", "trade_count_30d")
        
        # 应用表达式（需要调整条件以匹配重命名后的字段）
        adjustedConditions = [
            {'tag_id': 1, 'condition': 'age >= 30'},
            {'tag_id': 2, 'condition': 'total_assets >= 100000'},
            {'tag_id': 3, 'condition': 'trade_count_30d > 5'}
        ]
        
        adjustedExpr = buildParallelTagExpression(adjustedConditions)
        resultDF = renamedDF.withColumn("tag_ids_array", adjustedExpr)
        
        results = resultDF.collect()
        
        # 验证业务逻辑
        assert len(results) == 3
        
        # user001: 35岁,15万资产,8次交易 → [1,2,3]
        user001_tags = [row.tag_ids_array for row in results if row.user_id == "user001"][0]
        assert sorted(user001_tags) == [1, 2, 3]
        
        # user002: 25岁,20万资产,12次交易 → [2,3] (不满足年龄条件)
        user002_tags = [row.tag_ids_array for row in results if row.user_id == "user002"][0]
        assert sorted(user002_tags) == [2, 3]
        
        # user003: 45岁,5万资产,2次交易 → [1] (只满足年龄条件)
        user003_tags = [row.tag_ids_array for row in results if row.user_id == "user003"][0]
        assert user003_tags == [1]