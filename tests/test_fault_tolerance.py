#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试标签系统的容错机制
测试表加载失败、字段不存在、标签失败追踪等功能
"""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame
from pyspark.sql.types import *

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.tag_engine.meta.HiveMeta import HiveMeta
from src.tag_engine.engine.TagGroup import TagGroup
from src.tag_engine.engine.TagEngine import TagEngine


class TestHiveMetaFaultTolerance:
    """测试HiveMeta容错机制"""
    
    def test_loadAndJoinTablesWithFailureTracking_success(self, spark, create_test_tables):
        """测试所有表成功加载的情况"""
        hiveMeta = HiveMeta(spark)
        
        tableNames = ["user_basic_info", "user_asset_summary"]
        tagTableMapping = {
            1: ["user_basic_info"],
            2: ["user_asset_summary"]
        }
        
        # 执行容错加载
        resultDF, failed_tag_ids = hiveMeta.loadAndJoinTablesWithFailureTracking(
            tableNames, tagTableMapping
        )
        
        # 验证结果
        assert failed_tag_ids == []  # 没有失败的标签
        assert resultDF.count() > 0  # 有数据返回
        assert hiveMeta.getSuccessfulTables() == tableNames  # 所有表都成功
        assert hiveMeta.getFailureSummary() == "无表加载失败"
    
    def test_loadAndJoinTablesWithFailureTracking_table_not_exist(self, spark, create_test_tables):
        """测试表不存在的情况"""
        hiveMeta = HiveMeta(spark)
        
        tableNames = ["user_basic_info", "non_existent_table"]
        tagTableMapping = {
            1: ["user_basic_info"],
            2: ["non_existent_table"],  # 这个标签依赖不存在的表
            3: ["user_basic_info"]  # 这个标签不受影响
        }
        
        # 执行容错加载
        resultDF, failed_tag_ids = hiveMeta.loadAndJoinTablesWithFailureTracking(
            tableNames, tagTableMapping
        )
        
        # 验证结果
        assert 2 in failed_tag_ids  # 标签2失败
        assert 1 not in failed_tag_ids  # 标签1成功
        assert 3 not in failed_tag_ids  # 标签3成功
        assert resultDF.count() > 0  # 还是有数据（来自成功的表）
        assert "user_basic_info" in hiveMeta.getSuccessfulTables()
        assert "non_existent_table" not in hiveMeta.getSuccessfulTables()
        assert "non_existent_table" in hiveMeta.getFailureSummary()
    
    def test_loadAndJoinTablesWithFailureTracking_field_not_exist(self, spark, create_test_tables):
        """测试字段不存在的情况"""
        hiveMeta = HiveMeta(spark)
        
        tableNames = ["user_basic_info"]
        tagTableMapping = {
            1: ["user_basic_info"]
        }
        fieldMapping = {
            "user_basic_info": ["user_id", "non_existent_field"]  # 包含不存在的字段
        }
        
        # 执行容错加载
        resultDF, failed_tag_ids = hiveMeta.loadAndJoinTablesWithFailureTracking(
            tableNames, tagTableMapping, fieldMapping
        )
        
        # 验证结果
        assert 1 in failed_tag_ids  # 标签1失败
        assert resultDF.count() == 0  # 没有数据（表加载失败）
        assert hiveMeta.getSuccessfulTables() == []
        assert "字段不存在" in hiveMeta.getFailureSummary()
    
    def test_loadAndJoinTablesWithFailureTracking_all_tables_fail(self, spark):
        """测试所有表都失败的情况"""
        hiveMeta = HiveMeta(spark)
        
        tableNames = ["tag_system.non_existent_table1", "tag_system.non_existent_table2"]
        tagTableMapping = {
            1: ["tag_system.non_existent_table1"],
            2: ["tag_system.non_existent_table2"]
        }
        
        # 执行容错加载
        resultDF, failed_tag_ids = hiveMeta.loadAndJoinTablesWithFailureTracking(
            tableNames, tagTableMapping
        )
        
        # 验证结果
        assert set(failed_tag_ids) == {1, 2}  # 所有标签都失败
        assert resultDF.count() == 0  # 没有数据
        assert hiveMeta.getSuccessfulTables() == []
        assert "失败表(2个)" in hiveMeta.getFailureSummary()
    
    def test_loadAndJoinTablesWithFailureTracking_single_table_success(self, spark, create_test_tables):
        """测试单表成功加载的情况"""
        hiveMeta = HiveMeta(spark)
        
        tableNames = ["user_basic_info"]
        tagTableMapping = {
            1: ["user_basic_info"]
        }
        
        # 执行容错加载
        resultDF, failed_tag_ids = hiveMeta.loadAndJoinTablesWithFailureTracking(
            tableNames, tagTableMapping
        )
        
        # 验证结果
        assert failed_tag_ids == []
        assert resultDF.count() > 0
        # 验证返回的DataFrame有数据和正确的字段
        assert "user_id" in resultDF.columns


class TestTagGroupFaultTolerance:
    """测试TagGroup容错机制"""
    
    def test_computeTags_with_table_failures(self, spark, sample_rules_data):
        """测试TagGroup在表加载失败时的处理"""
        # 创建测试数据
        rules_df = spark.createDataFrame(sample_rules_data[:2], [
            "tag_id", "tag_name", "rule_conditions", "description"
        ])
        
        # 创建TagGroup
        tag_group = TagGroup([1, 2], ["tag_system.user_basic_info", "tag_system.user_asset_summary"])
        
        # Mock HiveMeta返回失败
        mock_hive_meta = Mock()
        mock_hive_meta.loadAndJoinTablesWithFailureTracking.return_value = (
            spark.createDataFrame([], StructType([StructField("user_id", StringType(), True)])),
            [2]  # 标签2失败
        )
        mock_hive_meta.getSuccessfulTables.return_value = ["tag_system.user_basic_info"]
        mock_hive_meta.getFailedTables.return_value = {"tag_system.user_asset_summary": "表不存在"}
        mock_hive_meta.getFailureSummary.return_value = "失败表(1个): user_asset_summary: 表不存在"
        mock_hive_meta.spark = spark
        
        # 执行计算
        result_df, failed_tag_ids = tag_group.computeTags(mock_hive_meta, rules_df)
        
        # 验证结果
        assert 2 in failed_tag_ids  # 标签2失败
        assert 1 not in failed_tag_ids  # 标签1成功（至少尝试计算）
        assert result_df is not None
    
    def test_computeTags_all_tables_fail(self, spark, sample_rules_data):
        """测试所有表都失败的情况"""
        rules_df = spark.createDataFrame(sample_rules_data[:1], [
            "tag_id", "tag_name", "rule_conditions", "description"
        ])
        
        tag_group = TagGroup([1], ["tag_system.user_basic_info"])
        
        # Mock所有表都失败
        mock_hive_meta = Mock()
        mock_hive_meta.loadAndJoinTablesWithFailureTracking.return_value = (
            spark.createDataFrame([], StructType([StructField("user_id", StringType(), True)])),
            [1]  # 所有标签都失败
        )
        mock_hive_meta.getSuccessfulTables.return_value = []
        mock_hive_meta.getFailedTables.return_value = {"tag_system.user_basic_info": "表不存在"}
        mock_hive_meta.getFailureSummary.return_value = "失败表(1个): user_basic_info: 表不存在"
        mock_hive_meta.spark = spark
        
        # 执行计算
        result_df, failed_tag_ids = tag_group.computeTags(mock_hive_meta, rules_df)
        
        # 验证结果
        assert 1 in failed_tag_ids
        assert result_df.count() == 0  # 空结果
    
    def test_computeTags_exception_handling(self, spark, sample_rules_data):
        """测试异常处理"""
        rules_df = spark.createDataFrame(sample_rules_data[:1], [
            "tag_id", "tag_name", "rule_conditions", "description"
        ])
        
        tag_group = TagGroup([1, 2], ["tag_system.user_basic_info"])
        
        # Mock抛出异常
        mock_hive_meta = Mock()
        mock_hive_meta.loadAndJoinTablesWithFailureTracking.side_effect = Exception("模拟异常")
        mock_hive_meta.spark = spark
        
        # 执行计算
        result_df, failed_tag_ids = tag_group.computeTags(mock_hive_meta, rules_df)
        
        # 验证结果
        assert set(failed_tag_ids) == {1, 2}  # 所有标签都失败
        assert result_df.count() == 0


class TestTagEngineFaultTolerance:
    """测试TagEngine容错机制"""
    
    def test_computeTags_return_failed_tag_ids(self):
        """测试TagEngine返回失败标签ID"""
        # Mock Spark和配置
        mock_spark = Mock()
        mock_config = {"host": "localhost", "port": 3306, "database": "test"}
        
        # 创建TagEngine
        with patch('src.tag_engine.engine.TagEngine.HiveMeta'), \
             patch('src.tag_engine.engine.TagEngine.MysqlMeta'), \
             patch('src.tag_engine.engine.TagEngine.TagRuleParser'):
            
            tag_engine = TagEngine(mock_spark, mysqlConfig=mock_config)
            
            # Mock依赖方法
            tag_engine._loadTagRules = Mock(return_value=Mock(count=Mock(return_value=2)))
            tag_engine._analyzeAndGroupTags = Mock(return_value=[Mock()])
            tag_engine._processTagGroupsPipeline = Mock(return_value=(True, [3, 5]))  # 模拟失败标签
            
            # 执行计算
            success, failed_tag_ids = tag_engine.computeTags(mode="task-all")
            
            # 验证结果
            assert success is True
            assert failed_tag_ids == [3, 5]
    
    def test_computeTags_no_failures(self):
        """测试没有失败标签的情况"""
        mock_spark = Mock()
        mock_config = {"host": "localhost", "port": 3306, "database": "test"}
        
        with patch('src.tag_engine.engine.TagEngine.HiveMeta'), \
             patch('src.tag_engine.engine.TagEngine.MysqlMeta'), \
             patch('src.tag_engine.engine.TagEngine.TagRuleParser'):
            
            tag_engine = TagEngine(mock_spark, mysqlConfig=mock_config)
            tag_engine._loadTagRules = Mock(return_value=Mock(count=Mock(return_value=2)))
            tag_engine._analyzeAndGroupTags = Mock(return_value=[Mock()])
            tag_engine._processTagGroupsPipeline = Mock(return_value=(True, []))  # 无失败标签
            
            success, failed_tag_ids = tag_engine.computeTags(mode="task-all")
            
            assert success is True
            assert failed_tag_ids == []
    
    def test_computeTags_exception_handling(self):
        """测试异常情况下的失败标签返回"""
        mock_spark = Mock()
        mock_config = {"host": "localhost", "port": 3306, "database": "test"}
        
        with patch('src.tag_engine.engine.TagEngine.HiveMeta'), \
             patch('src.tag_engine.engine.TagEngine.MysqlMeta'), \
             patch('src.tag_engine.engine.TagEngine.TagRuleParser'):
            
            tag_engine = TagEngine(mock_spark, mysqlConfig=mock_config)
            tag_engine._loadTagRules = Mock(side_effect=Exception("数据库连接失败"))
            
            # 测试全量计算异常
            success, failed_tag_ids = tag_engine.computeTags(mode="task-all")
            assert success is False
            assert failed_tag_ids == []  # 全量计算异常时返回空列表
            
            # 测试指定标签计算异常
            success, failed_tag_ids = tag_engine.computeTags(mode="task-tags", tagIds=[1, 2, 3])
            assert success is False
            assert failed_tag_ids == [1, 2, 3]  # 指定标签异常时返回所有指定标签
    
    def test_processTagGroupsPipeline_collect_failures(self):
        """测试流水线处理收集失败标签"""
        mock_spark = Mock()
        mock_config = {"host": "localhost", "port": 3306, "database": "test"}
        
        with patch('src.tag_engine.engine.TagEngine.HiveMeta'), \
             patch('src.tag_engine.engine.TagEngine.MysqlMeta'), \
             patch('src.tag_engine.engine.TagEngine.TagRuleParser'):
            
            tag_engine = TagEngine(mock_spark, mysqlConfig=mock_config)
            
            # Mock标签组
            mock_group1 = Mock()
            mock_group1.name = "Group1"
            mock_group1.tagIds = [1, 2]
            mock_group1.requiredTables = ["table1"]
            mock_group1.computeTags.return_value = (
                Mock(count=Mock(return_value=10)), 
                [2]  # 标签2失败
            )
            
            mock_group2 = Mock()
            mock_group2.name = "Group2"
            mock_group2.tagIds = [3, 4, 5]
            mock_group2.requiredTables = ["table2"]
            mock_group2.computeTags.return_value = (
                Mock(count=Mock(return_value=5)), 
                [4, 5]  # 标签4,5失败
            )
            
            # Mock其他依赖
            mock_rules_df = Mock()
            mock_rules_df.filter.return_value = Mock()
            tag_engine._mergeAndSaveGroup = Mock(return_value=True)
            tag_engine.hiveMeta.clearGroupCache = Mock()
            
            # 执行测试
            success, failed_tag_ids = tag_engine._processTagGroupsPipeline(
                [mock_group1, mock_group2], mock_rules_df
            )
            
            # 验证结果
            assert success is True
            assert set(failed_tag_ids) == {2, 4, 5}  # 收集所有失败的标签


class TestFailureSummaryIntegration:
    """测试失败摘要集成功能"""
    
    def test_failure_summary_display(self, spark):
        """测试失败摘要的显示格式"""
        hiveMeta = HiveMeta(spark)
        
        # 模拟失败情况
        hiveMeta.failed_tables = {
            "table1": "表不存在",
            "table2": "字段不存在: [unknown_field]",
            "table3": "权限不足"
        }
        
        summary = hiveMeta.getFailureSummary()
        
        # 验证摘要格式
        assert "失败表(3个)" in summary
        assert "table1: 表不存在" in summary
        assert "table2: 字段不存在" in summary
        assert "table3: 权限不足" in summary
    
    def test_no_failure_summary(self, spark):
        """测试无失败时的摘要"""
        hiveMeta = HiveMeta(spark)
        
        summary = hiveMeta.getFailureSummary()
        assert summary == "无表加载失败"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])