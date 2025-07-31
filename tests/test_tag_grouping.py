#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签分组逻辑测试
测试TagRuleParser的依赖分析和智能分组功能
"""
import pytest
import json
from src.tag_engine.parser.TagRuleParser import TagRuleParser


class TestTagGrouping:
    """标签分组逻辑测试类"""
    
    def test_analyze_dependencies_single_table(self, spark, sample_rules_data):
        """测试单表依赖分析"""
        parser = TagRuleParser()
        
        # 创建只有单表依赖的规则DataFrame
        single_table_rules = [
            {
                "tag_id": 1,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "total_asset_value",
                                        "operator": ">=",
                                        "value": "100000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            {
                "tag_id": 2,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "cash_balance",
                                        "operator": ">=",
                                        "value": "50000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            }
        ]
        
        rulesDF = spark.createDataFrame(single_table_rules)
        dependencies = parser.analyzeDependencies(rulesDF)
        
        # 验证依赖分析结果
        assert 1 in dependencies
        assert 2 in dependencies
        assert "tag_system.user_asset_summary" in dependencies[1]
        assert "tag_system.user_asset_summary" in dependencies[2]
        assert len(dependencies[1]) == 1  # 只依赖一个表
        assert len(dependencies[2]) == 1  # 只依赖一个表
    
    def test_analyze_dependencies_multi_table(self, spark):
        """测试多表依赖分析"""
        parser = TagRuleParser()
        
        # 创建多表依赖的规则
        multi_table_rules = [
            {
                "tag_id": 3,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "OR",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "user_level",
                                        "operator": "belongs_to",
                                        "value": ["VIP2", "VIP3"],
                                        "type": "enum"
                                    },
                                    {
                                        "table": "tag_system.user_asset_summary",
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
                                "fields": [
                                    {
                                        "table": "tag_system.user_activity_summary",
                                        "field": "trade_count_30d",
                                        "operator": ">",
                                        "value": "5",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            }
        ]
        
        rulesDF = spark.createDataFrame(multi_table_rules)
        dependencies = parser.analyzeDependencies(rulesDF)
        
        # 验证多表依赖
        assert 3 in dependencies
        expected_tables = {
            "tag_system.user_basic_info",
            "tag_system.user_asset_summary",
            "tag_system.user_activity_summary"
        }
        assert dependencies[3] == expected_tables
    
    def test_analyze_field_dependencies(self, spark):
        """测试字段依赖分析"""
        parser = TagRuleParser()
        
        rules_data = [
            {
                "tag_id": 1,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "AND",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "age",
                                        "operator": ">=",
                                        "value": "18",
                                        "type": "number"
                                    },
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "user_level",
                                        "operator": "!=",
                                        "value": "BANNED",
                                        "type": "string"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            {
                "tag_id": 2,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "total_asset_value",
                                        "operator": ">=",
                                        "value": "100000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            }
        ]
        
        rulesDF = spark.createDataFrame(rules_data)
        fieldDependencies = parser.analyzeFieldDependencies(rulesDF)
        
        # 验证字段依赖
        assert "tag_system.user_basic_info" in fieldDependencies
        assert "tag_system.user_asset_summary" in fieldDependencies
        
        basic_info_fields = fieldDependencies["tag_system.user_basic_info"]
        assert "age" in basic_info_fields
        assert "user_level" in basic_info_fields
        assert "user_id" in basic_info_fields  # 应该自动添加user_id
        
        asset_fields = fieldDependencies["tag_system.user_asset_summary"]
        assert "total_asset_value" in asset_fields
        assert "user_id" in asset_fields  # 应该自动添加user_id
    
    def test_group_tags_by_same_tables(self, spark):
        """测试相同表依赖的标签分组"""
        parser = TagRuleParser()
        
        # 创建有相同表依赖的标签规则
        same_table_rules = [
            {
                "tag_id": 1,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "total_asset_value",
                                        "operator": ">=",
                                        "value": "100000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            {
                "tag_id": 2,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "cash_balance",
                                        "operator": ">=",
                                        "value": "50000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            {
                "tag_id": 3,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_activity_summary",
                                        "field": "trade_count_30d",
                                        "operator": ">",
                                        "value": "10",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            }
        ]
        
        rulesDF = spark.createDataFrame(same_table_rules)
        dependencies = parser.analyzeDependencies(rulesDF)
        groups = parser.groupTagsByTables(dependencies)
        
        # 验证分组结果
        assert len(groups) == 2  # 应该分为2组
        
        # 找到各个组
        asset_group = None
        activity_group = None
        
        for group in groups:
            if "user_asset_summary" in group.requiredTables[0]:
                asset_group = group
            elif "user_activity_summary" in group.requiredTables[0]:
                activity_group = group
        
        # 验证资产表组
        assert asset_group is not None
        assert set(asset_group.tagIds) == {1, 2}
        assert len(asset_group.requiredTables) == 1
        assert "tag_system.user_asset_summary" in asset_group.requiredTables
        
        # 验证活动表组
        assert activity_group is not None
        assert set(activity_group.tagIds) == {3}
        assert len(activity_group.requiredTables) == 1
        assert "tag_system.user_activity_summary" in activity_group.requiredTables
    
    def test_group_tags_by_different_tables(self, spark):
        """测试不同表依赖的标签分组"""
        parser = TagRuleParser()
        
        # 创建不同表依赖的标签规则
        different_table_rules = [
            {
                "tag_id": 1,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "AND",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "user_level",
                                        "operator": "=",
                                        "value": "VIP3",
                                        "type": "string"
                                    },
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "total_asset_value",
                                        "operator": ">=",
                                        "value": "100000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            {
                "tag_id": 2,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "AND",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "age",
                                        "operator": ">=",
                                        "value": "25",
                                        "type": "number"
                                    },
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "cash_balance",
                                        "operator": ">=",
                                        "value": "50000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            {
                "tag_id": 3,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_activity_summary",
                                        "field": "trade_count_30d",
                                        "operator": ">",
                                        "value": "10",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            }
        ]
        
        rulesDF = spark.createDataFrame(different_table_rules)
        dependencies = parser.analyzeDependencies(rulesDF)
        groups = parser.groupTagsByTables(dependencies)
        
        # 验证分组结果
        assert len(groups) == 2  # 应该分为2组
        
        # 找到各个组
        multi_table_group = None
        single_table_group = None
        
        for group in groups:
            if len(group.requiredTables) > 1:
                multi_table_group = group
            else:
                single_table_group = group
        
        # 验证多表组（标签1和2有相同的表依赖组合）
        assert multi_table_group is not None
        assert set(multi_table_group.tagIds) == {1, 2}
        assert len(multi_table_group.requiredTables) == 2
        expected_tables = {"tag_system.user_basic_info", "tag_system.user_asset_summary"}
        assert set(multi_table_group.requiredTables) == expected_tables
        
        # 验证单表组（标签3只依赖一个表）
        assert single_table_group is not None
        assert set(single_table_group.tagIds) == {3}
        assert len(single_table_group.requiredTables) == 1
        assert "tag_system.user_activity_summary" in single_table_group.requiredTables
    
    def test_group_tags_complex_scenario(self, spark):
        """测试复杂场景的标签分组"""
        parser = TagRuleParser()
        
        # 创建复杂的标签规则组合
        complex_rules = [
            # 组1: 只依赖user_basic_info
            {
                "tag_id": 1,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "age",
                                        "operator": ">=",
                                        "value": "18",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            # 组1: 也只依赖user_basic_info
            {
                "tag_id": 2,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "user_level",
                                        "operator": "!=",
                                        "value": "BANNED",
                                        "type": "string"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            # 组2: 依赖user_basic_info + user_asset_summary
            {
                "tag_id": 3,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "AND",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "user_level",
                                        "operator": "=",
                                        "value": "VIP3",
                                        "type": "string"
                                    },
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "total_asset_value",
                                        "operator": ">=",
                                        "value": "100000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            # 组3: 依赖user_activity_summary + user_asset_summary
            {
                "tag_id": 4,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "AND",
                                "fields": [
                                    {
                                        "table": "tag_system.user_activity_summary",
                                        "field": "trade_count_30d",
                                        "operator": ">",
                                        "value": "10",
                                        "type": "number"
                                    },
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "cash_balance",
                                        "operator": ">=",
                                        "value": "50000",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            },
            # 组4: 依赖全部三个表
            {
                "tag_id": 5,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "AND",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "age",
                                        "operator": ">=",
                                        "value": "25",
                                        "type": "number"
                                    },
                                    {
                                        "table": "tag_system.user_asset_summary",
                                        "field": "total_asset_value",
                                        "operator": ">=",
                                        "value": "200000",
                                        "type": "number"
                                    },
                                    {
                                        "table": "tag_system.user_activity_summary",
                                        "field": "trade_count_30d",
                                        "operator": ">",
                                        "value": "20",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            }
        ]
        
        rulesDF = spark.createDataFrame(complex_rules)
        dependencies = parser.analyzeDependencies(rulesDF)
        groups = parser.groupTagsByTables(dependencies)
        
        # 验证分组结果
        assert len(groups) == 4  # 应该分为4组
        
        # 统计各组的特征
        group_info = {}
        for group in groups:
            table_key = tuple(sorted(group.requiredTables))
            group_info[table_key] = set(group.tagIds)
        
        # 验证各组
        basic_info_only = ("tag_system.user_basic_info",)
        basic_asset = tuple(sorted(["tag_system.user_basic_info", "tag_system.user_asset_summary"]))
        activity_asset = tuple(sorted(["tag_system.user_activity_summary", "tag_system.user_asset_summary"]))
        all_three = tuple(sorted(["tag_system.user_basic_info", "tag_system.user_asset_summary", "tag_system.user_activity_summary"]))
        
        assert basic_info_only in group_info
        assert basic_asset in group_info
        assert activity_asset in group_info
        assert all_three in group_info
        
        assert group_info[basic_info_only] == {1, 2}
        assert group_info[basic_asset] == {3}
        assert group_info[activity_asset] == {4}
        assert group_info[all_three] == {5}
    
    def test_empty_rules_handling(self, spark):
        """测试空规则处理"""
        parser = TagRuleParser()
        
        # 空DataFrame
        empty_rules = []
        rulesDF = spark.createDataFrame(empty_rules, "tag_id int, rule_conditions string")
        
        dependencies = parser.analyzeDependencies(rulesDF)
        assert len(dependencies) == 0
        
        groups = parser.groupTagsByTables(dependencies)
        assert len(groups) == 0
    
    def test_invalid_rule_dependencies(self, spark):
        """测试无效规则的依赖分析"""
        parser = TagRuleParser()
        
        # 包含无效规则的数据
        invalid_rules = [
            {
                "tag_id": 1,
                "rule_conditions": "invalid json"
            },
            {
                "tag_id": 2,
                "rule_conditions": ""
            },
            {
                "tag_id": 3,
                "rule_conditions": json.dumps({
                    "logic": "AND",
                    "conditions": [
                        {
                            "condition": {
                                "logic": "None",
                                "fields": [
                                    {
                                        "table": "tag_system.user_basic_info",
                                        "field": "age",
                                        "operator": ">=",
                                        "value": "18",
                                        "type": "number"
                                    }
                                ]
                            }
                        }
                    ]
                })
            }
        ]
        
        rulesDF = spark.createDataFrame(invalid_rules)
        dependencies = parser.analyzeDependencies(rulesDF)
        
        # 只有有效的规则应该被分析
        assert len(dependencies) == 3
        assert len(dependencies[1]) == 0  # 无效规则返回空集合
        assert len(dependencies[2]) == 0  # 空规则返回空集合
        assert len(dependencies[3]) == 1  # 有效规则返回正确依赖
        assert "tag_system.user_basic_info" in dependencies[3]