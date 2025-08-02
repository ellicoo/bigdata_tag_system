#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签规则解析器测试
测试TagRuleParser的规则解析和SQL生成功能
"""
import pytest
import json
from src.tag_engine.parser.TagRuleParser import TagRuleParser


class TestTagRuleParser:
    """TagRuleParser测试类"""
    
    def test_init(self):
        """测试初始化"""
        parser = TagRuleParser()
        assert parser is not None
    
    def test_simple_number_condition_sql_generation(self):
        """测试简单数值条件SQL生成"""
        parser = TagRuleParser()
        
        # 简单大于等于条件
        rule_json = json.dumps({
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
        
        # 测试多表场景
        sql = parser.parseRuleToSql(rule_json, ["tag_system.user_asset_summary", "tag_system.user_basic_info"])
        expected = "user_asset_summary.total_asset_value >= 100000"
        assert expected in sql
        
        # 测试单表场景
        sql_single = parser.parseRuleToSql(rule_json, ["tag_system.user_asset_summary"])
        expected_single = "user_asset_summary.total_asset_value >= 100000"
        assert expected_single in sql_single
    
    def test_string_condition_sql_generation(self):
        """测试字符串条件SQL生成"""
        parser = TagRuleParser()
        
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "user_level",
                                "operator": "=",
                                "value": "VIP3",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json, ["tag_system.user_basic_info"])
        expected = "user_basic_info.user_level = 'VIP3'"
        assert expected in sql
    
    def test_enum_belongs_to_condition(self):
        """测试枚举属于条件"""
        parser = TagRuleParser()
        
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "user_level",
                                "operator": "belongs_to",
                                "value": ["VIP2", "VIP3", "VIP4"],
                                "type": "enum"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json, ["tag_system.user_basic_info"])
        expected = "user_basic_info.user_level IN ('VIP2','VIP3','VIP4')"
        assert expected in sql
    
    def test_date_range_condition(self):
        """测试日期范围条件"""
        parser = TagRuleParser()
        
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "registration_date",
                                "operator": "date_in_range",
                                "value": ["2024-01-01", "2024-12-31"],
                                "type": "date"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json, ["user_basic_info"])
        expected = "user_basic_info.registration_date BETWEEN '2024-01-01' AND '2024-12-31'"
        assert expected in sql
    
    def test_boolean_condition(self):
        """测试布尔条件"""
        parser = TagRuleParser()
        
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "is_vip",
                                "operator": "is_true",
                                "value": "true",
                                "type": "boolean"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json, ["user_basic_info"])
        expected = "tag_system.user_basic_info.is_vip = TRUE"
        assert expected in sql
    
    def test_null_conditions(self):
        """测试空值条件"""
        parser = TagRuleParser()
        
        # is_null条件
        rule_json_null = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "middle_name",
                                "operator": "is_null",
                                "value": "",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json_null, ["user_basic_info"])
        expected = "tag_system.user_basic_info.middle_name IS NULL"
        assert expected in sql
        
        # is_not_null条件
        rule_json_not_null = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "first_name",
                                "operator": "is_not_null",
                                "value": "",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json_not_null, ["user_basic_info"])
        expected = "tag_system.user_basic_info.first_name IS NOT NULL"
        assert expected in sql
    
    def test_complex_multi_condition_and_logic(self):
        """测试复杂多条件AND逻辑"""
        parser = TagRuleParser()
        
        rule_json = json.dumps({
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
                        "logic": "AND",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "kyc_status",
                                "operator": "=",
                                "value": "verified",
                                "type": "enum"
                            },
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
        
        sql = parser.parseRuleToSql(rule_json, ["user_basic_info", "user_asset_summary", "user_activity_summary"])
        
        # 验证包含各个条件部分
        assert "tag_system.user_basic_info.user_level IN ('VIP2','VIP3')" in sql
        assert "tag_system.user_asset_summary.total_asset_value >= 100000" in sql
        assert "tag_system.user_basic_info.kyc_status = 'verified'" in sql
        assert "tag_system.user_activity_summary.trade_count_30d > 5" in sql
        
        # 验证逻辑结构：外层AND，内层有OR和AND
        assert " AND " in sql
        assert " OR " in sql
    
    def test_complex_multi_condition_or_logic(self):
        """测试复杂多条件OR逻辑"""
        parser = TagRuleParser()
        
        rule_json = json.dumps({
            "logic": "OR",
            "conditions": [
                {
                    "condition": {
                        "logic": "AND",
                        "fields": [
                            {
                                "table": "tag_system.user_activity_summary",
                                "field": "last_login_date",
                                "operator": "date_in_range",
                                "value": ["2025-01-01", "2025-07-26"],
                                "type": "date"
                            },
                            {
                                "table": "tag_system.user_activity_summary",
                                "field": "trade_count_30d",
                                "operator": ">",
                                "value": "5",
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
        
        sql = parser.parseRuleToSql(rule_json, ["user_activity_summary", "user_asset_summary"])
        
        # 验证包含各个条件部分
        assert "BETWEEN '2025-01-01' AND '2025-07-26'" in sql
        assert "tag_system.user_activity_summary.trade_count_30d > 5" in sql
        assert "tag_system.user_asset_summary.cash_balance >= 50000" in sql
        
        # 验证逻辑结构：外层OR，内层AND
        assert " OR " in sql
        assert " AND " in sql
    
    def test_not_logic(self):
        """测试NOT逻辑"""
        parser = TagRuleParser()
        
        # 简单NOT逻辑
        rule_json = json.dumps({
            "logic": "NOT",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "user_level",
                                "operator": "=",
                                "value": "VIP1",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json, ["user_basic_info"])
        
        # NOT逻辑应该在最外层
        assert sql.startswith("NOT") or "NOT (" in sql
        assert "tag_system.user_basic_info.user_level = 'VIP1'" in sql
    
    def test_string_pattern_matching(self):
        """测试字符串模式匹配"""
        parser = TagRuleParser()
        
        # contains操作符
        rule_json_contains = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "phone_number",
                                "operator": "contains",
                                "value": "138",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json_contains, ["user_basic_info"])
        expected = "tag_system.user_basic_info.phone_number LIKE '%138%'"
        assert expected in sql
        
        # starts_with操作符
        rule_json_starts = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "phone_number",
                                "operator": "starts_with",
                                "value": "+86",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json_starts, ["user_basic_info"])
        expected = "tag_system.user_basic_info.phone_number LIKE '+86%'"
        assert expected in sql
        
        # ends_with操作符
        rule_json_ends = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "email",
                                "operator": "ends_with",
                                "value": "gmail.com",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json_ends, ["user_basic_info"])
        expected = "tag_system.user_basic_info.email LIKE '%gmail.com'"
        assert expected in sql
    
    def test_list_operations(self):
        """测试列表操作"""
        parser = TagRuleParser()
        
        # contains_any操作符
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_preferences",
                                "field": "interested_products",
                                "operator": "contains_any",
                                "value": ["stocks", "bonds"],
                                "type": "list"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json, ["user_preferences"])
        assert "array_contains" in sql
        assert " OR " in sql  # contains_any应该用OR连接
        
        # contains_all操作符
        rule_json_all = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "None",
                        "fields": [
                            {
                                "table": "tag_system.user_preferences",
                                "field": "owned_products",
                                "operator": "contains_all",
                                "value": ["savings", "checking"],
                                "type": "list"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json_all, ["user_preferences"])
        assert "array_contains" in sql
        assert " AND " in sql  # contains_all应该用AND连接
    
    def test_invalid_rule_handling(self):
        """测试无效规则处理"""
        parser = TagRuleParser()
        
        # 空规则
        sql = parser.parseRuleToSql("", ["user_basic_info"])
        assert sql == "1=0"
        
        # 无效JSON
        sql = parser.parseRuleToSql("invalid json", ["user_basic_info"])
        assert sql == "1=0"
        
        # None规则
        sql = parser.parseRuleToSql(None, ["user_basic_info"])
        assert sql == "1=0"
    
    def test_field_logic_precedence(self):
        """测试字段逻辑优先级"""
        parser = TagRuleParser()
        
        # 测试字段间OR逻辑被正确处理
        rule_json = json.dumps({
            "logic": "AND",
            "conditions": [
                {
                    "condition": {
                        "logic": "OR",  # 字段间OR逻辑
                        "fields": [
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "user_level",
                                "operator": "=",
                                "value": "VIP2",
                                "type": "string"
                            },
                            {
                                "table": "tag_system.user_basic_info",
                                "field": "user_level",
                                "operator": "=",
                                "value": "VIP3",
                                "type": "string"
                            }
                        ]
                    }
                }
            ]
        })
        
        sql = parser.parseRuleToSql(rule_json, ["user_basic_info"])
        
        # 验证字段间的OR逻辑被正确解析
        assert "tag_system.user_basic_info.user_level = 'VIP2'" in sql
        assert "tag_system.user_basic_info.user_level = 'VIP3'" in sql
        assert " OR " in sql