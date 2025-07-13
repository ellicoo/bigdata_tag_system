"""
规则解析器单元测试
"""

import unittest
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.engine.rule_parser import RuleConditionParser


class TestRuleConditionParser(unittest.TestCase):
    """测试规则条件解析器"""
    
    def setUp(self):
        self.parser = RuleConditionParser()
    
    def test_simple_equal_condition(self):
        """测试简单等于条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "kyc_status",
                    "operator": "=",
                    "value": "verified",
                    "type": "string"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(kyc_status = 'verified')"
        self.assertEqual(result, expected)
    
    def test_number_comparison(self):
        """测试数字比较条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "total_asset_value",
                    "operator": ">=",
                    "value": 100000,
                    "type": "number"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(total_asset_value >= 100000)"
        self.assertEqual(result, expected)
    
    def test_in_condition(self):
        """测试IN条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "user_level",
                    "operator": "in",
                    "value": ["VIP1", "VIP2", "VIP3"],
                    "type": "string"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(user_level IN ('VIP1', 'VIP2', 'VIP3'))"
        self.assertEqual(result, expected)
    
    def test_not_in_condition(self):
        """测试NOT IN条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "risk_level",
                    "operator": "not_in",
                    "value": ["HIGH", "EXTREME"],
                    "type": "string"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(risk_level NOT IN ('HIGH', 'EXTREME'))"
        self.assertEqual(result, expected)
    
    def test_range_condition(self):
        """测试范围条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "age",
                    "operator": "in_range",
                    "value": [18, 65],
                    "type": "number"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(age BETWEEN 18 AND 65)"
        self.assertEqual(result, expected)
    
    def test_contains_condition(self):
        """测试包含条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "user_tags",
                    "operator": "contains",
                    "value": "premium",
                    "type": "string"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(user_tags LIKE '%premium%')"
        self.assertEqual(result, expected)
    
    def test_null_conditions(self):
        """测试NULL条件"""
        # is_null
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "phone_number",
                    "operator": "is_null",
                    "value": None,
                    "type": "string"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(phone_number IS NULL)"
        self.assertEqual(result, expected)
        
        # is_not_null
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "email",
                    "operator": "is_not_null",
                    "value": None,
                    "type": "string"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(email IS NOT NULL)"
        self.assertEqual(result, expected)
    
    def test_recent_days_condition(self):
        """测试最近天数条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "last_login_date",
                    "operator": "recent_days",
                    "value": 30,
                    "type": "date"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(last_login_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY))"
        self.assertEqual(result, expected)
    
    def test_and_logic(self):
        """测试AND逻辑"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "total_asset_value",
                    "operator": ">=",
                    "value": 100000,
                    "type": "number"
                },
                {
                    "field": "kyc_status",
                    "operator": "=",
                    "value": "verified",
                    "type": "string"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(total_asset_value >= 100000 AND kyc_status = 'verified')"
        self.assertEqual(result, expected)
    
    def test_or_logic(self):
        """测试OR逻辑"""
        conditions = {
            "logic": "OR",
            "conditions": [
                {
                    "field": "user_level",
                    "operator": "=",
                    "value": "VIP3",
                    "type": "string"
                },
                {
                    "field": "total_asset_value",
                    "operator": ">=",
                    "value": 500000,
                    "type": "number"
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(user_level = 'VIP3' OR total_asset_value >= 500000)"
        self.assertEqual(result, expected)
    
    def test_nested_conditions(self):
        """测试嵌套条件"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "kyc_status",
                    "operator": "=",
                    "value": "verified",
                    "type": "string"
                },
                {
                    "logic": "OR",
                    "conditions": [
                        {
                            "field": "user_level",
                            "operator": "=",
                            "value": "VIP3",
                            "type": "string"
                        },
                        {
                            "field": "total_asset_value",
                            "operator": ">=",
                            "value": 1000000,
                            "type": "number"
                        }
                    ]
                }
            ]
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        expected = "(kyc_status = 'verified' AND (user_level = 'VIP3' OR total_asset_value >= 1000000))"
        self.assertEqual(result, expected)
    
    def test_invalid_operator(self):
        """测试无效操作符"""
        conditions = {
            "logic": "AND",
            "conditions": [
                {
                    "field": "test_field",
                    "operator": "invalid_op",
                    "value": "test",
                    "type": "string"
                }
            ]
        }
        
        with self.assertRaises(ValueError):
            self.parser.parse_rule_conditions(conditions)
    
    def test_empty_conditions(self):
        """测试空条件"""
        conditions = {
            "logic": "AND",
            "conditions": []
        }
        
        result = self.parser.parse_rule_conditions(conditions)
        self.assertEqual(result, "(1=1)")  # 空条件应返回总是为真的条件


if __name__ == "__main__":
    unittest.main(verbosity=2)