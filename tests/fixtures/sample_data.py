"""
测试用的样例数据
"""

import json
from datetime import datetime, date


class SampleData:
    """样例数据生成器"""
    
    @staticmethod
    def get_user_basic_info():
        """获取用户基础信息样例"""
        return [
            {
                "user_id": "user_001",
                "kyc_status": "verified",
                "user_level": "VIP2",
                "registration_date": date(2024, 1, 15),
                "country": "CN",
                "age": 35,
                "gender": "M"
            },
            {
                "user_id": "user_002",
                "kyc_status": "verified",
                "user_level": "VIP1",
                "registration_date": date(2024, 2, 20),
                "country": "CN",
                "age": 28,
                "gender": "F"
            },
            {
                "user_id": "user_003",
                "kyc_status": "pending",
                "user_level": "VIP3",
                "registration_date": date(2024, 3, 10),
                "country": "US",
                "age": 42,
                "gender": "M"
            },
            {
                "user_id": "user_004",
                "kyc_status": "verified",
                "user_level": "VIP1",
                "registration_date": date(2024, 4, 5),
                "country": "CN",
                "age": 31,
                "gender": "F"
            },
            {
                "user_id": "user_005",
                "kyc_status": "verified",
                "user_level": "VIP3",
                "registration_date": date(2024, 5, 12),
                "country": "SG",
                "age": 38,
                "gender": "M"
            }
        ]
    
    @staticmethod
    def get_user_asset_summary():
        """获取用户资产汇总样例"""
        return [
            {
                "user_id": "user_001",
                "total_asset_value": 150000,
                "cash_balance": 50000,
                "crypto_balance": 100000,
                "stock_balance": 0,
                "last_updated": datetime(2024, 7, 13, 10, 0, 0)
            },
            {
                "user_id": "user_002",
                "total_asset_value": 50000,
                "cash_balance": 20000,
                "crypto_balance": 30000,
                "stock_balance": 0,
                "last_updated": datetime(2024, 7, 13, 10, 0, 0)
            },
            {
                "user_id": "user_003",
                "total_asset_value": 200000,
                "cash_balance": 100000,
                "crypto_balance": 80000,
                "stock_balance": 20000,
                "last_updated": datetime(2024, 7, 13, 10, 0, 0)
            },
            {
                "user_id": "user_004",
                "total_asset_value": 80000,
                "cash_balance": 30000,
                "crypto_balance": 50000,
                "stock_balance": 0,
                "last_updated": datetime(2024, 7, 13, 10, 0, 0)
            },
            {
                "user_id": "user_005",
                "total_asset_value": 300000,
                "cash_balance": 150000,
                "crypto_balance": 100000,
                "stock_balance": 50000,
                "last_updated": datetime(2024, 7, 13, 10, 0, 0)
            }
        ]
    
    @staticmethod
    def get_user_activity_summary():
        """获取用户活动汇总样例"""
        return [
            {
                "user_id": "user_001",
                "login_count_30d": 25,
                "trade_count_30d": 15,
                "last_login_date": date(2024, 7, 12),
                "last_trade_date": date(2024, 7, 10),
                "total_trade_volume": 50000
            },
            {
                "user_id": "user_002",
                "login_count_30d": 10,
                "trade_count_30d": 5,
                "last_login_date": date(2024, 7, 5),
                "last_trade_date": date(2024, 7, 3),
                "total_trade_volume": 15000
            },
            {
                "user_id": "user_003",
                "login_count_30d": 30,
                "trade_count_30d": 20,
                "last_login_date": date(2024, 7, 13),
                "last_trade_date": date(2024, 7, 13),
                "total_trade_volume": 80000
            },
            {
                "user_id": "user_004",
                "login_count_30d": 8,
                "trade_count_30d": 3,
                "last_login_date": date(2024, 6, 25),
                "last_trade_date": date(2024, 6, 20),
                "total_trade_volume": 8000
            },
            {
                "user_id": "user_005",
                "login_count_30d": 28,
                "trade_count_30d": 25,
                "last_login_date": date(2024, 7, 13),
                "last_trade_date": date(2024, 7, 12),
                "total_trade_volume": 120000
            }
        ]
    
    @staticmethod
    def get_tag_rules():
        """获取标签规则样例"""
        return [
            {
                "tag_id": 1,
                "tag_name": "高净值用户",
                "tag_category": "资产等级",
                "description": "总资产价值大于等于10万的用户",
                "rule_conditions": {
                    "logic": "AND",
                    "conditions": [
                        {
                            "field": "total_asset_value",
                            "operator": ">=",
                            "value": 100000,
                            "type": "number"
                        }
                    ]
                },
                "is_active": True,
                "created_date": datetime(2024, 1, 1),
                "updated_date": datetime(2024, 1, 1)
            },
            {
                "tag_id": 2,
                "tag_name": "VIP客户",
                "tag_category": "客户等级",
                "description": "VIP2或VIP3级别且KYC已验证的用户",
                "rule_conditions": {
                    "logic": "AND",
                    "conditions": [
                        {
                            "field": "user_level",
                            "operator": "in",
                            "value": ["VIP2", "VIP3"],
                            "type": "string"
                        },
                        {
                            "field": "kyc_status",
                            "operator": "=",
                            "value": "verified",
                            "type": "string"
                        }
                    ]
                },
                "is_active": True,
                "created_date": datetime(2024, 1, 1),
                "updated_date": datetime(2024, 1, 1)
            },
            {
                "tag_id": 3,
                "tag_name": "活跃交易者",
                "tag_category": "行为特征",
                "description": "30天内交易次数大于10次的用户",
                "rule_conditions": {
                    "logic": "AND",
                    "conditions": [
                        {
                            "field": "trade_count_30d",
                            "operator": ">",
                            "value": 10,
                            "type": "number"
                        }
                    ]
                },
                "is_active": True,
                "created_date": datetime(2024, 1, 1),
                "updated_date": datetime(2024, 1, 1)
            },
            {
                "tag_id": 4,
                "tag_name": "现金充足",
                "tag_category": "资产结构",
                "description": "现金余额大于等于10万的用户",
                "rule_conditions": {
                    "logic": "AND",
                    "conditions": [
                        {
                            "field": "cash_balance",
                            "operator": ">=",
                            "value": 100000,
                            "type": "number"
                        }
                    ]
                },
                "is_active": True,
                "created_date": datetime(2024, 1, 1),
                "updated_date": datetime(2024, 1, 1)
            },
            {
                "tag_id": 5,
                "tag_name": "年轻用户",
                "tag_category": "人口特征",
                "description": "年龄在18-30岁之间的用户",
                "rule_conditions": {
                    "logic": "AND",
                    "conditions": [
                        {
                            "field": "age",
                            "operator": "in_range",
                            "value": [18, 30],
                            "type": "number"
                        }
                    ]
                },
                "is_active": True,
                "created_date": datetime(2024, 1, 1),
                "updated_date": datetime(2024, 1, 1)
            },
            {
                "tag_id": 6,
                "tag_name": "最近活跃",
                "tag_category": "时效性",
                "description": "最近7天内有登录的用户",
                "rule_conditions": {
                    "logic": "AND",
                    "conditions": [
                        {
                            "field": "last_login_date",
                            "operator": "recent_days",
                            "value": 7,
                            "type": "date"
                        }
                    ]
                },
                "is_active": True,
                "created_date": datetime(2024, 1, 1),
                "updated_date": datetime(2024, 1, 1)
            },
            {
                "tag_id": 7,
                "tag_name": "复合条件用户",
                "tag_category": "综合评估",
                "description": "高净值且活跃的VIP客户",
                "rule_conditions": {
                    "logic": "AND",
                    "conditions": [
                        {
                            "field": "total_asset_value",
                            "operator": ">=",
                            "value": 100000,
                            "type": "number"
                        },
                        {
                            "field": "user_level",
                            "operator": "in",
                            "value": ["VIP2", "VIP3"],
                            "type": "string"
                        },
                        {
                            "logic": "OR",
                            "conditions": [
                                {
                                    "field": "trade_count_30d",
                                    "operator": ">=",
                                    "value": 10,
                                    "type": "number"
                                },
                                {
                                    "field": "login_count_30d",
                                    "operator": ">=",
                                    "value": 20,
                                    "type": "number"
                                }
                            ]
                        }
                    ]
                },
                "is_active": True,
                "created_date": datetime(2024, 1, 1),
                "updated_date": datetime(2024, 1, 1)
            }
        ]
    
    @staticmethod
    def get_expected_tag_results():
        """获取预期的标签计算结果"""
        return {
            "tag_1": ["user_001", "user_003", "user_005"],  # 高净值用户
            "tag_2": ["user_001", "user_005"],              # VIP客户
            "tag_3": ["user_001", "user_003", "user_005"],  # 活跃交易者
            "tag_4": ["user_003", "user_005"],              # 现金充足
            "tag_5": ["user_002"],                          # 年轻用户
            "tag_6": ["user_001", "user_003", "user_005"],  # 最近活跃
            "tag_7": ["user_001", "user_005"]               # 复合条件用户
        }


class SampleRules:
    """样例规则生成器"""
    
    @staticmethod
    def simple_rule():
        """简单单条件规则"""
        return {
            "tag_id": 100,
            "tag_name": "测试标签",
            "rule_conditions": {
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
        }
    
    @staticmethod
    def complex_rule():
        """复杂多条件规则"""
        return {
            "tag_id": 101,
            "tag_name": "复杂测试标签",
            "rule_conditions": {
                "logic": "AND",
                "conditions": [
                    {
                        "field": "total_asset_value",
                        "operator": ">=",
                        "value": 50000,
                        "type": "number"
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
                                "field": "trade_count_30d",
                                "operator": ">=",
                                "value": 15,
                                "type": "number"
                            }
                        ]
                    }
                ]
            }
        }
    
    @staticmethod
    def invalid_rule():
        """无效规则（用于错误测试）"""
        return {
            "tag_id": 999,
            "tag_name": "无效标签",
            "rule_conditions": {
                "logic": "AND",
                "conditions": [
                    {
                        "field": "invalid_field",
                        "operator": "invalid_op",
                        "value": "invalid_value",
                        "type": "string"
                    }
                ]
            }
        }