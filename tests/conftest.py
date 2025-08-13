#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pytest配置文件
提供测试用的Spark会话和测试数据
"""
import pytest
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *


@pytest.fixture(scope="session")
def spark():
    """创建测试用的Spark会话"""
    spark = SparkSession.builder \
        .appName("TagSystem_Test") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def sample_rules_data():
    """提供测试用的标签规则数据"""
    return [
        {
            "tag_id": 1,
            "tag_name": "高净值用户",
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
            }),
            "description": "总资产大于等于10万的用户"
        },
        {
            "tag_id": 2,
            "tag_name": "活跃交易者",
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
            }),
            "description": "30天交易次数大于10的用户"
        },
        {
            "tag_id": 3,
            "tag_name": "VIP用户",
            "rule_conditions": json.dumps({
                "logic": "OR",
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
                    },
                    {
                        "condition": {
                            "logic": "None",
                            "fields": [
                                {
                                    "table": "tag_system.user_asset_summary",
                                    "field": "total_asset_value",
                                    "operator": ">=",
                                    "value": "500000",
                                    "type": "number"
                                }
                            ]
                        }
                    }
                ]
            }),
            "description": "VIP等级用户或资产超过50万的用户"
        },
        {
            "tag_id": 4,
            "tag_name": "复杂组合标签",
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
            }),
            "description": "复杂多条件组合标签"
        }
    ]


@pytest.fixture
def sample_user_data():
    """提供测试用的用户数据"""
    return {
        "user_basic_info": [
            ("user001", 30, "VIP2", "verified", True),
            ("user002", 25, "VIP1", "verified", False),
            ("user003", 35, "VIP3", "pending", True),
            ("user004", 28, "REGULAR", "verified", False),
            ("user005", 40, "VIP4", "verified", True)
        ],
        "user_asset_summary": [
            ("user001", 150000.00, 50000.00),
            ("user002", 80000.00, 20000.00),
            ("user003", 300000.00, 100000.00),
            ("user004", 25000.00, 5000.00),
            ("user005", 600000.00, 200000.00)
        ],
        "user_activity_summary": [
            ("user001", 15, "2025-07-30"),
            ("user002", 8, "2025-07-29"),
            ("user003", 25, "2025-07-30"),
            ("user004", 3, "2025-07-28"),
            ("user005", 30, "2025-07-30")
        ]
    }


@pytest.fixture
def create_test_tables(spark, sample_user_data):
    """创建测试用的Hive表"""
    
    # 创建user_basic_info表
    basic_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("user_level", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("is_vip", BooleanType(), True)
    ])
    
    basic_df = spark.createDataFrame(sample_user_data["user_basic_info"], basic_schema)
    basic_df.createOrReplaceTempView("user_basic_info")
    
    # 创建user_asset_summary表
    asset_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("total_asset_value", DoubleType(), True),
        StructField("cash_balance", DoubleType(), True)
    ])
    
    asset_df = spark.createDataFrame(sample_user_data["user_asset_summary"], asset_schema)
    asset_df.createOrReplaceTempView("user_asset_summary")
    
    # 创建user_activity_summary表
    activity_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("trade_count_30d", IntegerType(), True),
        StructField("last_login_date", StringType(), True)
    ])
    
    activity_df = spark.createDataFrame(sample_user_data["user_activity_summary"], activity_schema)
    activity_df.createOrReplaceTempView("user_activity_summary")
    
    return {
        "user_basic_info": basic_df,
        "user_asset_summary": asset_df,
        "user_activity_summary": activity_df
    }