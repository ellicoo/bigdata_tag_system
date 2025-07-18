"""
基础功能测试
"""

import unittest
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.base import SparkConfig, S3Config, MySQLConfig, BaseConfig
from src.config.manager import ConfigManager
from src.engine.rule_parser import RuleConditionParser
from src.engine.parallel_tag_engine import ParallelTagEngine


class TestRuleParser(unittest.TestCase):
    """测试规则解析器"""
    
    def setUp(self):
        self.parser = RuleConditionParser()
    
    def test_simple_condition(self):
        """测试简单条件解析"""
        rule_conditions = {
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
        
        result = self.parser.parse_rule_conditions(rule_conditions)
        expected = "(total_asset_value >= 100000)"
        self.assertEqual(result, expected)
    
    def test_multiple_conditions(self):
        """测试多条件解析"""
        rule_conditions = {
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
        
        result = self.parser.parse_rule_conditions(rule_conditions)
        expected = "(total_asset_value >= 100000 AND kyc_status = 'verified')"
        self.assertEqual(result, expected)
    
    def test_range_condition(self):
        """测试范围条件"""
        rule_conditions = {
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
        
        result = self.parser.parse_rule_conditions(rule_conditions)
        expected = "(age BETWEEN 18 AND 65)"
        self.assertEqual(result, expected)
    
    def test_in_condition(self):
        """测试IN条件"""
        rule_conditions = {
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
        
        result = self.parser.parse_rule_conditions(rule_conditions)
        expected = "(user_level IN ('VIP1', 'VIP2', 'VIP3'))"
        self.assertEqual(result, expected)


class TestTagCompute(unittest.TestCase):
    """测试标签计算引擎"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestTagCompute") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
        
        cls.tag_engine = ParallelTagEngine(cls.spark)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def setUp(self):
        # 创建测试数据
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("total_asset_value", IntegerType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("user_level", StringType(), True)
        ])
        
        test_data = [
            ("user_001", 150000, "verified", "VIP2"),
            ("user_002", 50000, "verified", "VIP1"),
            ("user_003", 200000, "pending", "VIP3"),
            ("user_004", 80000, "verified", "VIP1"),
            ("user_005", 300000, "verified", "VIP3")
        ]
        
        self.test_df = self.spark.createDataFrame(test_data, schema)
    
    def test_high_value_user_tag(self):
        """测试高净值用户标签计算"""
        rule = {
            "tag_id": 1,
            "tag_name": "高净值用户",
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
            }
        }
        
        result_df = self.tag_engine._compute_single_tag(self.test_df, rule)
        
        self.assertIsNotNone(result_df)
        
        # 验证结果
        result_count = result_df.count()
        self.assertEqual(result_count, 3)  # user_001, user_003, user_005
        
        # 验证字段
        self.assertIn("user_id", result_df.columns)
        self.assertIn("tag_id", result_df.columns)
        self.assertIn("tag_detail", result_df.columns)
    
    def test_vip_user_tag(self):
        """测试VIP用户标签计算"""
        rule = {
            "tag_id": 2,
            "tag_name": "VIP用户", 
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
            }
        }
        
        result_df = self.tag_engine._compute_single_tag(self.test_df, rule)
        
        self.assertIsNotNone(result_df)
        
        # 验证结果
        result_count = result_df.count()
        self.assertEqual(result_count, 2)  # user_001, user_005
    
    def test_batch_compute(self):
        """测试批量标签计算"""
        rules = [
            {
                "tag_id": 1,
                "tag_name": "高净值用户",
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
                }
            },
            {
                "tag_id": 2,
                "tag_name": "KYC用户",
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
        ]
        
        results = self.tag_engine._compute_tags_parallel(self.test_df, rules)
        
        self.assertEqual(len(results), 2)
        
        # 验证第一个标签结果
        high_value_result = results[0]
        self.assertEqual(high_value_result.count(), 3)
        
        # 验证第二个标签结果  
        kyc_result = results[1]
        self.assertEqual(kyc_result.count(), 4)


class TestConfig(unittest.TestCase):
    """测试配置管理"""
    
    def test_spark_config(self):
        """测试Spark配置"""
        config = SparkConfig(
            app_name="TestApp",
            master="local[2]",
            executor_memory="2g"
        )
        
        config_dict = config.to_dict()
        
        self.assertEqual(config_dict["spark.app.name"], "TestApp")
        self.assertEqual(config_dict["spark.master"], "local[2]")
        self.assertEqual(config_dict["spark.executor.memory"], "2g")
    
    def test_mysql_config(self):
        """测试MySQL配置"""
        config = MySQLConfig(
            host="localhost",
            port=3306,
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        expected_url = "jdbc:mysql://localhost:3306/test_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&connectionCollation=utf8mb4_unicode_ci"
        self.assertEqual(config.jdbc_url, expected_url)
        
        props = config.connection_properties
        self.assertEqual(props["user"], "test_user")
        self.assertEqual(props["password"], "test_pass")
        self.assertEqual(props["driver"], "com.mysql.cj.jdbc.Driver")
    
    def test_config_manager(self):
        """测试配置管理器"""
        # 测试本地环境配置加载
        config = ConfigManager.load_config('local')
        
        self.assertIsInstance(config.spark, SparkConfig)
        self.assertIsInstance(config.s3, S3Config)
        self.assertIsInstance(config.mysql, MySQLConfig)
        
        self.assertEqual(config.environment, "local")
        self.assertEqual(config.spark.app_name, "TagSystem-Local")


if __name__ == "__main__":
    # 运行测试
    unittest.main(verbosity=2)