"""
端到端集成测试
"""

import unittest
import os
import sys
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config.manager import ConfigManager
from src.scheduler.main_scheduler import TagComputeScheduler


class TestEndToEndIntegration(unittest.TestCase):
    """端到端集成测试"""
    
    @classmethod
    def setUpClass(cls):
        """设置测试环境"""
        cls.spark = SparkSession.builder \
            .appName("TestIntegration") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """清理测试环境"""
        cls.spark.stop()
    
    def setUp(self):
        """设置每个测试的环境"""
        # 创建测试数据
        self.create_test_data()
        
        # 创建配置
        self.config = ConfigManager.load_config('local')
    
    def create_test_data(self):
        """创建测试数据"""
        # 用户基础信息
        user_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("user_level", StringType(), True),
            StructField("registration_date", DateType(), True)
        ])
        
        user_data = [
            ("user_001", "verified", "VIP2", "2024-01-15"),
            ("user_002", "verified", "VIP1", "2024-02-20"),
            ("user_003", "pending", "VIP3", "2024-03-10"),
            ("user_004", "verified", "VIP1", "2024-04-05"),
            ("user_005", "verified", "VIP3", "2024-05-12")
        ]
        
        self.user_df = self.spark.createDataFrame(user_data, user_schema)
        
        # 用户资产信息
        asset_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("total_asset_value", IntegerType(), True),
            StructField("cash_balance", IntegerType(), True)
        ])
        
        asset_data = [
            ("user_001", 150000, 50000),
            ("user_002", 50000, 20000),
            ("user_003", 200000, 100000),
            ("user_004", 80000, 30000),
            ("user_005", 300000, 150000)
        ]
        
        self.asset_df = self.spark.createDataFrame(asset_data, asset_schema)
        
        # 标签规则（模拟从MySQL读取的数据）
        self.tag_rules = [
            {
                "tag_id": 1,
                "tag_name": "高净值用户",
                "tag_category": "资产等级",
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
                "is_active": True
            },
            {
                "tag_id": 2,
                "tag_name": "VIP客户",
                "tag_category": "客户等级",
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
                "is_active": True
            },
            {
                "tag_id": 3,
                "tag_name": "现金充足",
                "tag_category": "资产结构",
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
                "is_active": True
            }
        ]
    
    @patch('src.readers.rule_reader.RuleReader.read_active_rules')
    @patch('src.readers.hive_reader.HiveDataReader.read_user_data')
    @patch('src.writers.mysql_writer.MySQLWriter.write_user_tags')
    def test_full_tag_compute_workflow(self, mock_write_tags, mock_read_data, mock_read_rules):
        """测试完整的标签计算工作流"""
        
        # 模拟数据读取
        mock_read_rules.return_value = self.tag_rules
        mock_read_data.return_value = {
            'user_basic_info': self.user_df,
            'user_asset_summary': self.asset_df
        }
        mock_write_tags.return_value = True
        
        # 创建调度器
        scheduler = TagComputeScheduler(self.config)
        scheduler.spark = self.spark  # 使用测试的Spark会话
        
        # 执行完整标签计算
        result = scheduler.run_full_tag_compute()
        
        # 验证结果
        self.assertTrue(result)
        
        # 验证方法调用
        mock_read_rules.assert_called_once()
        mock_read_data.assert_called_once()
        mock_write_tags.assert_called()
    
    @patch('src.readers.rule_reader.RuleReader.read_specific_rules')
    @patch('src.readers.hive_reader.HiveDataReader.read_user_data')
    @patch('src.writers.mysql_writer.MySQLWriter.write_user_tags')
    def test_specific_tags_workflow(self, mock_write_tags, mock_read_data, mock_read_rules):
        """测试指定标签计算工作流"""
        
        # 只返回指定的标签规则
        specific_rules = [rule for rule in self.tag_rules if rule['tag_id'] in [1, 2]]
        
        mock_read_rules.return_value = specific_rules
        mock_read_data.return_value = {
            'user_basic_info': self.user_df,
            'user_asset_summary': self.asset_df
        }
        mock_write_tags.return_value = True
        
        # 创建调度器
        scheduler = TagComputeScheduler(self.config)
        scheduler.spark = self.spark
        
        # 执行指定标签计算
        result = scheduler.run_specific_tags([1, 2])
        
        # 验证结果
        self.assertTrue(result)
        
        # 验证只读取了指定的标签
        mock_read_rules.assert_called_once_with([1, 2])
    
    @patch('src.readers.rule_reader.RuleReader.read_active_rules')
    @patch('src.readers.hive_reader.HiveDataReader.read_incremental_data')
    @patch('src.writers.mysql_writer.MySQLWriter.write_user_tags')
    def test_incremental_compute_workflow(self, mock_write_tags, mock_read_data, mock_read_rules):
        """测试增量计算工作流"""
        
        mock_read_rules.return_value = self.tag_rules
        mock_read_data.return_value = {
            'user_basic_info': self.user_df.limit(2),  # 模拟增量数据
            'user_asset_summary': self.asset_df.limit(2)
        }
        mock_write_tags.return_value = True
        
        # 创建调度器
        scheduler = TagComputeScheduler(self.config)
        scheduler.spark = self.spark
        
        # 执行增量计算
        result = scheduler.run_incremental_compute(days_back=3)
        
        # 验证结果
        self.assertTrue(result)
        
        # 验证调用了增量数据读取
        mock_read_data.assert_called_once_with(3)
    
    @patch('src.readers.rule_reader.RuleReader.health_check')
    @patch('src.readers.hive_reader.HiveDataReader.health_check')
    @patch('src.writers.mysql_writer.MySQLWriter.health_check')
    def test_health_check_workflow(self, mock_writer_check, mock_reader_check, mock_rule_check):
        """测试健康检查工作流"""
        
        # 模拟健康检查通过
        mock_rule_check.return_value = True
        mock_reader_check.return_value = True
        mock_writer_check.return_value = True
        
        # 创建调度器
        scheduler = TagComputeScheduler(self.config)
        
        # 执行健康检查
        result = scheduler.health_check()
        
        # 验证结果
        self.assertTrue(result)
        
        # 验证所有组件都进行了健康检查
        mock_rule_check.assert_called_once()
        mock_reader_check.assert_called_once()
        mock_writer_check.assert_called_once()
    
    @patch('src.readers.rule_reader.RuleReader.health_check')
    def test_health_check_failure(self, mock_rule_check):
        """测试健康检查失败的情况"""
        
        # 模拟健康检查失败
        mock_rule_check.return_value = False
        
        # 创建调度器
        scheduler = TagComputeScheduler(self.config)
        
        # 执行健康检查
        result = scheduler.health_check()
        
        # 验证结果
        self.assertFalse(result)


class TestDataIntegration(unittest.TestCase):
    """数据集成测试"""
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestDataIntegration") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_data_join_operations(self):
        """测试数据关联操作"""
        # 创建测试数据
        user_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("kyc_status", StringType(), True)
        ])
        
        asset_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("total_asset_value", IntegerType(), True)
        ])
        
        user_data = [("user_001", "verified"), ("user_002", "pending")]
        asset_data = [("user_001", 150000), ("user_002", 50000)]
        
        user_df = self.spark.createDataFrame(user_data, user_schema)
        asset_df = self.spark.createDataFrame(asset_data, asset_schema)
        
        # 测试数据关联
        joined_df = user_df.join(asset_df, "user_id", "inner")
        
        # 验证关联结果
        self.assertEqual(joined_df.count(), 2)
        self.assertIn("kyc_status", joined_df.columns)
        self.assertIn("total_asset_value", joined_df.columns)
    
    def test_data_filtering(self):
        """测试数据过滤操作"""
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("total_asset_value", IntegerType(), True)
        ])
        
        data = [
            ("user_001", 150000),
            ("user_002", 50000),
            ("user_003", 200000)
        ]
        
        df = self.spark.createDataFrame(data, schema)
        
        # 测试过滤
        filtered_df = df.filter(df.total_asset_value >= 100000)
        
        # 验证过滤结果
        self.assertEqual(filtered_df.count(), 2)
        
        result_values = [row.total_asset_value for row in filtered_df.collect()]
        self.assertIn(150000, result_values)
        self.assertIn(200000, result_values)


if __name__ == "__main__":
    unittest.main(verbosity=2)