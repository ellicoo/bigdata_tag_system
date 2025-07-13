"""
配置模块单元测试
"""

import unittest
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.config.base import SparkConfig, S3Config, MySQLConfig
from src.config.manager import ConfigManager


class TestSparkConfig(unittest.TestCase):
    """测试Spark配置"""
    
    def test_default_config(self):
        """测试默认配置"""
        config = SparkConfig()
        
        self.assertEqual(config.app_name, "TagSystem")
        self.assertEqual(config.master, "local[*]")
        
    def test_custom_config(self):
        """测试自定义配置"""
        config = SparkConfig(
            app_name="TestApp",
            master="yarn",
            executor_memory="4g",
            driver_memory="2g"
        )
        
        self.assertEqual(config.app_name, "TestApp")
        self.assertEqual(config.master, "yarn")
        self.assertEqual(config.executor_memory, "4g")
        self.assertEqual(config.driver_memory, "2g")
    
    def test_to_dict(self):
        """测试转换为字典"""
        config = SparkConfig(app_name="TestApp")
        config_dict = config.to_dict()
        
        self.assertIn("spark.app.name", config_dict)
        self.assertEqual(config_dict["spark.app.name"], "TestApp")


class TestS3Config(unittest.TestCase):
    """测试S3配置"""
    
    def test_default_config(self):
        """测试默认配置"""
        config = S3Config()
        
        self.assertEqual(config.bucket, "tag-system-data-lake")
        self.assertIsNone(config.endpoint)
    
    def test_minio_config(self):
        """测试MinIO配置"""
        config = S3Config(
            bucket="test-bucket",
            access_key="testkey",
            secret_key="testsecret", 
            endpoint="http://localhost:9000"
        )
        
        self.assertEqual(config.bucket, "test-bucket")
        self.assertEqual(config.endpoint, "http://localhost:9000")


class TestMySQLConfig(unittest.TestCase):
    """测试MySQL配置"""
    
    def test_default_config(self):
        """测试默认配置"""
        config = MySQLConfig()
        
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 3306)
        self.assertEqual(config.database, "tag_system")
    
    def test_jdbc_url(self):
        """测试JDBC URL生成"""
        config = MySQLConfig(
            host="testhost",
            port=3307,
            database="testdb"
        )
        
        expected_url = "jdbc:mysql://testhost:3307/testdb?useSSL=false&allowPublicKeyRetrieval=true"
        self.assertEqual(config.jdbc_url, expected_url)
    
    def test_connection_properties(self):
        """测试连接属性"""
        config = MySQLConfig(
            username="testuser",
            password="testpass"
        )
        
        props = config.connection_properties
        self.assertEqual(props["user"], "testuser")
        self.assertEqual(props["password"], "testpass")
        self.assertEqual(props["driver"], "com.mysql.cj.jdbc.Driver")


class TestConfigManager(unittest.TestCase):
    """测试配置管理器"""
    
    def test_load_local_config(self):
        """测试加载本地配置"""
        config = ConfigManager.load_config('local')
        
        self.assertEqual(config.environment, "local")
        self.assertIsInstance(config.spark, SparkConfig)
        self.assertIsInstance(config.s3, S3Config)
        self.assertIsInstance(config.mysql, MySQLConfig)
    
    def test_load_glue_dev_config(self):
        """测试加载Glue开发配置"""
        config = ConfigManager.load_config('glue-dev')
        
        self.assertEqual(config.environment, "glue-dev")
        self.assertEqual(config.spark.master, "yarn")
    
    def test_load_glue_prod_config(self):
        """测试加载Glue生产配置"""
        config = ConfigManager.load_config('glue-prod')
        
        self.assertEqual(config.environment, "glue-prod")
        self.assertEqual(config.spark.master, "yarn")
    
    def test_invalid_environment(self):
        """测试无效环境"""
        with self.assertRaises((ImportError, ModuleNotFoundError)):
            ConfigManager.load_config('invalid-env')


if __name__ == "__main__":
    unittest.main(verbosity=2)