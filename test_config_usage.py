#!/usr/bin/env python3
"""
配置使用示例 - 展示新config模块的用法
演示驼峰命名风格的配置管理
"""

import sys
import os
import logging

# 添加项目路径到系统路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.batch.config import ConfigManager, ConfigFactory, SparkConfig, S3Config, MySQLConfig

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def testConfigManager():
    """测试配置管理器"""
    logger.info("🧪 测试配置管理器功能...")
    
    try:
        # 测试加载本地配置
        config = ConfigManager.loadConfig('local')
        logger.info(f"✅ 加载配置成功: {config}")
        
        # 测试配置验证
        isValid = ConfigManager.validateCurrentConfig()
        logger.info(f"📊 配置验证结果: {isValid}")
        
        # 测试配置摘要
        summary = ConfigManager.getConfigSummary()
        logger.info(f"📋 配置摘要: {summary}")
        
        # 测试支持的环境
        supportedEnvs = ConfigManager.getSupportedEnvironments()
        logger.info(f"🌍 支持的环境: {supportedEnvs}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试配置管理器失败: {str(e)}")
        return False


def testConfigFactory():
    """测试配置工厂"""
    logger.info("🧪 测试配置工厂功能...")
    
    try:
        # 测试创建自定义配置
        sparkConfig = SparkConfig(
            appName="TestApp",
            master="local[2]",
            executorMemory="1g",
            driverMemory="512m"
        )
        
        s3Config = S3Config(
            bucket="test-bucket",
            accessKey="test-key",
            secretKey="test-secret",
            endpoint="http://localhost:9000"
        )
        
        mysqlConfig = MySQLConfig(
            host="localhost",
            port=3306,
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        customConfig = ConfigFactory.createCustomConfig(
            environment="test",
            sparkConfig=sparkConfig,
            s3Config=s3Config,
            mysqlConfig=mysqlConfig,
            batchSize=5000,
            enableCache=False
        )
        
        logger.info(f"✅ 自定义配置创建成功: {customConfig}")
        
        # 测试配置转换
        configDict = customConfig.toDict()
        logger.info(f"📊 配置字典: {configDict}")
        
        # 测试从字典创建配置
        configFromDict = ConfigFactory.createConfigFromDict(configDict)
        logger.info(f"🔄 从字典创建配置成功: {configFromDict}")
        
        # 测试配置模板
        template = ConfigFactory.getDefaultConfigTemplate('local')
        logger.info(f"📋 本地配置模板: {template}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试配置工厂失败: {str(e)}")
        return False


def testConfigProperties():
    """测试配置属性和方法"""
    logger.info("🧪 测试配置属性和方法...")
    
    try:
        # 创建测试配置
        config = ConfigFactory.createLocalConfig() if hasattr(ConfigFactory, 'createLocalConfig') else None
        if not config:
            # 如果没有LocalConfig，创建自定义配置
            config = ConfigFactory.createCustomConfig(
                environment="test",
                sparkConfig=SparkConfig(),
                s3Config=S3Config(bucket="test"),
                mysqlConfig=MySQLConfig(host="localhost")
            )
        
        # 测试Spark配置
        sparkConfigDict = config.spark.toDict()
        logger.info(f"⚡ Spark配置: {sparkConfigDict}")
        
        # 测试S3配置
        s3SparkConfig = config.s3.toSparkConfig()
        logger.info(f"🗄️ S3 Spark配置: {s3SparkConfig}")
        
        # 测试MySQL配置
        jdbcUrl = config.mysql.jdbcUrl
        connectionProps = config.mysql.connectionProperties
        logger.info(f"🗃️ JDBC URL: {jdbcUrl}")
        logger.info(f"🔗 连接属性: {connectionProps}")
        
        # 测试完整Spark配置
        fullSparkConfig = config.getSparkConfig()
        logger.info(f"🚀 完整Spark配置: {len(fullSparkConfig)} 项配置")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试配置属性失败: {str(e)}")
        return False


def testConfigUsage():
    """测试配置模块的完整用法"""
    logger.info("🚀 开始测试配置模块...")
    
    allTestsPassed = True
    
    # 测试1: 配置管理器
    logger.info("\n" + "="*50)
    logger.info("测试1: 配置管理器")
    logger.info("="*50)
    test1Result = testConfigManager()
    allTestsPassed = allTestsPassed and test1Result
    logger.info(f"测试1结果: {'✅ 通过' if test1Result else '❌ 失败'}")
    
    # 测试2: 配置工厂
    logger.info("\n" + "="*50)
    logger.info("测试2: 配置工厂")
    logger.info("="*50)
    test2Result = testConfigFactory()
    allTestsPassed = allTestsPassed and test2Result
    logger.info(f"测试2结果: {'✅ 通过' if test2Result else '❌ 失败'}")
    
    # 测试3: 配置属性和方法
    logger.info("\n" + "="*50)
    logger.info("测试3: 配置属性和方法")
    logger.info("="*50)
    test3Result = testConfigProperties()
    allTestsPassed = allTestsPassed and test3Result
    logger.info(f"测试3结果: {'✅ 通过' if test3Result else '❌ 失败'}")
    
    # 总结
    logger.info("\n" + "="*60)
    logger.info("🎯 配置模块测试总结")
    logger.info("="*60)
    logger.info(f"总体结果: {'✅ 所有测试通过' if allTestsPassed else '❌ 部分测试失败'}")
    logger.info("🔧 新配置模块特性:")
    logger.info("   📁 重构位置: src/batch/config/")
    logger.info("   🐪 驼峰命名: ConfigManager, BaseConfig, SparkConfig")
    logger.info("   🏗️ 配置工厂: 支持多种配置创建方式")
    logger.info("   📦 Bean设计: 使用dataclass提供类型安全")
    logger.info("   🔄 向后兼容: 保持原有功能的同时优化设计")
    
    return allTestsPassed


if __name__ == "__main__":
    try:
        success = testConfigUsage()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 测试被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 测试执行异常: {str(e)}")
        sys.exit(1)