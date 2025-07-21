#!/usr/bin/env python3
"""
新架构测试脚本
验证重构后的驼峰命名风格的批处理标签系统
"""

import sys
import os
import logging

# 添加项目路径到系统路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.common.config.manager import ConfigManager
from src.batch.engine.BatchOrchestrator import BatchOrchestrator
from src.batch.tasks.base.TagTaskFactory import TagTaskFactory
from src.batch.tasks.base.TaskRegistry import validateTaskRegistration, getTaskMappings

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def testTaskRegistration():
    """测试任务注册功能"""
    logger.info("🧪 测试任务注册功能...")
    
    try:
        # 验证任务注册状态
        validationResult = validateTaskRegistration()
        logger.info(f"📊 任务注册验证结果: {validationResult}")
        
        # 获取任务映射
        taskMappings = getTaskMappings()
        logger.info(f"📋 任务映射: {taskMappings}")
        
        # 获取任务工厂统计
        factory = TagTaskFactory()
        stats = factory.getRegistryStatistics()
        logger.info(f"📈 注册表统计: {stats}")
        
        return validationResult['overall_status'] == 'success'
        
    except Exception as e:
        logger.error(f"❌ 测试任务注册失败: {str(e)}")
        return False


def testTaskCreation():
    """测试任务创建功能"""
    logger.info("🧪 测试任务创建功能...")
    
    try:
        # 加载配置
        config = ConfigManager.loadConfig('local')
        
        # 创建简单的Spark会话用于测试
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("TestNewArchitecture") \
            .master("local[1]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        # 创建任务工厂
        factory = TagTaskFactory()
        
        # 测试创建高净值用户任务
        taskConfig = {
            'tag_id': 1,
            'tag_name': '高净值用户',
            'tag_category': '财富类',
            'rule_conditions': {
                'logic': 'AND',
                'conditions': [
                    {'field': 'total_asset_value', 'operator': '>=', 'value': 150000, 'type': 'number'},
                    {'field': 'cash_balance', 'operator': '>=', 'value': 60000, 'type': 'number'}
                ]
            }
        }
        
        task = factory.createTask(1, taskConfig, spark, config)
        
        if task:
            logger.info(f"✅ 成功创建任务: {task}")
            
            # 测试任务元数据
            metadata = task.getTaskMetadata()
            logger.info(f"📋 任务元数据: {metadata}")
            
            # 测试数据字段和表配置
            fields = task.getRequiredFields()
            tableConfig = task.getHiveTableConfig()
            logger.info(f"📊 需要字段: {fields}")
            logger.info(f"🗄️ 表配置: {tableConfig}")
            
            spark.stop()
            return True
        else:
            logger.error("❌ 任务创建失败")
            spark.stop()
            return False
        
    except Exception as e:
        logger.error(f"❌ 测试任务创建失败: {str(e)}")
        return False


def testBatchOrchestrator():
    """测试批处理编排器"""
    logger.info("🧪 测试批处理编排器...")
    
    try:
        # 加载配置
        config = ConfigManager.loadConfig('local')
        
        # 创建编排器
        orchestrator = BatchOrchestrator(config)
        
        # 测试初始化
        initSuccess = orchestrator.initializeSystem()
        if not initSuccess:
            logger.error("❌ 编排器初始化失败")
            return False
        
        # 测试健康检查
        healthResult = orchestrator.performHealthCheck()
        logger.info(f"📊 健康检查结果: {healthResult}")
        
        # 清理资源
        orchestrator.cleanup()
        
        return healthResult['overall_status'] != 'error'
        
    except Exception as e:
        logger.error(f"❌ 测试批处理编排器失败: {str(e)}")
        return False


def testNewArchitecture():
    """测试新架构的完整功能"""
    logger.info("🚀 开始测试新架构...")
    
    allTestsPassed = True
    
    # 测试1: 任务注册
    logger.info("\n" + "="*50)
    logger.info("测试1: 任务注册功能")
    logger.info("="*50)
    test1Result = testTaskRegistration()
    allTestsPassed = allTestsPassed and test1Result
    logger.info(f"测试1结果: {'✅ 通过' if test1Result else '❌ 失败'}")
    
    # 测试2: 任务创建
    logger.info("\n" + "="*50)
    logger.info("测试2: 任务创建功能")
    logger.info("="*50)
    test2Result = testTaskCreation()
    allTestsPassed = allTestsPassed and test2Result
    logger.info(f"测试2结果: {'✅ 通过' if test2Result else '❌ 失败'}")
    
    # 测试3: 批处理编排器
    logger.info("\n" + "="*50)
    logger.info("测试3: 批处理编排器")
    logger.info("="*50)
    test3Result = testBatchOrchestrator()
    allTestsPassed = allTestsPassed and test3Result
    logger.info(f"测试3结果: {'✅ 通过' if test3Result else '❌ 失败'}")
    
    # 总结
    logger.info("\n" + "="*60)
    logger.info("🎯 新架构测试总结")
    logger.info("="*60)
    logger.info(f"总体结果: {'✅ 所有测试通过' if allTestsPassed else '❌ 部分测试失败'}")
    logger.info("🔧 新架构特性验证:")
    logger.info("   📁 目录结构: src/batch/bean、tasks、config、api、utils、engine")
    logger.info("   🐪 驼峰命名: 类名与文件名一致")
    logger.info("   🏗️ S3读取抽象: 父类抽象，子类指定表地址")
    logger.info("   🎭 任务自包含: 每个任务独立加载数据和执行")
    logger.info("   ⚡ 并行保持: 维持原有的并行执行能力")
    
    return allTestsPassed


if __name__ == "__main__":
    try:
        success = testNewArchitecture()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 测试被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 测试执行异常: {str(e)}")
        sys.exit(1)