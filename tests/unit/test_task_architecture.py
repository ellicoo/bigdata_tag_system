#!/usr/bin/env python3
"""
测试任务化标签架构
"""

import sys
import os
import logging

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config.manager import ConfigManager
from src.scheduler.tag_scheduler import TagScheduler


def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)


def test_task_listing():
    """测试任务列表功能"""
    logger = setup_logging()
    logger.info("🧪 测试任务列表功能")
    
    try:
        # 加载配置
        config = ConfigManager.load_config('local')
        
        # 创建调度器
        scheduler = TagScheduler(config)
        scheduler.initialize()
        
        # 获取可用任务
        available_tasks = scheduler.get_available_tasks()
        logger.info(f"📋 可用任务数量: {len(available_tasks)}")
        
        for tag_id, task_class in available_tasks.items():
            logger.info(f"  🏷️  {tag_id}: {task_class}")
        
        # 获取任务摘要
        task_summary = scheduler.get_task_summary()
        logger.info(f"📊 任务摘要数量: {len(task_summary)}")
        
        # 显示第一个任务的详细信息
        if task_summary:
            first_task_id = list(task_summary.keys())[0]
            summary = task_summary[first_task_id]
            logger.info(f"""
🔍 示例任务详情 (ID: {first_task_id}):
   📝 任务类: {summary['task_class']}
   📊 必需字段: {summary['required_fields']}
   🗂️  数据源: {summary['data_sources']}
            """)
        
        scheduler.cleanup()
        logger.info("✅ 任务列表测试完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 任务列表测试失败: {str(e)}")
        return False


def test_task_execution():
    """测试任务执行功能"""
    logger = setup_logging()
    logger.info("🧪 测试任务执行功能")
    
    try:
        # 加载配置
        config = ConfigManager.load_config('local')
        
        # 创建调度器
        scheduler = TagScheduler(config)
        scheduler.initialize()
        
        # 测试任务化指定用户指定标签计算
        test_user_ids = ['user_000001', 'user_000002']
        test_tag_ids = [1, 5]  # 高净值用户 + VIP客户
        
        logger.info(f"🎯 测试任务化计算 - 用户: {test_user_ids}, 标签: {test_tag_ids}")
        
        success = scheduler.scenario_task_users_parallel(test_user_ids, test_tag_ids)
        
        scheduler.cleanup()
        
        if success:
            logger.info("✅ 任务执行测试完成")
            return True
        else:
            logger.error("❌ 任务执行测试失败")
            return False
        
    except Exception as e:
        logger.error(f"❌ 任务执行测试异常: {str(e)}")
        return False


def test_task_factory():
    """测试任务工厂功能"""
    logger = setup_logging()
    logger.info("🧪 测试任务工厂功能")
    
    try:
        from src.tasks.task_factory import TagTaskFactory
        from src.tasks.task_registry import register_all_tasks
        
        # 注册所有任务
        register_all_tasks()
        
        # 测试工厂方法
        registry_info = TagTaskFactory.get_registry_info()
        logger.info(f"📋 工厂注册信息: {registry_info}")
        
        # 测试创建任务实例
        test_config = {
            'tag_id': 1,
            'tag_name': '高净值用户',
            'tag_category': '用户价值'
        }
        
        task_instance = TagTaskFactory.create_task(1, test_config)
        logger.info(f"🏷️  创建任务实例: {task_instance}")
        
        # 获取任务元数据
        metadata = task_instance.get_task_metadata()
        logger.info(f"📊 任务元数据: {metadata}")
        
        logger.info("✅ 任务工厂测试完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 任务工厂测试失败: {str(e)}")
        return False


def main():
    """主测试函数"""
    logger = setup_logging()
    
    logger.info("🚀 开始任务化架构测试")
    logger.info("=" * 60)
    
    test_results = []
    
    # 测试1: 任务工厂
    logger.info("🧪 测试1: 任务工厂功能")
    test_results.append(("任务工厂", test_task_factory()))
    
    # 测试2: 任务列表
    logger.info("🧪 测试2: 任务列表功能")
    test_results.append(("任务列表", test_task_listing()))
    
    # 测试3: 任务执行
    logger.info("🧪 测试3: 任务执行功能")
    test_results.append(("任务执行", test_task_execution()))
    
    # 汇总结果
    logger.info("=" * 60)
    logger.info("📊 测试结果汇总:")
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results:
        status = "✅ 通过" if result else "❌ 失败"
        logger.info(f"  {test_name}: {status}")
        if result:
            passed += 1
        else:
            failed += 1
    
    logger.info(f"""
🎯 总结:
   ✅ 通过: {passed} 个测试
   ❌ 失败: {failed} 个测试
   📊 成功率: {passed/(passed+failed)*100:.1f}%
    """)
    
    if failed == 0:
        logger.info("🎉 所有测试通过！任务化架构工作正常。")
        return 0
    else:
        logger.error("⚠️ 部分测试失败，请检查日志。")
        return 1


if __name__ == "__main__":
    sys.exit(main())