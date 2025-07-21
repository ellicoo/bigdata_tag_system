"""
任务注册器 - 重构为驼峰命名风格
自动注册系统中的所有标签任务，建立标签ID与任务类的映射关系
"""

import logging
from typing import Dict, Any, List

from src.batch.tasks.base.TagTaskFactory import TagTaskFactory

logger = logging.getLogger(__name__)


def registerAllTasks():
    """注册所有标签任务"""
    try:
        logger.info("📋 开始注册所有标签任务...")
        
        # 标签任务映射表
        taskMappings = {
            # 财富类标签
            1: None,  # HighNetWorthUserTask (高净值用户)
            6: None,  # CashRichUserTask (现金充足用户)
            
            # 行为类标签
            2: None,  # ActiveTraderTask (活跃交易者)
            8: None,  # RecentActiveUserTask (最近活跃用户)
            
            # 人口统计类标签
            7: None,  # YoungUserTask (年轻用户)
            
            # 生命周期类标签
            4: None,  # NewUserTask (新注册用户)
            5: None,  # VIPUserTask (VIP客户)
            
            # 风险类标签
            3: None,  # LowRiskUserTask (低风险用户)
        }
        
        # 动态导入并注册任务类
        registeredCount = 0
        
        # 财富类标签
        try:
            from src.batch.tasks.wealth.HighNetWorthUserTask import HighNetWorthUserTask
            TagTaskFactory.registerTask(1, HighNetWorthUserTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 HighNetWorthUserTask: {str(e)}")
        
        try:
            from src.batch.tasks.wealth.CashRichUserTask import CashRichUserTask
            TagTaskFactory.registerTask(6, CashRichUserTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 CashRichUserTask: {str(e)}")
        
        # 行为类标签
        try:
            from src.batch.tasks.behavior.ActiveTraderTask import ActiveTraderTask
            TagTaskFactory.registerTask(2, ActiveTraderTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 ActiveTraderTask: {str(e)}")
        
        try:
            from src.batch.tasks.behavior.RecentActiveTask import RecentActiveUserTask
            TagTaskFactory.registerTask(8, RecentActiveUserTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 RecentActiveUserTask: {str(e)}")
        
        # 人口统计类标签
        try:
            from src.batch.tasks.demographic.YoungUserTask import YoungUserTask
            TagTaskFactory.registerTask(7, YoungUserTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 YoungUserTask: {str(e)}")
        
        # 生命周期类标签
        try:
            from src.batch.tasks.lifecycle.NewUserTask import NewUserTask
            TagTaskFactory.registerTask(4, NewUserTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 NewUserTask: {str(e)}")
        
        try:
            from src.batch.tasks.lifecycle.VIPUserTask import VIPUserTask
            TagTaskFactory.registerTask(5, VIPUserTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 VIPUserTask: {str(e)}")
        
        # 风险类标签
        try:
            from src.batch.tasks.risk.LowRiskUserTask import LowRiskUserTask
            TagTaskFactory.registerTask(3, LowRiskUserTask)
            registeredCount += 1
        except ImportError as e:
            logger.warning(f"⚠️ 无法导入 LowRiskUserTask: {str(e)}")
        
        logger.info(f"✅ 任务注册完成: 成功注册 {registeredCount} 个任务")
        
        # 显示注册详情
        registeredTasks = TagTaskFactory.getRegisteredTasks()
        for tagId, taskClass in registeredTasks.items():
            logger.info(f"   📝 标签 {tagId}: {taskClass.__name__}")
        
        return registeredCount
        
    except Exception as e:
        logger.error(f"❌ 注册所有任务失败: {str(e)}")
        return 0


def registerTasksByCategory(category: str) -> int:
    """
    按类别注册任务
    
    Args:
        category: 任务类别 (wealth, behavior, demographic, lifecycle, risk)
        
    Returns:
        int: 注册的任务数量
    """
    try:
        logger.info(f"📋 注册 {category} 类别的任务...")
        
        registeredCount = 0
        
        if category == 'wealth':
            # 财富类标签
            try:
                from src.batch.tasks.wealth.HighNetWorthUserTask import HighNetWorthUserTask
                TagTaskFactory.registerTask(1, HighNetWorthUserTask)
                registeredCount += 1
            except ImportError:
                pass
            
            try:
                from src.batch.tasks.wealth.CashRichUserTask import CashRichUserTask
                TagTaskFactory.registerTask(6, CashRichUserTask)
                registeredCount += 1
            except ImportError:
                pass
                
        elif category == 'behavior':
            # 行为类标签
            try:
                from src.batch.tasks.behavior.ActiveTraderTask import ActiveTraderTask
                TagTaskFactory.registerTask(2, ActiveTraderTask)
                registeredCount += 1
            except ImportError:
                pass
            
            try:
                from src.batch.tasks.behavior.RecentActiveTask import RecentActiveUserTask
                TagTaskFactory.registerTask(8, RecentActiveUserTask)
                registeredCount += 1
            except ImportError:
                pass
                
        elif category == 'demographic':
            # 人口统计类标签
            try:
                from src.batch.tasks.demographic.YoungUserTask import YoungUserTask
                TagTaskFactory.registerTask(7, YoungUserTask)
                registeredCount += 1
            except ImportError:
                pass
                
        elif category == 'lifecycle':
            # 生命周期类标签
            try:
                from src.batch.tasks.lifecycle.NewUserTask import NewUserTask
                TagTaskFactory.registerTask(4, NewUserTask)
                registeredCount += 1
            except ImportError:
                pass
            
            try:
                from src.batch.tasks.lifecycle.VIPUserTask import VIPUserTask
                TagTaskFactory.registerTask(5, VIPUserTask)
                registeredCount += 1
            except ImportError:
                pass
                
        elif category == 'risk':
            # 风险类标签
            try:
                from src.batch.tasks.risk.LowRiskUserTask import LowRiskUserTask
                TagTaskFactory.registerTask(3, LowRiskUserTask)
                registeredCount += 1
            except ImportError:
                pass
        
        logger.info(f"✅ {category} 类别任务注册完成: {registeredCount} 个任务")
        return registeredCount
        
    except Exception as e:
        logger.error(f"❌ 注册 {category} 类别任务失败: {str(e)}")
        return 0


def getTaskMappings() -> Dict[int, str]:
    """
    获取标签ID到任务类名的映射
    
    Returns:
        Dict[int, str]: {标签ID: 任务类名} 的映射
    """
    return {
        1: 'HighNetWorthUserTask',    # 高净值用户
        2: 'ActiveTraderTask',        # 活跃交易者
        3: 'LowRiskUserTask',        # 低风险用户
        4: 'NewUserTask',            # 新注册用户
        5: 'VIPUserTask',            # VIP客户
        6: 'CashRichUserTask',       # 现金充足用户
        7: 'YoungUserTask',          # 年轻用户
        8: 'RecentActiveUserTask',   # 最近活跃用户
    }


def getTaskCategories() -> Dict[str, List[int]]:
    """
    获取任务类别到标签ID列表的映射
    
    Returns:
        Dict[str, List[int]]: {类别: [标签ID列表]} 的映射
    """
    return {
        'wealth': [1, 6],        # 财富类
        'behavior': [2, 8],      # 行为类
        'demographic': [7],      # 人口统计类
        'lifecycle': [4, 5],     # 生命周期类
        'risk': [3],            # 风险类
    }


def validateTaskRegistration() -> Dict[str, Any]:
    """
    验证任务注册状态
    
    Returns:
        Dict[str, Any]: 验证结果
    """
    try:
        logger.info("🔍 验证任务注册状态...")
        
        expectedTasks = getTaskMappings()
        registeredTasks = TagTaskFactory.getRegisteredTasks()
        
        validationResult = {
            'overall_status': 'success',
            'expected_tasks': len(expectedTasks),
            'registered_tasks': len(registeredTasks),
            'missing_tasks': [],
            'extra_tasks': [],
            'category_status': {}
        }
        
        # 检查缺失的任务
        missingTaskIds = set(expectedTasks.keys()) - set(registeredTasks.keys())
        validationResult['missing_tasks'] = list(missingTaskIds)
        
        # 检查额外的任务
        extraTaskIds = set(registeredTasks.keys()) - set(expectedTasks.keys())
        validationResult['extra_tasks'] = list(extraTaskIds)
        
        # 按类别验证
        categories = getTaskCategories()
        for category, tagIds in categories.items():
            registeredInCategory = sum(1 for tagId in tagIds if tagId in registeredTasks)
            validationResult['category_status'][category] = {
                'expected': len(tagIds),
                'registered': registeredInCategory,
                'missing': [tagId for tagId in tagIds if tagId not in registeredTasks]
            }
        
        # 判断整体状态
        if missingTaskIds:
            validationResult['overall_status'] = 'incomplete'
        
        logger.info(f"📊 任务注册验证完成: {validationResult['overall_status']}")
        return validationResult
        
    except Exception as e:
        logger.error(f"❌ 验证任务注册失败: {str(e)}")
        return {
            'overall_status': 'error',
            'error': str(e)
        }


# 模块加载时自动注册所有任务
try:
    registerAllTasks()
except Exception as e:
    logger.warning(f"⚠️ 模块加载时自动注册任务失败: {str(e)}")
    logger.info("ℹ️ 可以稍后手动调用 registerAllTasks() 进行注册")