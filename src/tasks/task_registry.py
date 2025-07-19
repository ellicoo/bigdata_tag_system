"""
标签任务注册模块 - 自动注册所有标签任务
"""

import logging
from .task_factory import TagTaskFactory

# 导入所有标签任务
from .wealth import HighNetWorthUserTask, CashRichUserTask
from .behavior import ActiveTraderTask, RecentActiveUserTask
from .demographic import YoungUserTask
from .lifecycle import NewUserTask, VIPUserTask
from .risk import LowRiskUserTask

logger = logging.getLogger(__name__)


def register_all_tasks():
    """注册所有标签任务到工厂"""
    
    # 标签任务映射表
    task_mappings = {
        1: HighNetWorthUserTask,     # 高净值用户
        2: ActiveTraderTask,         # 活跃交易者
        3: LowRiskUserTask,         # 低风险用户
        4: NewUserTask,             # 新注册用户
        5: VIPUserTask,             # VIP客户
        6: CashRichUserTask,        # 现金充足用户
        7: YoungUserTask,           # 年轻用户
        8: RecentActiveUserTask,    # 最近活跃用户
    }
    
    # 批量注册任务
    for tag_id, task_class in task_mappings.items():
        try:
            TagTaskFactory.register_task(tag_id, task_class)
        except Exception as e:
            logger.error(f"❌ 注册标签任务失败 {tag_id}: {task_class.__name__} - {str(e)}")
    
    # 输出注册结果
    registry_info = TagTaskFactory.get_registry_info()
    logger.info(f"✅ 标签任务注册完成 - 总计: {registry_info['total_tasks']} 个任务")
    logger.info(f"📋 已注册标签ID: {registry_info['registered_tag_ids']}")
    
    return registry_info


def get_task_summary():
    """获取所有任务的摘要信息"""
    summary = {}
    available_tasks = TagTaskFactory.get_all_available_tasks()
    
    for tag_id, task_class_name in available_tasks.items():
        try:
            # 创建临时配置用于获取元数据
            temp_config = {
                'tag_id': tag_id,
                'tag_name': f'Task_{tag_id}',
                'tag_category': 'unknown'
            }
            
            task_instance = TagTaskFactory.create_task(tag_id, temp_config)
            metadata = task_instance.get_task_metadata()
            
            summary[tag_id] = {
                'task_class': task_class_name,
                'required_fields': metadata['required_fields'],
                'data_sources': metadata['data_sources']
            }
            
        except Exception as e:
            logger.warning(f"⚠️ 获取任务 {tag_id} 元数据失败: {str(e)}")
            
    return summary


# 自动注册（导入时执行）
if __name__ != "__main__":
    register_all_tasks()