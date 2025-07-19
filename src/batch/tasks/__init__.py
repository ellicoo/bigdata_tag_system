"""
标签任务模块 - 实现标签任务的抽象化和分布式开发支持
"""

from .base_tag_task import BaseTagTask
from .task_factory import TagTaskFactory

__all__ = ['BaseTagTask', 'TagTaskFactory']