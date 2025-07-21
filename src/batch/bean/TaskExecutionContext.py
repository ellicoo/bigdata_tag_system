"""
任务执行上下文 - Bean类
封装任务执行时的上下文信息，包括配置、状态、统计等
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"           # 待执行
    RUNNING = "running"           # 执行中
    COMPLETED = "completed"       # 已完成
    FAILED = "failed"            # 失败
    CANCELLED = "cancelled"       # 已取消


@dataclass
class TaskExecutionContext:
    """
    任务执行上下文数据模型
    封装任务执行过程中的状态和统计信息
    """
    taskId: str                              # 任务ID
    tagId: int                              # 标签ID
    tagName: str                            # 标签名称
    status: TaskStatus = TaskStatus.PENDING  # 任务状态
    startTime: Optional[datetime] = None     # 开始时间
    endTime: Optional[datetime] = None       # 结束时间
    userFilter: Optional[List[str]] = None   # 用户过滤列表
    
    # 执行统计
    totalUsers: int = 0                      # 总用户数
    taggedUsers: int = 0                     # 命中标签的用户数
    errorCount: int = 0                      # 错误次数
    
    # 执行信息
    errorMessages: List[str] = field(default_factory=list)  # 错误信息列表
    executionLogs: List[str] = field(default_factory=list)  # 执行日志
    metadata: Dict[str, Any] = field(default_factory=dict)  # 元数据
    
    def startExecution(self):
        """开始执行"""
        self.status = TaskStatus.RUNNING
        self.startTime = datetime.now()
        self.addLog(f"任务 {self.taskId} 开始执行")
    
    def completeExecution(self, taggedUsers: int = 0):
        """完成执行"""
        self.status = TaskStatus.COMPLETED
        self.endTime = datetime.now()
        self.taggedUsers = taggedUsers
        duration = self.getDuration()
        self.addLog(f"任务 {self.taskId} 执行完成，耗时 {duration:.2f}秒，标签用户 {taggedUsers} 个")
    
    def failExecution(self, errorMessage: str):
        """执行失败"""
        self.status = TaskStatus.FAILED
        self.endTime = datetime.now()
        self.addError(errorMessage)
        duration = self.getDuration()
        self.addLog(f"任务 {self.taskId} 执行失败，耗时 {duration:.2f}秒")
    
    def cancelExecution(self):
        """取消执行"""
        self.status = TaskStatus.CANCELLED
        self.endTime = datetime.now()
        self.addLog(f"任务 {self.taskId} 已取消")
    
    def addError(self, errorMessage: str):
        """添加错误信息"""
        self.errorMessages.append(f"[{datetime.now().isoformat()}] {errorMessage}")
        self.errorCount += 1
    
    def addLog(self, logMessage: str):
        """添加执行日志"""
        self.executionLogs.append(f"[{datetime.now().isoformat()}] {logMessage}")
    
    def getDuration(self) -> float:
        """获取执行时长（秒）"""
        if self.startTime is None:
            return 0.0
        
        endTime = self.endTime or datetime.now()
        return (endTime - self.startTime).total_seconds()
    
    def getSuccessRate(self) -> float:
        """获取成功率"""
        if self.totalUsers == 0:
            return 0.0
        return (self.taggedUsers / self.totalUsers) * 100
    
    def isRunning(self) -> bool:
        """是否正在运行"""
        return self.status == TaskStatus.RUNNING
    
    def isCompleted(self) -> bool:
        """是否已完成"""
        return self.status == TaskStatus.COMPLETED
    
    def isFailed(self) -> bool:
        """是否失败"""
        return self.status == TaskStatus.FAILED
    
    def toDict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'task_id': self.taskId,
            'tag_id': self.tagId,
            'tag_name': self.tagName,
            'status': self.status.value,
            'start_time': self.startTime.isoformat() if self.startTime else None,
            'end_time': self.endTime.isoformat() if self.endTime else None,
            'duration_seconds': self.getDuration(),
            'user_filter': self.userFilter,
            'total_users': self.totalUsers,
            'tagged_users': self.taggedUsers,
            'success_rate': self.getSuccessRate(),
            'error_count': self.errorCount,
            'error_messages': self.errorMessages,
            'execution_logs': self.executionLogs,
            'metadata': self.metadata
        }
    
    @classmethod
    def createForTask(cls, taskId: str, tagId: int, tagName: str, 
                     userFilter: Optional[List[str]] = None) -> 'TaskExecutionContext':
        """为任务创建执行上下文"""
        return cls(
            taskId=taskId,
            tagId=tagId,
            tagName=tagName,
            userFilter=userFilter
        )
    
    def __str__(self) -> str:
        return f"TaskExecutionContext(task={self.taskId}, status={self.status.value})"
    
    def __repr__(self) -> str:
        return self.__str__()