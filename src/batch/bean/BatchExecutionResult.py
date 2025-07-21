"""
批处理执行结果Bean - 统一返回结果格式
封装批处理执行的结果，包括DataFrame、执行统计、元数据等
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field
from pyspark.sql import DataFrame

from src.batch.bean.TaskExecutionContext import TaskExecutionContext


@dataclass
class BatchExecutionResult:
    """
    批处理执行结果数据模型
    用于统一返回批处理任务的执行结果和统计信息
    """
    resultDataFrame: DataFrame                          # 最终结果DataFrame
    success: bool = True                               # 执行是否成功
    
    # 执行统计
    totalTasks: int = 0                                # 总任务数
    successfulTasks: int = 0                          # 成功任务数
    failedTasks: int = 0                              # 失败任务数
    totalUsers: int = 0                               # 总用户数
    
    # 时间统计
    startTime: Optional[datetime] = None               # 开始时间
    endTime: Optional[datetime] = None                 # 结束时间
    
    # 任务详情
    taskContexts: List[TaskExecutionContext] = field(default_factory=list)  # 任务执行上下文列表
    errorMessages: List[str] = field(default_factory=list)                  # 错误信息列表
    
    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)                  # 额外元数据
    
    def getDuration(self) -> float:
        """获取执行时长（秒）"""
        if self.startTime is None:
            return 0.0
        
        endTime = self.endTime or datetime.now()
        return (endTime - self.startTime).total_seconds()
    
    def getSuccessRate(self) -> float:
        """获取任务成功率"""
        if self.totalTasks == 0:
            return 0.0
        return (self.successfulTasks / self.totalTasks) * 100
    
    def addTaskContext(self, context: TaskExecutionContext):
        """添加任务执行上下文"""
        self.taskContexts.append(context)
        
        # 更新统计
        self.totalTasks = len(self.taskContexts)
        self.successfulTasks = sum(1 for ctx in self.taskContexts if ctx.isCompleted())
        self.failedTasks = sum(1 for ctx in self.taskContexts if ctx.isFailed())
    
    def addError(self, errorMessage: str):
        """添加错误信息"""
        timestamp = datetime.now().isoformat()
        self.errorMessages.append(f"[{timestamp}] {errorMessage}")
    
    def markStart(self):
        """标记开始执行"""
        self.startTime = datetime.now()
    
    def markEnd(self, success: bool = True):
        """标记执行结束"""
        self.endTime = datetime.now()
        self.success = success
    
    def getTaskSummary(self) -> Dict[str, Any]:
        """获取任务执行摘要"""
        completedTasks = [ctx for ctx in self.taskContexts if ctx.isCompleted()]
        failedTasks = [ctx for ctx in self.taskContexts if ctx.isFailed()]
        
        # 统计各任务的用户数
        taskUserStats = []
        for ctx in self.taskContexts:
            taskUserStats.append({
                'tag_id': ctx.tagId,
                'tag_name': ctx.tagName,
                'status': ctx.status.value,
                'tagged_users': ctx.taggedUsers,
                'duration': ctx.getDuration()
            })
        
        return {
            'total_tasks': self.totalTasks,
            'successful_tasks': self.successfulTasks,
            'failed_tasks': self.failedTasks,
            'success_rate': self.getSuccessRate(),
            'total_users': self.totalUsers,
            'total_duration': self.getDuration(),
            'task_details': taskUserStats,
            'completed_tasks': [ctx.tagId for ctx in completedTasks],
            'failed_tasks': [ctx.tagId for ctx in failedTasks]
        }
    
    def isValid(self) -> bool:
        """验证结果是否有效"""
        return (
            self.resultDataFrame is not None and
            self.totalTasks > 0 and
            self.successfulTasks > 0
        )
    
    def toDict(self) -> Dict[str, Any]:
        """转换为字典格式（不包含DataFrame）"""
        return {
            'success': self.success,
            'total_tasks': self.totalTasks,
            'successful_tasks': self.successfulTasks,
            'failed_tasks': self.failedTasks,
            'total_users': self.totalUsers,
            'start_time': self.startTime.isoformat() if self.startTime else None,
            'end_time': self.endTime.isoformat() if self.endTime else None,
            'duration_seconds': self.getDuration(),
            'success_rate': self.getSuccessRate(),
            'task_summary': self.getTaskSummary(),
            'error_messages': self.errorMessages,
            'metadata': self.metadata
        }
    
    @classmethod
    def createEmpty(cls) -> 'BatchExecutionResult':
        """创建空的执行结果"""
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
        
        spark = SparkSession.getActiveSession()
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("tag_ids", ArrayType(IntegerType()), False)
        ])
        emptyDataFrame = spark.createDataFrame([], schema)
        
        return cls(
            resultDataFrame=emptyDataFrame,
            success=False
        )
    
    @classmethod
    def createFromDataFrame(cls, dataFrame: DataFrame, totalUsers: int = None) -> 'BatchExecutionResult':
        """从DataFrame创建执行结果"""
        result = cls(resultDataFrame=dataFrame)
        result.totalUsers = totalUsers or dataFrame.count()
        result.success = True
        return result
    
    def __str__(self) -> str:
        return f"BatchExecutionResult(success={self.success}, tasks={self.totalTasks}, users={self.totalUsers})"
    
    def __repr__(self) -> str:
        return self.__str__()