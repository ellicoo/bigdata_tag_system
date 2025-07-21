"""
批处理标签执行器 - 重构为驼峰命名风格
整合原有的TaskBasedParallelEngine功能，支持全量和指定标签的任务化计算
实现并行任务执行和结果合并
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

from src.batch.config.BaseConfig import BaseConfig
from src.batch.bean.TaskExecutionContext import TaskExecutionContext
from src.batch.bean.TagResult import TagResult
from src.batch.bean.UserTagsSummary import UserTagsSummary
from src.batch.bean.HiveTableMeta import HiveTableMeta
from src.batch.bean.BatchExecutionResult import BatchExecutionResult

logger = logging.getLogger(__name__)


class BatchTagExecutor:
    """批处理标签执行器（原TaskBasedParallelEngine功能）"""
    
    def __init__(self, systemConfig: BaseConfig, spark: SparkSession):
        self.systemConfig = systemConfig
        self.spark = spark
        
        # 延迟初始化的组件
        self._taskFactory = None
        self._ruleReader = None
        self._hiveReader = None
        
        self.logger = logging.getLogger(__name__)
    
    def executeTagTasksInParallel(self, tagIds: List[int], 
                                 userFilter: Optional[List[str]] = None) -> 'BatchExecutionResult':
        """
        并行执行多个标签任务的完整流程
        1. 启动同一个Spark会话
        2. 并行读取S3业务数据
        3. 从缓存中取出MySQL标签规则
        4. 使用标签规则生成SQL并执行标签计算
        5. 内存合并各任务结果
        6. 返回包含执行统计的结果Bean
        
        Args:
            tagIds: 要执行的标签ID列表
            userFilter: 可选的用户ID过滤列表
            
        Returns:
            BatchExecutionResult: 包含结果DataFrame和执行统计的Bean对象
        """
        # 创建执行结果Bean
        executionResult = BatchExecutionResult.createEmpty()
        executionResult.markStart()
        
        try:
            self.logger.info(f"🚀 开始并行执行 {len(tagIds)} 个标签任务: {tagIds}")
            
            # 1. 初始化组件
            self._initializeComponents()
            
            # 2. 预缓存MySQL标签规则（所有任务共享）
            rulesCache = self._precacheTagRules(tagIds)
            
            # 3. 并行执行各个标签任务
            taskResults = self._executeTagTasksInParallel(tagIds, userFilter, rulesCache)
            
            if not taskResults:
                self.logger.warning("⚠️ 没有任务产生结果")
                executionResult.markEnd(success=False)
                return executionResult
            
            # 4. 内存合并各任务结果
            mergedResult = self._mergeTaskResultsInMemory(taskResults)
            
            # 5. 更新执行结果
            executionResult.resultDataFrame = mergedResult
            executionResult.totalUsers = mergedResult.count()
            executionResult.markEnd(success=True)
            
            # 6. 记录执行统计
            self._logParallelExecutionStatistics(tagIds, taskResults, mergedResult)
            
            return executionResult
            
        except Exception as e:
            executionResult.addError(str(e))
            executionResult.markEnd(success=False)
            self.logger.error(f"❌ 并行标签任务执行失败: {str(e)}")
            return executionResult
    
    def _initializeComponents(self):
        """初始化组件"""
        try:
            if self._taskFactory is None:
                from src.batch.tasks.base.TagTaskFactory import TagTaskFactory
                self._taskFactory = TagTaskFactory()
            
            if self._ruleReader is None:
                from src.batch.utils.RuleReader import RuleReader
                self._ruleReader = RuleReader(self.spark, self.systemConfig.mysql)
            
            if self._hiveReader is None:
                from src.batch.utils.HiveDataReader import HiveDataReader
                self._hiveReader = HiveDataReader(self.spark, self.systemConfig.s3)
                
        except Exception as e:
            self.logger.error(f"❌ 组件初始化失败: {str(e)}")
            raise
    
    def _precacheTagRules(self, tagIds: List[int]) -> DataFrame:
        """
        预缓存MySQL标签规则（所有任务共享）
        
        Args:
            tagIds: 标签ID列表
            
        Returns:
            DataFrame: 缓存的规则数据
        """
        try:
            self.logger.info(f"📚 预缓存 {len(tagIds)} 个标签规则...")
            
            # 获取所有活跃规则（如果已缓存则直接使用缓存）
            allRulesDataFrame = self._ruleReader.getActiveRulesDataFrame()
            
            # 过滤出需要的标签规则
            filteredRules = allRulesDataFrame.filter(col("tag_id").isin(tagIds))
            
            # 强制缓存到内存和磁盘
            from pyspark import StorageLevel
            cachedRules = filteredRules.persist(StorageLevel.MEMORY_AND_DISK)
            
            # 触发缓存
            ruleCount = cachedRules.count()
            self.logger.info(f"✅ 成功缓存 {ruleCount} 条标签规则")
            
            # 验证规则完整性
            for tagId in tagIds:
                rule = cachedRules.filter(col("tag_id") == tagId).first()
                if rule is None:
                    self.logger.warning(f"⚠️ 标签 {tagId} 没有对应的规则，将跳过")
            
            return cachedRules
            
        except Exception as e:
            self.logger.error(f"❌ 预缓存标签规则失败: {str(e)}")
            raise
    
    def _executeTagTasksInParallel(self, tagIds: List[int], 
                                  userFilter: Optional[List[str]], 
                                  rulesCache: DataFrame) -> List[DataFrame]:
        """
        并行执行各个标签任务
        
        Args:
            tagIds: 标签ID列表
            userFilter: 用户过滤列表
            rulesCache: 缓存的规则数据
            
        Returns:
            List[DataFrame]: 各任务的执行结果
        """
        try:
            self.logger.info(f"⚡ 并行执行 {len(tagIds)} 个标签任务...")
            
            # 创建任务结果收集器
            from src.batch.utils.TaskResultCollector import TaskResultCollector
            resultCollector = TaskResultCollector()
            
            # 创建共享的业务数据读取器
            from src.batch.utils.HiveDataReader import HiveDataReader
            hiveReader = HiveDataReader(self.spark, self.systemConfig.s3)
            
            # 为每个标签创建并执行任务
            for i, tagId in enumerate(tagIds, 1):
                # 创建任务执行上下文
                taskId = f"tag_{tagId}_{i}"
                executionContext = None
                
                try:
                    self.logger.info(f"🎯 执行任务 {i}/{len(tagIds)}: 标签 {tagId}")
                    
                    # 从缓存中获取规则
                    ruleRow = rulesCache.filter(col("tag_id") == tagId).first()
                    if ruleRow is None:
                        self.logger.warning(f"⚠️ 标签 {tagId} 规则缺失，跳过")
                        continue
                    
                    # 创建任务执行上下文
                    executionContext = TaskExecutionContext.createForTask(
                        taskId=taskId,
                        tagId=tagId,
                        tagName=ruleRow['tag_name'],
                        userFilter=userFilter
                    )
                    executionContext.startExecution()
                    
                    # 构建任务配置
                    taskConfig = {
                        'tag_id': tagId,
                        'tag_name': ruleRow['tag_name'],
                        'tag_category': ruleRow['tag_category'],
                        'rule_conditions': ruleRow['rule_conditions']
                    }
                    
                    # 创建任务实例
                    task = self._taskFactory.createTask(tagId, taskConfig, self.spark, self.systemConfig)
                    if task is None:
                        if executionContext:
                            executionContext.failExecution("任务创建失败")
                        self.logger.warning(f"⚠️ 标签 {tagId} 任务创建失败，跳过")
                        continue
                    
                    # 执行任务（这里使用共享的Spark会话和数据读取器）
                    taskResult = task.execute(userFilter)
                    
                    # 使用结果收集器收集结果
                    if taskResult is not None and taskResult.count() > 0:
                        taggedUserCount = taskResult.count()
                        resultCollector.addTaskResult(tagId, ruleRow['tag_name'], taskResult)
                        
                        # 更新执行上下文
                        if executionContext:
                            executionContext.completeExecution(taggedUserCount)
                        
                        self.logger.info(f"✅ 标签 {tagId} 任务完成: {taggedUserCount} 个用户")
                    else:
                        if executionContext:
                            executionContext.completeExecution(0)
                        self.logger.info(f"📊 标签 {tagId} 任务无匹配用户")
                        
                except Exception as e:
                    if executionContext:
                        executionContext.failExecution(str(e))
                    self.logger.error(f"❌ 标签 {tagId} 任务执行失败: {str(e)}")
                    continue
            
            # 验证和收集结果
            isValid, errors = resultCollector.validateAllResults()
            if not isValid:
                self.logger.warning(f"⚠️ 结果验证发现问题: {errors}")
            
            # 记录详细统计
            stats = resultCollector.getDetailedStatistics()
            self.logger.info(f"📊 任务执行统计: {stats}")
            
            # 获取所有结果
            allResults = resultCollector.collectAllResults()
            
            self.logger.info(f"✅ 并行任务执行完成，成功 {len(allResults)} / {len(tagIds)} 个任务")
            return allResults
            
        except Exception as e:
            self.logger.error(f"❌ 并行任务执行失败: {str(e)}")
            return []
    
    def _mergeTaskResultsInMemory(self, taskResults: List[DataFrame]) -> DataFrame:
        """
        内存合并各任务结果
        
        Args:
            taskResults: 各任务的DataFrame结果列表
            
        Returns:
            DataFrame: 合并后的结果 (user_id, tag_ids)
        """
        try:
            self.logger.info(f"🧩 内存合并 {len(taskResults)} 个任务结果...")
            
            if not taskResults:
                return self._createEmptyResult()
            
            # 使用批处理结果合并器
            from src.batch.utils.BatchResultMerger import BatchResultMerger
            resultMerger = BatchResultMerger(self.systemConfig.mysql)
            
            # 合并多个标签结果
            mergedResult = resultMerger.mergeMultipleTagResults(taskResults)
            
            mergedUserCount = mergedResult.count()
            self.logger.info(f"✅ 内存合并完成: {mergedUserCount} 个用户")
            
            return mergedResult
            
        except Exception as e:
            self.logger.error(f"❌ 内存合并失败: {str(e)}")
            raise
    
    def _createEmptyResult(self) -> DataFrame:
        """
        创建空结果DataFrame（兼容性方法）
        
        Returns:
            DataFrame: 空的标签结果DataFrame
        """
        try:
            from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
            
            schema = StructType([
                StructField("user_id", StringType(), False),
                StructField("tag_ids", ArrayType(IntegerType()), False)
            ])
            
            emptyResult = self.spark.createDataFrame([], schema)
            self.logger.info("📝 创建空结果DataFrame")
            
            return emptyResult
            
        except Exception as e:
            self.logger.error(f"❌ 创建空结果失败: {str(e)}")
            raise
    
    def _logParallelExecutionStatistics(self, tagIds: List[int], 
                                        taskResults: List[DataFrame], 
                                        mergedResult: DataFrame):
        """
        记录并行执行统计信息
        
        Args:
            tagIds: 计划执行的标签ID列表
            taskResults: 各任务的执行结果
            mergedResult: 合并后的最终结果
        """
        try:
            totalPlannedTags = len(tagIds)
            successfulTasks = len(taskResults)
            finalUserCount = mergedResult.count()
            
            # 计算各任务的用户数
            taskUserCounts = []
            totalTaskUsers = 0
            
            for i, result in enumerate(taskResults):
                try:
                    userCount = result.count()
                    taskUserCounts.append(userCount)
                    totalTaskUsers += userCount
                except Exception as e:
                    self.logger.warning(f"⚠️ 统计第 {i+1} 个任务结果失败: {str(e)}")
                    taskUserCounts.append(0)
            
            # 记录详细统计
            self.logger.info("📊 并行执行统计:")
            self.logger.info(f"   🎯 计划执行标签: {totalPlannedTags} 个")
            self.logger.info(f"   ✅ 成功执行任务: {successfulTasks} 个")
            self.logger.info(f"   👥 任务用户总数: {totalTaskUsers} 个")
            self.logger.info(f"   🎯 最终合并用户: {finalUserCount} 个")
            self.logger.info(f"   📈 执行成功率: {round(successfulTasks/totalPlannedTags*100, 1)}%")
            
            # 各任务详细统计
            for i, (tagId, userCount) in enumerate(zip(tagIds[:len(taskUserCounts)], taskUserCounts)):
                self.logger.info(f"   📋 标签 {tagId}: {userCount} 用户")
                
        except Exception as e:
            self.logger.warning(f"⚠️ 记录并行执行统计失败: {str(e)}")
    
    def _createTaskInstances(self, tagIds: List[int]) -> List[Any]:
        """
        创建任务实例
        
        Args:
            tagIds: 标签ID列表
            
        Returns:
            List[BaseTagTask]: 任务实例列表
        """
        try:
            self.logger.info(f"🔧 创建 {len(tagIds)} 个任务实例...")
            
            # 获取标签规则数据
            rulesDataFrame = self._ruleReader.getActiveRulesDataFrame()
            
            tasks = []
            for tagId in tagIds:
                try:
                    # 获取标签配置
                    ruleRow = rulesDataFrame.filter(col("tag_id") == tagId).first()
                    if ruleRow is None:
                        self.logger.warning(f"⚠️ 标签 {tagId} 没有找到对应规则，跳过")
                        continue
                    
                    # 构建任务配置
                    taskConfig = {
                        'tag_id': tagId,
                        'tag_name': ruleRow['tag_name'],
                        'tag_category': ruleRow['tag_category'],
                        'rule_conditions': ruleRow['rule_conditions']
                    }
                    
                    # 创建任务实例
                    task = self._taskFactory.createTask(tagId, taskConfig, self.spark, self.systemConfig)
                    if task:
                        tasks.append(task)
                        self.logger.info(f"✅ 任务创建成功: {task.tagName}")
                    else:
                        self.logger.warning(f"⚠️ 标签 {tagId} 任务创建失败，跳过")
                        
                except Exception as e:
                    self.logger.error(f"❌ 创建标签 {tagId} 任务失败: {str(e)}")
                    continue
            
            self.logger.info(f"✅ 成功创建 {len(tasks)} 个任务实例")
            return tasks
            
        except Exception as e:
            self.logger.error(f"❌ 创建任务实例失败: {str(e)}")
            return []
    
    def _executeTasksInParallel(self, tasks: List[Any], 
                               userFilter: Optional[List[str]] = None) -> List[DataFrame]:
        """
        并行执行任务
        
        Args:
            tasks: 任务列表
            userFilter: 用户过滤列表
            
        Returns:
            List[DataFrame]: 执行结果列表
        """
        try:
            self.logger.info(f"⚡ 开始并行执行 {len(tasks)} 个任务...")
            
            results = []
            
            # 由于PySpark的限制，这里采用顺序执行但内部并行的方式
            # 每个任务内部使用Spark的分布式并行能力
            for i, task in enumerate(tasks, 1):
                try:
                    self.logger.info(f"🔄 执行任务 {i}/{len(tasks)}: {task.tagName}")
                    
                    # 执行单个任务
                    taskResult = task.execute(userFilter)
                    
                    if taskResult is not None and taskResult.count() > 0:
                        results.append(taskResult)
                        self.logger.info(f"✅ 任务 {task.tagName} 完成: {taskResult.count()} 个用户")
                    else:
                        self.logger.info(f"📊 任务 {task.tagName} 没有匹配用户")
                        
                except Exception as e:
                    self.logger.error(f"❌ 任务 {task.tagName} 执行失败: {str(e)}")
                    continue
            
            self.logger.info(f"✅ 并行任务执行完成，成功 {len(results)} / {len(tasks)} 个任务")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 并行任务执行失败: {str(e)}")
            return []
    
    def _logExecutionStatistics(self, tagIds: List[int], results: List[DataFrame]):
        """
        记录执行统计信息
        
        Args:
            tagIds: 标签ID列表
            results: 结果列表
        """
        try:
            totalTaggedUsers = 0
            taskStats = []
            
            for result in results:
                try:
                    userCount = result.count()
                    totalTaggedUsers += userCount
                    
                    # 获取标签ID（假设结果中包含tag_id列）
                    if 'tag_id' in result.columns:
                        tagId = result.select("tag_id").first()['tag_id']
                        taskStats.append({
                            'tag_id': tagId,
                            'user_count': userCount
                        })
                        
                except Exception as e:
                    self.logger.warning(f"⚠️ 统计单个结果失败: {str(e)}")
                    continue
            
            # 记录统计信息
            self.logger.info("📊 标签执行统计:")
            self.logger.info(f"   🎯 计划执行标签: {len(tagIds)} 个")
            self.logger.info(f"   ✅ 成功执行标签: {len(results)} 个")
            self.logger.info(f"   👥 总计标签用户: {totalTaggedUsers} 个")
            
            # 详细统计
            for stat in taskStats:
                self.logger.info(f"   📋 标签 {stat['tag_id']}: {stat['user_count']} 用户")
                
        except Exception as e:
            self.logger.warning(f"⚠️ 记录执行统计失败: {str(e)}")
    
    def executeWithMySQLMerge(self, tagIds: List[int], 
                             userFilter: Optional[List[str]] = None) -> BatchExecutionResult:
        """
        执行标签任务并与MySQL现有标签合并
        
        Args:
            tagIds: 要执行的标签ID列表
            userFilter: 可选的用户ID过滤列表
            
        Returns:
            BatchExecutionResult: 包含MySQL合并后结果的执行结果Bean
        """
        try:
            self.logger.info(f"🔄 执行标签任务并合并MySQL现有标签: {tagIds}")
            
            # 执行并行任务
            executionResult = self.executeTagTasksInParallel(tagIds, userFilter)
            
            if not executionResult.success or executionResult.resultDataFrame.count() == 0:
                self.logger.warning("⚠️ 没有新标签结果需要合并")
                executionResult.metadata['mysql_merge'] = False
                return executionResult
            
            # 与MySQL现有标签合并
            from src.batch.utils.BatchResultMerger import BatchResultMerger
            resultMerger = BatchResultMerger(self.systemConfig.mysql)
            
            finalResult = resultMerger.mergeWithExistingTags(executionResult.resultDataFrame)
            
            # 更新执行结果
            executionResult.resultDataFrame = finalResult
            executionResult.totalUsers = finalResult.count()
            executionResult.metadata['mysql_merge'] = True
            
            self.logger.info(f"✅ MySQL标签合并完成: {executionResult.totalUsers} 个用户")
            
            return executionResult
            
        except Exception as e:
            errorResult = BatchExecutionResult.createEmpty()
            errorResult.addError(str(e))
            errorResult.markEnd(success=False)
            self.logger.error(f"❌ 标签任务与MySQL合并失败: {str(e)}")
            return errorResult
    
    def executeSingleTag(self, tagId: int, 
                        userFilter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        执行单个标签任务（兼容性方法，返回DataFrame）
        
        Args:
            tagId: 标签ID
            userFilter: 可选的用户ID过滤列表
            
        Returns:
            DataFrame: 标签计算结果，失败时返回None
        """
        try:
            self.logger.info(f"🎯 执行单个标签任务: {tagId}")
            
            # 使用Bean方法执行
            executionResult = self.executeTagTasksInParallel([tagId], userFilter)
            
            if executionResult.success and executionResult.resultDataFrame.count() > 0:
                return executionResult.resultDataFrame
            else:
                self.logger.warning(f"⚠️ 标签 {tagId} 没有计算结果")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 单个标签任务执行失败: {str(e)}")
            return None
    
    def validateTaskExecution(self, tagIds: List[int]) -> Dict[str, Any]:
        """
        验证任务执行能力
        
        Args:
            tagIds: 要验证的标签ID列表
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        try:
            self.logger.info(f"🔍 验证任务执行能力: {tagIds}")
            
            validationResult = {
                'overall_status': 'success',
                'tag_validation': {},
                'statistics': {}
            }
            
            # 初始化组件
            self._initializeComponents()
            
            # 验证每个标签
            validTasks = 0
            for tagId in tagIds:
                try:
                    # 尝试创建任务
                    tasks = self._createTaskInstances([tagId])
                    if tasks:
                        validationResult['tag_validation'][tagId] = 'valid'
                        validTasks += 1
                    else:
                        validationResult['tag_validation'][tagId] = 'invalid'
                        
                except Exception as e:
                    validationResult['tag_validation'][tagId] = f'error: {str(e)}'
            
            # 统计结果
            validationResult['statistics'] = {
                'total_tags': len(tagIds),
                'valid_tags': validTasks,
                'invalid_tags': len(tagIds) - validTasks
            }
            
            if validTasks == 0:
                validationResult['overall_status'] = 'failed'
            elif validTasks < len(tagIds):
                validationResult['overall_status'] = 'partial'
            
            self.logger.info(f"📊 验证完成: {validationResult['overall_status']}")
            return validationResult
            
        except Exception as e:
            self.logger.error(f"❌ 验证任务执行能力失败: {str(e)}")
            return {
                'overall_status': 'error',
                'error': str(e)
            }
    
    def getExecutorStatistics(self) -> Dict[str, Any]:
        """
        获取执行器统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        try:
            stats = {
                'spark_version': self.spark.version,
                'spark_app_name': self.spark.sparkContext.appName,
                'spark_master': self.spark.sparkContext.master,
                'environment': self.systemConfig.environment
            }
            
            # 获取可用标签数
            if self._ruleReader:
                ruleStats = self._ruleReader.getStatistics()
                stats.update(ruleStats)
            
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ 获取执行器统计失败: {str(e)}")
            return {}
    
    def cleanup(self):
        """清理执行器资源"""
        try:
            self.logger.info("🧹 清理标签执行器资源...")
            
            # 清理规则读取器缓存
            if self._ruleReader:
                self._ruleReader.clearCache()
            
            # 清理任务工厂
            if self._taskFactory:
                self._taskFactory.clearRegistry()
            
            # 重置组件引用
            self._ruleReader = None
            self._taskFactory = None
            self._hiveReader = None
            
            self.logger.info("✅ 标签执行器资源清理完成")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 清理执行器资源异常: {str(e)}")
    
    def __str__(self) -> str:
        return f"BatchTagExecutor(env={self.systemConfig.environment})"
    
    def __repr__(self) -> str:
        return self.__str__()