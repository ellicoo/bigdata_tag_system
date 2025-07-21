"""
批处理编排器 - 重构为驼峰命名风格
系统的核心编排器，整合原有TagScheduler的功能，协调所有组件
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel

from src.batch.config.ConfigManager import ConfigManager
from src.batch.config.BaseConfig import BaseConfig

logger = logging.getLogger(__name__)


class BatchOrchestrator:
    """批处理编排器（原TagScheduler功能）"""
    
    def __init__(self, systemConfig: BaseConfig):
        self.systemConfig = systemConfig
        self.spark = None
        
        # 核心组件（延迟初始化）
        self._dataLoader = None
        self._tagExecutor = None
        self._resultMerger = None
        self._dataWriter = None
        
        # 缓存
        self._existingTagsCache = None
        
        self.logger = logging.getLogger(__name__)
    
    def initializeSystem(self) -> bool:
        """
        初始化系统组件
        
        Returns:
            bool: 初始化是否成功
        """
        try:
            self.logger.info("🚀 开始初始化批处理标签系统...")
            
            # 1. 初始化Spark会话
            self._initializeSpark()
            
            # 2. 初始化核心组件
            self._initializeCoreComponents()
            
            # 3. 验证系统健康状态
            if not self._validateSystemHealth():
                raise Exception("系统健康检查失败")
            
            self.logger.info("✅ 批处理标签系统初始化完成")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 系统初始化失败: {str(e)}")
            return False
    
    def _initializeSpark(self):
        """初始化Spark会话"""
        try:
            self.logger.info("⚡ 初始化Spark会话...")
            
            sparkConfig = self.systemConfig.spark
            
            builder = SparkSession.builder \
                .appName(f"BatchTagSystem-{self.systemConfig.environment}") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            # 添加JAR配置（如果存在）
            if hasattr(sparkConfig, 'jars') and sparkConfig.jars:
                builder = builder.config("spark.jars", sparkConfig.jars)
                self.logger.info(f"📦 加载JAR文件: {sparkConfig.jars}")
            
            # 环境特定配置
            if self.systemConfig.environment == 'local':
                builder = builder.master("local[*]") \
                    .config("spark.sql.shuffle.partitions", "4") \
                    .config("spark.driver.memory", "2g") \
                    .config("spark.executor.memory", "2g")
            else:
                # Glue环境配置
                builder = builder \
                    .config("spark.sql.shuffle.partitions", "200") \
                    .config("spark.sql.adaptive.advisory.partitionSizeInBytes", "128MB")
            
            # S3配置（如果需要）
            if hasattr(self.systemConfig, 's3') and self.systemConfig.s3:
                s3Config = self.systemConfig.s3
                builder = builder \
                    .config("spark.hadoop.fs.s3a.access.key", s3Config.accessKey) \
                    .config("spark.hadoop.fs.s3a.secret.key", s3Config.secretKey) \
                    .config("spark.hadoop.fs.s3a.endpoint", s3Config.endpoint) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info(f"✅ Spark会话初始化完成: {self.spark.version}")
            
        except Exception as e:
            self.logger.error(f"❌ Spark初始化失败: {str(e)}")
            raise
    
    def _initializeCoreComponents(self):
        """初始化核心组件"""
        try:
            self.logger.info("🔧 初始化核心组件...")
            
            # 数据加载器
            from src.batch.utils.HiveDataReader import HiveDataReader
            from src.batch.utils.RuleReader import RuleReader
            self._hiveReader = HiveDataReader(self.spark, self.systemConfig.s3)
            self._ruleReader = RuleReader(self.spark, self.systemConfig.mysql)
            
            # 标签执行器
            from src.batch.engine.BatchTagExecutor import BatchTagExecutor
            self._tagExecutor = BatchTagExecutor(self.systemConfig, self.spark)
            
            # 结果合并器
            from src.batch.utils.BatchResultMerger import BatchResultMerger
            self._resultMerger = BatchResultMerger(self.systemConfig.mysql)
            
            # 数据写入器
            from src.batch.utils.BatchDataWriter import BatchDataWriter
            self._dataWriter = BatchDataWriter(self.systemConfig.mysql)
            
            self.logger.info("✅ 核心组件初始化完成")
            
        except Exception as e:
            self.logger.error(f"❌ 核心组件初始化失败: {str(e)}")
            raise
    
    def _validateSystemHealth(self) -> bool:
        """验证系统健康状态"""
        try:
            self.logger.info("🔍 执行系统健康检查...")
            
            # 1. 验证Spark会话
            if self.spark is None:
                self.logger.error("❌ Spark会话未初始化")
                return False
            
            # 2. 验证MySQL连接（只测试一次，避免重复日志）
            if not self._dataWriter.testConnection():
                self.logger.error("❌ MySQL连接失败")
                return False
            
            # 3. 跳过RuleReader的连接验证，复用上面的MySQL连接测试结果
            # MySQL连接已经验证成功，RuleReader使用相同的连接配置
            
            # 4. 基本功能测试
            testDataFrame = self.spark.range(1).toDF("test")
            if testDataFrame.count() != 1:
                self.logger.error("❌ Spark基本功能测试失败")
                return False
            
            self.logger.info("✅ 系统健康检查通过")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 系统健康检查失败: {str(e)}")
            return False
    
    def executeFullWorkflow(self, userFilter: Optional[List[str]] = None) -> bool:
        """
        执行全量标签计算工作流
        
        Args:
            userFilter: 可选的用户ID过滤列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            self.logger.info("🚀 开始执行全量标签计算工作流...")
            
            # 1. 预缓存现有标签
            self._precacheExistingTags()
            
            # 2. 获取所有活跃标签
            allTagIds = self._getAllActiveTagIds()
            if not allTagIds:
                self.logger.warning("⚠️ 没有找到活跃标签")
                return True
            
            # 3. 执行标签计算
            executionResult = self._tagExecutor.executeTagTasksInParallel(allTagIds, userFilter)
            tagResults = [executionResult.resultDataFrame] if executionResult.success else []
            
            if not tagResults:
                self.logger.warning("⚠️ 没有标签计算结果")
                return True
            
            # 4. 合并结果
            mergedResults = self._resultMerger.mergeMultipleTagResults(tagResults)
            
            # 5. 与现有标签合并
            finalResults = self._resultMerger.mergeWithExistingTags(
                mergedResults, self._existingTagsCache
            )
            
            # 6. 写入数据库
            writeSuccess = self._dataWriter.writeTaggedUsers(finalResults)
            
            if writeSuccess:
                self.logger.info("✅ 全量标签计算工作流执行完成")
                return True
            else:
                self.logger.error("❌ 数据写入失败")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 全量标签计算工作流失败: {str(e)}")
            return False
    
    def executeSpecificTagsWorkflow(self, tagIds: List[int], 
                                   userFilter: Optional[List[str]] = None) -> bool:
        """
        执行指定标签计算工作流
        
        Args:
            tagIds: 要计算的标签ID列表
            userFilter: 可选的用户ID过滤列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            self.logger.info(f"🚀 开始执行指定标签计算: {tagIds}")
            
            # 1. 预缓存现有标签
            self._precacheExistingTags()
            
            # 2. 验证标签ID有效性
            validTagIds = self._validateTagIds(tagIds)
            if not validTagIds:
                self.logger.warning("⚠️ 没有有效的标签ID")
                return True
            
            # 3. 执行标签计算
            executionResult = self._tagExecutor.executeTagTasksInParallel(validTagIds, userFilter)
            tagResults = [executionResult.resultDataFrame] if executionResult.success else []
            
            if not tagResults:
                self.logger.warning("⚠️ 没有标签计算结果")
                return True
            
            # 4. 合并结果
            mergedResults = self._resultMerger.mergeMultipleTagResults(tagResults)
            
            # 5. 与现有标签合并
            finalResults = self._resultMerger.mergeWithExistingTags(
                mergedResults, self._existingTagsCache
            )
            
            # 6. 写入数据库
            writeSuccess = self._dataWriter.writeTaggedUsers(finalResults)
            
            if writeSuccess:
                self.logger.info("✅ 指定标签计算工作流执行完成")
                return True
            else:
                self.logger.error("❌ 数据写入失败")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 指定标签计算工作流失败: {str(e)}")
            return False
    
    def executeSpecificUsersWorkflow(self, userIds: List[str], 
                                    tagIds: Optional[List[int]] = None) -> bool:
        """
        执行指定用户标签计算工作流
        
        Args:
            userIds: 用户ID列表
            tagIds: 可选的标签ID列表，None表示所有标签
            
        Returns:
            bool: 执行是否成功
        """
        try:
            self.logger.info(f"🚀 开始执行指定用户标签计算: {len(userIds)} 个用户")
            
            # 1. 预缓存现有标签
            self._precacheExistingTags()
            
            # 2. 确定要计算的标签
            targetTagIds = tagIds if tagIds else self._getAllActiveTagIds()
            if not targetTagIds:
                self.logger.warning("⚠️ 没有要计算的标签")
                return True
            
            # 3. 执行标签计算
            executionResult = self._tagExecutor.executeTagTasksInParallel(targetTagIds, userIds)
            tagResults = [executionResult.resultDataFrame] if executionResult.success else []
            
            if not tagResults:
                self.logger.warning("⚠️ 没有标签计算结果")
                return True
            
            # 4. 合并结果
            mergedResults = self._resultMerger.mergeMultipleTagResults(tagResults)
            
            # 5. 与现有标签合并
            finalResults = self._resultMerger.mergeWithExistingTags(
                mergedResults, self._existingTagsCache
            )
            
            # 6. 写入数据库
            writeSuccess = self._dataWriter.writeTaggedUsers(finalResults)
            
            if writeSuccess:
                self.logger.info("✅ 指定用户标签计算工作流执行完成")
                return True
            else:
                self.logger.error("❌ 数据写入失败")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 指定用户标签计算工作流失败: {str(e)}")
            return False
    
    def _precacheExistingTags(self):
        """预缓存现有标签"""
        try:
            if self._existingTagsCache is None:
                self.logger.info("📚 预缓存现有标签...")
                self._existingTagsCache = self._ruleReader.loadExistingUserTags()
                
                if self._existingTagsCache is not None:
                    # 缓存到内存和磁盘
                    self._existingTagsCache = self._existingTagsCache.persist(StorageLevel.MEMORY_AND_DISK)
                    cacheCount = self._existingTagsCache.count()
                    self.logger.info(f"✅ 现有标签预缓存完成: {cacheCount} 个用户")
                else:
                    self.logger.info("ℹ️ 没有现有标签数据（可能是首次运行）")
                    
        except Exception as e:
            self.logger.warning(f"⚠️ 预缓存现有标签失败: {str(e)}")
            self._existingTagsCache = None
    
    def _getAllActiveTagIds(self) -> List[int]:
        """获取所有活跃标签ID"""
        try:
            rulesDataFrame = self._ruleReader.getActiveRulesDataFrame()
            tagIds = [row['tag_id'] for row in rulesDataFrame.select("tag_id").distinct().collect()]
            
            self.logger.info(f"📋 获取到 {len(tagIds)} 个活跃标签: {tagIds}")
            return tagIds
            
        except Exception as e:
            self.logger.error(f"❌ 获取活跃标签ID失败: {str(e)}")
            return []
    
    def _validateTagIds(self, tagIds: List[int]) -> List[int]:
        """验证标签ID有效性"""
        try:
            allActiveTagIds = self._getAllActiveTagIds()
            validTagIds = [tagId for tagId in tagIds if tagId in allActiveTagIds]
            
            invalidTagIds = set(tagIds) - set(validTagIds)
            if invalidTagIds:
                self.logger.warning(f"⚠️ 无效的标签ID: {list(invalidTagIds)}")
            
            self.logger.info(f"✅ 有效标签ID: {validTagIds}")
            return validTagIds
            
        except Exception as e:
            self.logger.error(f"❌ 验证标签ID失败: {str(e)}")
            return []
    
    def performHealthCheck(self) -> Dict[str, Any]:
        """
        执行系统健康检查
        
        Returns:
            Dict[str, Any]: 健康检查结果
        """
        try:
            self.logger.info("🔍 执行系统健康检查...")
            
            healthStatus = {
                'overall_status': 'healthy',
                'components': {},
                'statistics': {}
            }
            
            # 检查各个组件
            healthStatus['components']['spark'] = self.spark is not None
            
            # 只检查一次MySQL连接，避免重复日志
            mysql_connection_ok = self._dataWriter.testConnection()
            healthStatus['components']['mysql_connection'] = mysql_connection_ok
            
            # 如果MySQL连接失败，跳过规则读取器验证
            if mysql_connection_ok:
                healthStatus['components']['rule_reader'] = True  # 复用MySQL连接状态
            else:
                healthStatus['components']['rule_reader'] = False
            
            # 获取统计信息
            healthStatus['statistics'] = self._ruleReader.getStatistics()
            
            # 判断整体状态
            if not all(healthStatus['components'].values()):
                healthStatus['overall_status'] = 'unhealthy'
            
            self.logger.info(f"📊 健康检查完成: {healthStatus['overall_status']}")
            return healthStatus
            
        except Exception as e:
            self.logger.error(f"❌ 健康检查失败: {str(e)}")
            return {
                'overall_status': 'error',
                'error': str(e)
            }
    
    def get_available_tasks(self) -> Dict[int, str]:
        """
        获取所有可用的标签任务
        
        Returns:
            Dict[int, str]: {tag_id: task_class_name}
        """
        try:
            self.logger.info("📋 获取可用标签任务...")
            
            # 硬编码已有的任务映射关系（修正后的映射）
            task_mapping = {
                1: "HighNetWorthUserTask",     # 高净值用户
                2: "ActiveTraderTask",         # 活跃交易者
                3: "LowRiskUserTask",          # 低风险用户
                4: "NewUserTask",              # 新注册用户
                5: "VIPUserTask",              # VIP客户
                6: "CashRichUserTask",         # 现金充足用户
                7: "YoungUserTask",            # 年轻用户
                8: "RecentActiveTask"          # 最近活跃用户
            }
            
            # 尝试从数据库获取活跃标签，如果失败则返回所有任务
            try:
                if not hasattr(self, '_ruleReader') or self._ruleReader is None:
                    from src.batch.utils.RuleReader import RuleReader
                    self._ruleReader = RuleReader(self.spark, self.systemConfig.mysql)
                
                # 获取所有活跃标签
                rulesDataFrame = self._ruleReader.getActiveRulesDataFrame()
                available_tasks = {}
                
                if rulesDataFrame:
                    active_tag_ids = [row['tag_id'] for row in rulesDataFrame.select("tag_id").distinct().orderBy("tag_id").collect()]
                    
                    # 只返回在数据库中存在且有对应任务类的标签
                    for tag_id in active_tag_ids:
                        if tag_id in task_mapping:
                            available_tasks[tag_id] = task_mapping[tag_id]
                else:
                    # 数据库中没有数据，返回所有任务
                    available_tasks = task_mapping
                    
            except Exception as db_error:
                self.logger.warning(f"⚠️ 无法从数据库获取标签信息，返回所有可用任务: {str(db_error)}")
                available_tasks = task_mapping
            
            # 显示任务映射关系：ID:TaskClass格式
            task_mappings = [f"{task_id}:{task_class}" for task_id, task_class in available_tasks.items()]
            self.logger.info(f"✅ 找到 {len(available_tasks)} 个可用任务: {task_mappings}")
            return available_tasks
            
        except Exception as e:
            self.logger.error(f"❌ 获取可用任务失败: {str(e)}")
            return {}
    
    def get_task_summary(self, available_tasks: Optional[Dict[int, str]] = None) -> Dict[int, Dict[str, Any]]:
        """
        获取任务详细摘要信息，使用getRuleByTagId方法从MySQL读取真实标签规则
        
        Args:
            available_tasks: 可用任务字典，如果不提供则内部获取（避免重复调用）
            
        Returns:
            Dict[int, Dict[str, Any]]: {tag_id: {tag_name, tag_category, rule_conditions, status, ...}}
        """
        try:
            self.logger.info("📊 获取任务摘要信息...")
            
            # 如果没有提供available_tasks，则获取（但不记录日志避免重复）
            if available_tasks is None:
                available_tasks = self._get_available_tasks_silent()
            
            task_summaries = {}
            
            try:
                # 确保RuleReader已初始化
                if not hasattr(self, '_ruleReader') or self._ruleReader is None:
                    from src.batch.utils.RuleReader import RuleReader
                    self._ruleReader = RuleReader(self.spark, self.systemConfig.mysql)
                
                self.logger.info("🔍 使用getRuleByTagId从MySQL读取各标签规则...")
                
                # 为每个可用任务获取对应的标签规则
                for tag_id, task_class in available_tasks.items():
                    rule_dict = self._ruleReader.getRuleByTagId(tag_id)
                    
                    if rule_dict:
                        # 使用getRuleByTagId返回的完整规则信息
                        summary = {
                            'tag_name': rule_dict.get('tag_name', f'标签{tag_id}'),
                            'tag_category': rule_dict.get('tag_category', 'Unknown'),
                            'description': rule_dict.get('tag_description', f'标签{tag_id}描述'),
                            'rule_conditions': rule_dict.get('rule_conditions', '{}'),
                            'status': 'active' if rule_dict.get('is_active', 1) == 1 else 'inactive',
                            'data_source': 'mysql',
                            'task_class': task_class,
                            # 从规则条件中解析所需字段
                            'required_fields': self._extractRequiredFields(rule_dict.get('rule_conditions', '{}')),
                            'data_sources': self._inferDataSources(rule_dict.get('rule_conditions', '{}'))
                        }
                        task_summaries[tag_id] = summary
                    else:
                        self.logger.warning(f"⚠️ 标签 {tag_id} 在MySQL中没有找到对应规则")
                
                if task_summaries:
                    self.logger.info(f"✅ 成功从MySQL读取 {len(task_summaries)} 个标签的规则")
                else:
                    self.logger.warning("⚠️ MySQL中没有找到任何标签规则数据")
                    
            except Exception as db_error:
                self.logger.error(f"❌ 无法从MySQL读取标签规则: {str(db_error)}")
                # 完全失败时返回空字典，不提供降级方案
                return {}
            
            return task_summaries
            
        except Exception as e:
            self.logger.error(f"❌ 获取任务摘要失败: {str(e)}")
            return {}
    
    def _get_available_tasks_silent(self) -> Dict[int, str]:
        """
        静默获取可用任务，不记录日志（内部使用，避免重复日志）
        """
        try:
            # 硬编码已有的任务映射关系（修正后的映射）
            task_mapping = {
                1: "HighNetWorthUserTask",     # 高净值用户
                2: "ActiveTraderTask",         # 活跃交易者
                3: "LowRiskUserTask",          # 低风险用户
                4: "NewUserTask",              # 新注册用户
                5: "VIPUserTask",              # VIP客户
                6: "CashRichUserTask",         # 现金充足用户
                7: "YoungUserTask",            # 年轻用户
                8: "RecentActiveTask"          # 最近活跃用户
            }
            
            # 尝试从数据库获取活跃标签，如果失败则返回所有任务
            try:
                if not hasattr(self, '_ruleReader') or self._ruleReader is None:
                    from src.batch.utils.RuleReader import RuleReader
                    self._ruleReader = RuleReader(self.spark, self.systemConfig.mysql)
                
                # 获取所有活跃标签
                rulesDataFrame = self._ruleReader.getActiveRulesDataFrame()
                available_tasks = {}
                
                if rulesDataFrame:
                    active_tag_ids = [row['tag_id'] for row in rulesDataFrame.select("tag_id").distinct().orderBy("tag_id").collect()]
                    
                    # 只返回在数据库中存在且有对应任务类的标签
                    for tag_id in active_tag_ids:
                        if tag_id in task_mapping:
                            available_tasks[tag_id] = task_mapping[tag_id]
                else:
                    # 数据库中没有数据，返回所有任务
                    available_tasks = task_mapping
                    
            except Exception:
                available_tasks = task_mapping
            
            return available_tasks
            
        except Exception:
            return {}
    
    def _extractRequiredFields(self, rule_conditions: str) -> List[str]:
        """从规则条件JSON中提取所需字段"""
        try:
            import json
            conditions = json.loads(rule_conditions)
            fields = []
            
            # 递归提取条件中的字段
            def extract_fields(obj):
                if isinstance(obj, dict):
                    if 'field' in obj:
                        fields.append(obj['field'])
                    if 'conditions' in obj:
                        for condition in obj['conditions']:
                            extract_fields(condition)
                elif isinstance(obj, list):
                    for item in obj:
                        extract_fields(item)
            
            extract_fields(conditions)
            # 始终包含user_id
            if 'user_id' not in fields:
                fields.insert(0, 'user_id')
            
            return fields
            
        except Exception as e:
            self.logger.warning(f"⚠️ 解析规则条件失败: {str(e)}")
            return ['user_id']
    
    def _inferDataSources(self, rule_conditions: str) -> List[str]:
        """根据规则条件推断数据源"""
        # 基于字段名称的简单推断规则
        field_to_source_mapping = {
            'total_asset_value': 'user_asset_summary',
            'cash_balance': 'user_asset_summary', 
            'trade_count_30d': 'user_activity_summary',
            'last_login_date': 'user_activity_summary',
            'age': 'user_basic_info',
            'registration_date': 'user_basic_info',
            'risk_score': 'user_basic_info',
            'user_level': 'user_basic_info',
            'kyc_status': 'user_basic_info'
        }
        
        # 从规则条件中获取字段，然后映射到数据源
        try:
            fields = self._extractRequiredFields(rule_conditions)
            sources = []
            for field in fields:
                if field in field_to_source_mapping:
                    source = field_to_source_mapping[field]
                    if source not in sources:
                        sources.append(source)
            return sources if sources else ['user_basic_info']
        except Exception:
            pass
        
        return ['user_basic_info']
    
    def cleanup(self):
        """清理系统资源"""
        try:
            self.logger.info("🧹 开始清理系统资源...")
            
            # 清理缓存
            if self._existingTagsCache is not None:
                self._existingTagsCache.unpersist()
                self._existingTagsCache = None
            
            if hasattr(self, '_ruleReader') and self._ruleReader:
                self._ruleReader.clearCache()
            
            # 停止Spark会话
            if self.spark:
                self.spark.stop()
                self.spark = None
            
            self.logger.info("✅ 系统资源清理完成")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 系统资源清理异常: {str(e)}")
    
    def __enter__(self):
        """上下文管理器入口"""
        self.initializeSystem()
        return self
    
    def __exit__(self, excType, excVal, excTb):
        """上下文管理器退出"""
        self.cleanup()
        
        if excType is not None:
            self.logger.error(f"❌ 系统执行异常: {excType.__name__}: {excVal}")
        
        return False  # 不抑制异常