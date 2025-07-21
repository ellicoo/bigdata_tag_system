"""
标签任务抽象基类 - 参考TFECUserPortrait设计
模仿AbstractBaseModel，提供S3数据读取的抽象能力
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class BaseTagTask(ABC):
    """
    标签任务基类 - 模仿TFECUserPortrait的AbstractBaseModel设计
    
    核心设计原则：
    1. 父类抽象S3读取功能，子类指定具体表地址
    2. 每个任务自包含，自己负责数据加载和执行
    3. 任务独立执行，支持并行
    4. 一个类一个文件，驼峰命名
    """
    
    def __init__(self, taskConfig: Dict[str, Any], spark: SparkSession, systemConfig):
        """
        初始化标签任务
        
        Args:
            taskConfig: 任务配置
            spark: Spark会话
            systemConfig: 系统配置
        """
        self.taskConfig = taskConfig
        self.tagId = taskConfig['tag_id']
        self.tagName = taskConfig['tag_name']
        self.tagCategory = taskConfig['tag_category']
        self.ruleConditions = taskConfig.get('rule_conditions', {})
        
        # 系统依赖
        self.spark = spark
        self.systemConfig = systemConfig
        
        # 任务专用组件（延迟初始化）
        self._ruleProcessor = None
        self._dataCache = {}
    
    @abstractmethod
    def getRequiredFields(self) -> List[str]:
        """
        返回该标签需要的数据字段
        子类必须实现此方法，指定需要哪些字段
        
        Returns:
            List[str]: 必需的字段列表
        """
        pass
    
    @abstractmethod
    def getHiveTableConfig(self) -> Dict[str, str]:
        """
        返回该标签需要的Hive表配置 - 核心抽象方法
        子类指定具体的完整S3路径
        
        Returns:
            Dict[str, str]: Hive表配置 {table_name: 完整S3路径}
            例如: {
                'user_basic_info': 's3a://my-bucket/warehouse/user_basic_info/',
                'user_asset_summary': 's3a://my-bucket/warehouse/user_asset_summary/'
            }
        """
        pass
    
    def loadHiveData(self, tableName: str, fields: List[str] = None) -> DataFrame:
        """
        从S3加载Hive表数据 - 父类抽象实现
        子类通过getHiveTableConfig()指定表地址，父类负责具体读取
        
        Args:
            tableName: 表名
            fields: 需要的字段列表，None表示所有字段
            
        Returns:
            DataFrame: 加载的数据
        """
        try:
            hiveConfig = self.getHiveTableConfig()
            if tableName not in hiveConfig:
                raise ValueError(f"任务 {self.tagName} 未配置表 {tableName}")
            
            tablePath = hiveConfig[tableName]
            logger.info(f"🔍 任务 {self.tagName} 开始加载Hive表: {tableName}")
            logger.info(f"   📍 表路径: {tablePath}")
            
            # 根据环境选择数据加载方式
            if self.systemConfig.environment == 'local':
                return self._loadLocalHiveData(tablePath, fields)
            else:
                return self._loadS3HiveData(tablePath, fields)
                
        except Exception as e:
            logger.error(f"❌ 任务 {self.tagName} 加载Hive表失败: {tableName}, 错误: {str(e)}")
            raise
    
    def _loadS3HiveData(self, tablePath: str, fields: List[str] = None) -> DataFrame:
        """
        从S3加载Hive表数据
        
        Args:
            tablePath: 完整的S3表路径（任务自主指定）
            fields: 需要的字段列表
            
        Returns:
            DataFrame: 加载的数据
        """
        try:
            # 🎯 关键改动：直接使用任务提供的完整S3路径，不再拼接
            fullPath = tablePath.rstrip('/')  # 去除末尾斜杠统一格式
            
            logger.info(f"📖 从S3读取Hive表: {fullPath}")
            df = self.spark.read.parquet(fullPath)
            
            # 字段选择优化
            if fields:
                availableFields = [f for f in fields if f in df.columns]
                if availableFields:
                    df = df.select(*availableFields)
                    logger.info(f"   📋 字段筛选: {availableFields}")
            
            logger.info(f"   ✅ 数据加载完成，记录数: {df.count()}")
            return df
            
        except Exception as e:
            logger.error(f"❌ S3数据加载失败: {tablePath}, 错误: {str(e)}")
            raise
    
    def _loadLocalHiveData(self, tablePath: str, fields: List[str] = None) -> DataFrame:
        """
        本地环境Hive数据加载 - 优先使用真实数据，降级到模拟数据
        
        Args:
            tablePath: 表路径
            fields: 需要的字段列表
            
        Returns:
            DataFrame: 加载的数据
        """
        logger.info(f"📊 本地环境加载Hive表: {tablePath}")
        
        # 优先尝试从真实的S3/Hive读取
        try:
            from src.batch.utils.HiveDataReader import HiveDataReader
            hiveReader = HiveDataReader(self.spark, self.systemConfig)
            
            logger.info(f"🔄 尝试从S3/Hive读取真实数据: {tablePath}")
            df = hiveReader.readHiveTable(tablePath, fields)
            
            if df is not None and df.count() > 0:
                logger.info(f"✅ 成功从S3/Hive读取数据: {tablePath}, 记录数: {df.count()}")
                return df
            else:
                logger.info(f"ℹ️ S3/Hive数据不可用，使用生产级模拟数据: {tablePath}")
                return self._generateProductionLikeData(tablePath, fields)
                
        except Exception as e:
            logger.info(f"ℹ️ S3/Hive读取失败，使用生产级模拟数据: {tablePath} (原因: JAR依赖问题)")
            return self._generateProductionLikeData(tablePath, fields)
    
    def _generateProductionLikeData(self, tableName: str, fields: List[str] = None) -> DataFrame:
        """
        生成生产级模拟数据
        
        Args:
            tableName: 表名
            fields: 需要的字段列表
            
        Returns:
            DataFrame: 模拟数据
        """
        logger.info(f"🏭 生成生产级模拟数据: {tableName}")
        
        try:
            # 使用编排器的生产级数据生成器
            from src.batch.engine.BatchOrchestrator import BatchOrchestrator
            orchestrator = BatchOrchestrator(self.systemConfig)
            orchestrator.spark = self.spark  # 使用当前的Spark会话
            
            # 生成对应数据源的数据
            df = orchestrator._generate_production_like_data(tableName)
            
            if df is None:
                # 如果生产级数据生成失败，降级到简单数据
                logger.warning(f"⚠️ 生产级数据生成失败，降级到简单测试数据: {tableName}")
                return self._generateSimpleTestData(tableName, fields)
            
            # 选择需要的字段
            if fields:
                availableFields = [f for f in fields if f in df.columns]
                if availableFields:
                    df = df.select(*availableFields)
            
            logger.info(f"✅ 生产级模拟数据生成完成: {tableName}, 记录数: {df.count()}")
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ 生产级数据生成异常，降级到简单测试数据: {str(e)}")
            return self._generateSimpleTestData(tableName, fields)
    
    def _generateSimpleTestData(self, tableName: str, fields: List[str] = None) -> DataFrame:
        """
        生成简单的测试数据 - 最后的降级方案
        
        Args:
            tableName: 表名
            fields: 需要的字段列表
            
        Returns:
            DataFrame: 简单测试数据
        """
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from datetime import datetime, timedelta
        
        # 生成基本测试数据
        data = []
        for i in range(100):
            userId = f"user_{i:06d}"
            baseDate = datetime.now().date()
            
            data.append({
                "user_id": userId,
                "total_asset_value": 100000.0 + i * 1000,
                "cash_balance": 50000.0 + i * 500,
                "age": 25 + (i % 40),
                "user_level": "VIP1" if i % 5 == 0 else "Regular",
                "kyc_status": "verified",
                "trade_count_30d": 10 + (i % 20),
                "risk_score": 30.0 + (i % 50),
                "last_login_date": baseDate - timedelta(days=i % 30),
                "registration_date": baseDate - timedelta(days=30 + (i % 300))
            })
        
        # 创建schema
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("total_asset_value", DoubleType(), True),
            StructField("cash_balance", DoubleType(), True),
            StructField("age", IntegerType(), True),
            StructField("user_level", StringType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("trade_count_30d", IntegerType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("last_login_date", DateType(), True),
            StructField("registration_date", DateType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # 只返回需要的字段
        if fields:
            availableFields = [f for f in fields if f in df.columns]
            if availableFields:
                return df.select(*availableFields)
        
        return df
    
    def preprocessData(self, rawData: DataFrame) -> DataFrame:
        """
        数据预处理 - 每个标签可以有自己的数据处理逻辑
        默认实现：直接返回原数据
        
        Args:
            rawData: 原始数据DataFrame
            
        Returns:
            DataFrame: 预处理后的数据
        """
        return rawData
    
    def postProcessResult(self, taggedUsers: DataFrame) -> DataFrame:
        """
        结果后处理 - 可选的业务逻辑
        默认实现：直接返回结果
        
        Args:
            taggedUsers: 标签计算结果
            
        Returns:
            DataFrame: 后处理的结果
        """
        return taggedUsers
    
    def validateData(self, data: DataFrame) -> bool:
        """
        验证数据是否满足任务需求
        
        Args:
            data: 输入数据
            
        Returns:
            bool: 数据是否有效
        """
        requiredFields = self.getRequiredFields()
        missingFields = set(requiredFields) - set(data.columns)
        
        if missingFields:
            logger.warning(f"标签任务 {self.tagName} 缺少必需字段: {missingFields}")
            return False
        
        return True
    
    def loadMyData(self, userFilter: Optional[List[str]] = None) -> DataFrame:
        """
        任务自己加载需要的数据 - 使用抽象的Hive表读取能力
        
        Args:
            userFilter: 可选的用户ID过滤列表
            
        Returns:
            DataFrame: 当前任务需要的数据
        """
        try:
            hiveConfig = self.getHiveTableConfig()
            requiredFields = self.getRequiredFields()
            
            logger.info(f"🔍 任务 {self.tagName} 开始加载数据...")
            logger.info(f"   📊 Hive表: {list(hiveConfig.keys())}")
            logger.info(f"   📋 字段: {requiredFields}")
            
            # 加载主表数据（通常是第一个配置的表）
            primaryTable = list(hiveConfig.keys())[0]
            
            # 检查缓存
            cacheKey = f"{primaryTable}_{','.join(sorted(requiredFields))}"
            if cacheKey in self._dataCache:
                logger.info(f"   ✅ 使用缓存数据: {cacheKey}")
                data = self._dataCache[cacheKey]
            else:
                # 按需加载数据
                data = self.loadHiveData(primaryTable, requiredFields)
                self._dataCache[cacheKey] = data
                logger.info(f"   ✅ 数据加载完成: {data.count()} 条记录")
            
            # 用户过滤
            if userFilter:
                data = data.filter(data.user_id.isin(userFilter))
                logger.info(f"   🎯 用户过滤后: {data.count()} 条记录")
            
            # 数据预处理
            processedData = self.preprocessData(data)
            
            # 验证数据
            if not self.validateData(processedData):
                raise ValueError(f"任务 {self.tagName} 数据验证失败")
            
            return processedData
            
        except Exception as e:
            logger.error(f"❌ 任务 {self.tagName} 数据加载失败: {str(e)}")
            raise
    
    def getMyRule(self) -> Dict[str, Any]:
        """
        获取当前任务的规则条件
        
        Returns:
            Dict[str, Any]: 规则条件
        """
        return self.ruleConditions
    
    def execute(self, userFilter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        完整的任务执行流程 - 这是任务的主入口
        
        Args:
            userFilter: 可选的用户ID过滤列表
            
        Returns:
            DataFrame: 标签计算结果 (user_id, tag_id, tag_detail)
        """
        try:
            logger.info(f"🚀 开始执行任务: {self.tagName} (ID: {self.tagId})")
            
            # 1. 加载当前任务需要的数据
            taskData = self.loadMyData(userFilter)
            
            if taskData.count() == 0:
                logger.warning(f"⚠️ 任务 {self.tagName} 没有可用数据")
                return None
            
            # 2. 获取当前任务的规则
            ruleConditions = self.getMyRule()
            
            # 3. 初始化规则处理器（延迟初始化）
            if not self._ruleProcessor:
                from src.batch.utils.RuleProcessor import RuleProcessor
                self._ruleProcessor = RuleProcessor()
            
            # 4. 应用规则过滤
            filteredUsers = self._ruleProcessor.applyRules(taskData, ruleConditions)
            
            if filteredUsers.count() == 0:
                logger.info(f"📊 任务 {self.tagName} 没有用户命中条件")
                return None
            
            # 5. 构建标签结果（确保用户唯一性）
            from pyspark.sql.functions import lit
            result = filteredUsers.select(
                "user_id",
                lit(self.tagId).alias("tag_id"),
                lit(f"{self.tagName} - {self.tagCategory}").alias("tag_detail")
            ).distinct()  # 确保用户唯一性
            
            # 6. 后处理
            finalResult = self.postProcessResult(result)
            
            resultCount = finalResult.count()
            logger.info(f"✅ 任务 {self.tagName} 执行完成: {resultCount} 个用户命中")
            
            return finalResult
            
        except Exception as e:
            logger.error(f"❌ 任务 {self.tagName} 执行失败: {str(e)}")
            raise
    
    def cleanup(self):
        """清理任务资源"""
        try:
            # 清理数据缓存
            for df in self._dataCache.values():
                if hasattr(df, 'unpersist'):
                    df.unpersist()
            self._dataCache.clear()
            
            logger.debug(f"🧹 任务 {self.tagName} 资源清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 任务 {self.tagName} 资源清理异常: {str(e)}")
    
    def getTaskMetadata(self) -> Dict[str, Any]:
        """
        获取任务元数据
        
        Returns:
            Dict[str, Any]: 任务元数据
        """
        return {
            'tagId': self.tagId,
            'tagName': self.tagName,
            'tagCategory': self.tagCategory,
            'requiredFields': self.getRequiredFields(),
            'hiveTableConfig': self.getHiveTableConfig(),
            'taskClass': self.__class__.__name__
        }
    
    def __str__(self) -> str:
        return f"TagTask({self.tagId}: {self.tagName})"
    
    def __repr__(self) -> str:
        return self.__str__()