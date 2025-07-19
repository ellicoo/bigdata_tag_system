"""
标签任务抽象基类 - 重构为自包含的任务架构
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class BaseTagTask(ABC):
    """
    标签任务基类 - 每个标签任务自包含，自己负责数据加载和执行
    
    核心设计原则：
    1. 每个任务只关心自己需要的数据源和字段
    2. 任务自己负责按需加载数据
    3. 任务自己获取自己的规则
    4. 任务独立执行，不依赖其他任务
    """
    
    def __init__(self, task_config: Dict[str, Any], spark: SparkSession, system_config):
        self.task_config = task_config
        self.tag_id = task_config['tag_id']
        self.tag_name = task_config['tag_name']
        self.tag_category = task_config['tag_category']
        self.rule_conditions = task_config.get('rule_conditions', {})
        
        # 系统依赖（只传入必要的依赖）
        self.spark = spark
        self.system_config = system_config
        
        # 任务专用的组件（延迟初始化）
        self._rule_processor = None
        self._data_cache = {}
    
    @abstractmethod
    def get_required_fields(self) -> List[str]:
        """
        返回该标签需要的数据字段
        
        Returns:
            List[str]: 必需的字段列表
        """
        pass
    
    @abstractmethod
    def get_data_sources(self) -> Dict[str, str]:
        """
        返回该标签需要的数据源配置
        
        Returns:
            Dict[str, str]: 数据源映射 {source_name: source_path}
        """
        pass
    
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """
        数据预处理 - 每个标签可以有自己的数据处理逻辑
        默认实现：直接返回原数据
        
        Args:
            raw_data: 原始数据DataFrame
            
        Returns:
            DataFrame: 预处理后的数据
        """
        return raw_data
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """
        结果后处理 - 可选的业务逻辑
        默认实现：直接返回结果
        
        Args:
            tagged_users: 标签计算结果
            
        Returns:
            DataFrame: 后处理的结果
        """
        return tagged_users
    
    def validate_data(self, data: DataFrame) -> bool:
        """
        验证数据是否满足任务需求
        
        Args:
            data: 输入数据
            
        Returns:
            bool: 数据是否有效
        """
        required_fields = self.get_required_fields()
        missing_fields = set(required_fields) - set(data.columns)
        
        if missing_fields:
            logger.warning(f"标签任务 {self.tag_name} 缺少必需字段: {missing_fields}")
            return False
        
        return True
    
    def load_my_data(self, user_filter: Optional[List[str]] = None) -> DataFrame:
        """
        任务自己加载需要的数据 - 按需加载，只读取自己需要的数据源和字段
        
        Args:
            user_filter: 可选的用户ID过滤列表
            
        Returns:
            DataFrame: 当前任务需要的数据
        """
        try:
            data_sources = self.get_data_sources()
            required_fields = self.get_required_fields()
            
            logger.info(f"🔍 任务 {self.tag_name} 开始加载数据...")
            logger.info(f"   📊 数据源: {data_sources}")
            logger.info(f"   📋 字段: {required_fields}")
            
            # 加载主要数据源
            primary_source = data_sources.get('primary')
            if not primary_source:
                raise ValueError(f"任务 {self.tag_name} 没有定义主要数据源")
            
            # 从缓存检查
            cache_key = f"{primary_source}_{','.join(sorted(required_fields))}"
            if cache_key in self._data_cache:
                logger.info(f"   ✅ 使用缓存数据: {cache_key}")
                data = self._data_cache[cache_key]
            else:
                # 按需加载数据
                data = self._load_data_from_source(primary_source, required_fields)
                self._data_cache[cache_key] = data
                logger.info(f"   ✅ 数据加载完成: {data.count()} 条记录")
            
            # 用户过滤
            if user_filter:
                data = data.filter(data.user_id.isin(user_filter))
                logger.info(f"   🎯 用户过滤后: {data.count()} 条记录")
            
            # 数据预处理
            processed_data = self.preprocess_data(data)
            
            # 验证数据
            if not self.validate_data(processed_data):
                raise ValueError(f"任务 {self.tag_name} 数据验证失败")
            
            return processed_data
            
        except Exception as e:
            logger.error(f"❌ 任务 {self.tag_name} 数据加载失败: {str(e)}")
            raise
    
    def _load_data_from_source(self, source_name: str, fields: List[str]) -> DataFrame:
        """从指定数据源加载指定字段的数据"""
        try:
            # 根据环境选择数据加载方式
            if self.system_config.environment == 'local':
                # 本地环境：生成测试数据或从MinIO读取
                return self._load_local_data(source_name, fields)
            else:
                # Glue环境：从S3读取
                return self._load_s3_data(source_name, fields)
                
        except Exception as e:
            logger.error(f"❌ 从数据源 {source_name} 加载数据失败: {str(e)}")
            raise
    
    def _load_local_data(self, source_name: str, fields: List[str]) -> DataFrame:
        """本地环境数据加载 - 优先使用真实的S3/Hive数据"""
        logger.info(f"📊 本地环境加载数据源: {source_name}")
        
        # 优先尝试从真实的S3/Hive读取
        try:
            from src.batch.core.data_loader import BatchDataLoader
            data_loader = BatchDataLoader(self.spark, self.system_config)
            
            logger.info(f"🔄 尝试从S3/Hive读取真实数据: {source_name}")
            df = data_loader.hive_loader.read_hive_table(source_name, fields)
            
            if df is not None and df.count() > 0:
                logger.info(f"✅ 成功从S3/Hive读取数据: {source_name}, 记录数: {df.count()}")
                return df
            else:
                logger.info(f"ℹ️ S3/Hive数据不可用，使用生产级模拟数据: {source_name}")
                return self._generate_production_like_data(source_name, fields)
                
        except Exception as e:
            logger.info(f"ℹ️ S3/Hive读取失败，使用生产级模拟数据: {source_name} (原因: JAR依赖问题)")
            # 降级到生产级模拟数据生成
            return self._generate_production_like_data(source_name, fields)
    
    def _generate_simple_test_data(self, source_name: str, fields: List[str]) -> DataFrame:
        """生成简单的测试数据"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from datetime import datetime, timedelta
        
        # 生成基本测试数据
        data = []
        for i in range(100):
            user_id = f"user_{i:06d}"
            base_date = datetime.now().date()
            
            data.append({
                "user_id": user_id,
                "total_asset_value": 100000.0 + i * 1000,
                "cash_balance": 50000.0 + i * 500,
                "age": 25 + (i % 40),
                "user_level": "VIP1" if i % 5 == 0 else "Regular",
                "kyc_status": "verified",
                "trade_count_30d": 10 + (i % 20),
                "risk_score": 30.0 + (i % 50),
                "last_login_date": base_date - timedelta(days=i % 30),  # 添加登录日期
                "registration_date": base_date - timedelta(days=30 + (i % 300))  # 添加注册日期
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
            available_fields = [f for f in fields if f in df.columns]
            if available_fields:
                return df.select(*available_fields)
        
        return df
    
    def _generate_production_like_data(self, source_name: str, fields: List[str]) -> DataFrame:
        """生成生产级模拟数据，接近真实Hive表数据质量"""
        logger.info(f"🏭 生成生产级模拟数据: {source_name}")
        
        try:
            # 使用编排器的生产级数据生成器
            from src.batch.orchestrator.batch_orchestrator import BatchOrchestrator
            orchestrator = BatchOrchestrator(self.system_config)
            orchestrator.spark = self.spark  # 使用当前的Spark会话
            
            # 生成对应数据源的数据
            df = orchestrator._generate_production_like_data(source_name)
            
            if df is None:
                # 如果生产级数据生成失败，降级到简单数据
                logger.warning(f"⚠️ 生产级数据生成失败，降级到简单测试数据: {source_name}")
                return self._generate_simple_test_data(source_name, fields)
            
            # 选择需要的字段
            if fields:
                available_fields = [f for f in fields if f in df.columns]
                if available_fields:
                    df = df.select(*available_fields)
            
            logger.info(f"✅ 生产级模拟数据生成完成: {source_name}, 记录数: {df.count()}")
            return df
            
        except Exception as e:
            logger.warning(f"⚠️ 生产级数据生成异常，降级到简单测试数据: {str(e)}")
            return self._generate_simple_test_data(source_name, fields)
    
    def _load_s3_data(self, source_name: str, fields: List[str]) -> DataFrame:
        """S3环境数据加载"""
        try:
            s3_config = self.system_config.s3
            table_path = f"{s3_config.warehouse_path}{source_name}/"
            
            logger.info(f"📖 从S3读取: {table_path}")
            df = self.spark.read.parquet(table_path)
            
            if fields:
                available_fields = [f for f in fields if f in df.columns]
                if available_fields:
                    df = df.select(*available_fields)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ S3数据加载失败: {str(e)}")
            raise
    
    def get_my_rule(self) -> Dict[str, Any]:
        """
        获取当前任务的规则条件
        
        Returns:
            Dict[str, Any]: 规则条件
        """
        return self.rule_conditions
    
    def execute(self, user_filter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        完整的任务执行流程 - 这是任务的主入口
        
        Args:
            user_filter: 可选的用户ID过滤列表
            
        Returns:
            DataFrame: 标签计算结果 (user_id, tag_id, tag_detail)
        """
        try:
            logger.info(f"🚀 开始执行任务: {self.tag_name} (ID: {self.tag_id})")
            
            # 1. 加载当前任务需要的数据
            task_data = self.load_my_data(user_filter)
            
            if task_data.count() == 0:
                logger.warning(f"⚠️ 任务 {self.tag_name} 没有可用数据")
                return None
            
            # 2. 获取当前任务的规则
            rule_conditions = self.get_my_rule()
            
            # 3. 初始化规则处理器（延迟初始化）
            if not self._rule_processor:
                from src.batch.core.rule_processor import RuleProcessor
                self._rule_processor = RuleProcessor()
            
            # 4. 应用规则过滤
            filtered_users = self._rule_processor.apply_rules(task_data, rule_conditions)
            
            if filtered_users.count() == 0:
                logger.info(f"📊 任务 {self.tag_name} 没有用户命中条件")
                return None
            
            # 5. 构建标签结果（确保用户唯一性）
            from pyspark.sql.functions import lit
            result = filtered_users.select(
                "user_id",
                lit(self.tag_id).alias("tag_id"),
                lit(f"{self.tag_name} - {self.tag_category}").alias("tag_detail")
            ).distinct()  # 确保用户唯一性
            
            # 6. 后处理
            final_result = self.post_process_result(result)
            
            result_count = final_result.count()
            logger.info(f"✅ 任务 {self.tag_name} 执行完成: {result_count} 个用户命中")
            
            return final_result
            
        except Exception as e:
            logger.error(f"❌ 任务 {self.tag_name} 执行失败: {str(e)}")
            raise
    
    def cleanup(self):
        """清理任务资源"""
        try:
            # 清理数据缓存
            for df in self._data_cache.values():
                if hasattr(df, 'unpersist'):
                    df.unpersist()
            self._data_cache.clear()
            
            logger.debug(f"🧹 任务 {self.tag_name} 资源清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 任务 {self.tag_name} 资源清理异常: {str(e)}")
    
    def get_task_metadata(self) -> Dict[str, Any]:
        """
        获取任务元数据
        
        Returns:
            Dict[str, Any]: 任务元数据
        """
        return {
            'tag_id': self.tag_id,
            'tag_name': self.tag_name,
            'tag_category': self.tag_category,
            'required_fields': self.get_required_fields(),
            'data_sources': self.get_data_sources(),
            'task_class': self.__class__.__name__
        }
    
    def __str__(self) -> str:
        return f"TagTask({self.tag_id}: {self.tag_name})"
    
    def __repr__(self) -> str:
        return self.__str__()