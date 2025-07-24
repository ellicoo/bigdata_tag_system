import logging
import time
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.config.base import BaseConfig
from src.readers.hive_reader import HiveDataReader
from src.readers.rule_reader import RuleReader
from src.engine.task_parallel_engine import TaskBasedParallelEngine
from src.merger.tag_merger import UnifiedTagMerger, TagMergeStrategy
from src.writers.optimized_mysql_writer import OptimizedMySQLWriter

logger = logging.getLogger(__name__)


class TagScheduler:
    """标签调度器 - 基于任务化架构的标签计算调度"""
    
    def __init__(self, config: BaseConfig, max_workers: int = 4):
        self.config = config
        self.max_workers = max_workers
        self.spark = None
        
        # 组件初始化
        self.rule_reader = None
        self.hive_reader = None
        self.task_engine = None
        self.unified_merger = None
        self.mysql_writer = None
    
    def initialize(self):
        """初始化组件"""
        try:
            logger.info("🚀 初始化标签调度器...")
            
            # 初始化Spark
            self.spark = self._create_spark_session()
            
            # 初始化组件
            self.rule_reader = RuleReader(self.spark, self.config.mysql)
            self.hive_reader = HiveDataReader(self.spark, self.config)
            
            # 初始化规则数据
            self.rule_reader.initialize()
            
            # 初始化任务引擎，传入已初始化的rule_reader
            self.task_engine = TaskBasedParallelEngine(self.spark, self.config, self.max_workers, self.rule_reader)
            self.unified_merger = UnifiedTagMerger(self.spark, self.config.mysql)
            self.mysql_writer = OptimizedMySQLWriter(self.spark, self.config.mysql)
            
            # 初始化时预缓存MySQL现有标签数据
            self.cached_existing_tags = None
            
            # 预缓存现有标签数据
            self._preload_existing_tags()
            
            logger.info("✅ 标签调度器初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 标签调度器初始化失败: {str(e)}")
            raise
    
    def _create_spark_session(self) -> SparkSession:
        """创建Spark会话"""
        builder = SparkSession.builder
        
        for key, value in self.config.spark.to_dict().items():
            builder = builder.config(key, value)
        
        # 启用Hive支持（基于您的方式）
        builder = builder.enableHiveSupport()
        
        # 移除S3配置，直接读取Hive表
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")  # 更严格的日志级别，只显示错误
        
        # 关闭Spark的详细日志
        spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger("org").setLevel(
            spark.sparkContext._jvm.org.apache.log4j.Level.ERROR
        )
        spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger("akka").setLevel(
            spark.sparkContext._jvm.org.apache.log4j.Level.ERROR
        )
        
        logger.info(f"Spark会话创建成功: {spark.sparkContext.applicationId}")
        return spark
    
    def _preload_existing_tags(self):
        """预缓存MySQL中的现有标签数据"""
        try:
            logger.info("🔄 预缓存MySQL现有用户标签数据-persist(内存&磁盘)...")
            
            existing_df = self.spark.read.jdbc(
                url=self.config.mysql.jdbc_url,
                table="user_tags",
                properties=self.config.mysql.connection_properties
            )
            
            if existing_df.count() == 0:
                logger.info("MySQL中暂无现有用户标签数据")
                self.cached_existing_tags = None
            else:
                # 预缓存现有标签数据，进行JSON转换
                from pyspark import StorageLevel
                from pyspark.sql.functions import from_json
                from pyspark.sql.types import ArrayType, IntegerType
                
                # JSON转换：将tag_ids从字符串转为数组
                processed_df = existing_df.select(
                    "user_id",
                    from_json(col("tag_ids"), ArrayType(IntegerType())).alias("tag_ids"),
                    "tag_details",
                    "created_time", 
                    "updated_time"
                )
                
                self.cached_existing_tags = processed_df.persist(StorageLevel.MEMORY_AND_DISK)
                # 触发缓存
                cached_count = self.cached_existing_tags.count()
                logger.info(f"✅ 预缓存MySQL现有用户标签数据完成（含JSON转换），共 {cached_count} 条记录")
            
        except Exception as e:
            logger.warning(f"⚠️ 预缓存MySQL现有用户标签数据失败: {str(e)}")
            self.cached_existing_tags = None
    
    def health_check(self) -> bool:
        """系统健康检查"""
        try:
            logger.info("🏥 执行系统健康检查...")
            
            # 1. 检查Spark连接
            if not self.spark:
                logger.error("❌ Spark会话未初始化")
                return False
            
            # 2. 检查规则读取器
            if not self.rule_reader:
                logger.error("❌ 规则读取器未初始化")
                return False
            
            # 3. 检查任务引擎
            if not self.task_engine:
                logger.error("❌ 任务引擎未初始化")
                return False
            
            # 4. 检查数据库连接
            try:
                test_df = self.spark.read.jdbc(
                    url=self.config.mysql.jdbc_url,
                    table="(SELECT 1 as test_connection) as test",
                    properties=self.config.mysql.connection_properties
                )
                test_df.count()
                logger.info("✅ MySQL连接正常")
            except Exception as e:
                logger.error(f"❌ MySQL连接失败: {str(e)}")
                return False
            
            # 5. 检查任务注册状态
            available_tasks = self.get_available_tasks()
            logger.info(f"✅ 已注册任务: {len(available_tasks)} 个")
            
            logger.info("🎉 系统健康检查通过")
            return True
            
        except Exception as e:
            logger.error(f"❌ 系统健康检查失败: {str(e)}")
            return False
    
    # ==================== 任务化架构方法 ====================
    
    def scenario_task_all_users_all_tags(self, user_filter: Optional[List[str]] = None) -> bool:
        """任务化场景：全量用户全量标签计算"""
        try:
            start_time = time.time()
            logger.info(f"🎯 开始任务化全量用户全量标签计算")
            logger.info(f"👥 用户过滤: {user_filter if user_filter else '全量用户'}")
            
            # 1. 使用任务引擎执行所有任务
            task_results = self.task_engine.execute_all_tasks(user_filter)
            
            if task_results is None or task_results.count() == 0:
                logger.info("📊 全量标签任务执行完成 - 无用户符合任何标签条件 (这是正常情况)")
                return True
            
            # 2. 与MySQL已存在标签合并（所有场景都需要合并）
            logger.info("🔄 开始与MySQL已存在标签合并...")
            final_merged = self.unified_merger.advanced_merger.merge_with_existing_tags(
                task_results, 
                self.cached_existing_tags
            )
            if final_merged is None:
                logger.error("❌ 与MySQL已存在标签合并失败")
                return False
            
            logger.info(f"✅ 与MySQL已存在标签合并完成，最终影响用户数: {final_merged.count()}")
            
            # 3. 写入MySQL
            logger.info("📝 写入合并后的标签结果到MySQL...")
            success = self.mysql_writer.write_tag_results(final_merged)
            
            # 4. 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 任务化全量用户全量标签计算完成（含MySQL标签合并）！
🏷️  执行了所有已注册的任务类
👥 影响用户数: {final_merged.count()}
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 任务化全量标签计算失败: {str(e)}")
            return False
        finally:
            # 清理任务引擎缓存
            if self.task_engine:
                self.task_engine.cleanup_cache()
    
    def get_available_tasks(self) -> Dict[int, str]:
        """获取所有可用的标签任务"""
        from src.tasks.task_factory import TagTaskFactory
        return TagTaskFactory.get_all_available_tasks()
    
    def get_task_summary(self) -> Dict[int, Dict[str, Any]]:
        """获取任务摘要信息"""
        from src.tasks.task_registry import get_task_summary
        return get_task_summary()
    
    def scenario_task_all_users_specific_tags(self, tag_ids: List[int]) -> Optional[DataFrame]:
        """
        任务化场景: 全量用户跑指定标签任务
        - 执行指定标签ID对应的任务类
        - 并行计算指定标签
        - 与MySQL现有标签合并
        - 插入MySQL
        """
        try:
            logger.info(f"🎯 任务化场景: 全量用户跑指定标签任务 (标签ID: {tag_ids})")
            start_time = time.time()
            
            # 使用任务引擎执行指定标签任务
            result = self.task_engine.execute_specific_tag_tasks(tag_ids)
            
            if result is None:
                logger.warning(f"指定标签任务没有产生结果: {tag_ids}")
                return None
            
            # 与MySQL现有标签合并
            merged_result = self.unified_merger.advanced_merger.merge_with_existing_tags(
                result, 
                self.cached_existing_tags
            )
            
            # 写入MySQL
            success = self.mysql_writer.write_tag_results(merged_result)
            
            # 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 任务化场景完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return merged_result if success else None
            
        except Exception as e:
            logger.error(f"任务化场景执行失败: {str(e)}")
            return None
    
    def scenario_task_specific_users_specific_tags(self, user_ids: List[str], tag_ids: List[int]) -> Optional[DataFrame]:
        """
        任务化场景: 指定用户跑指定标签任务
        - 执行指定标签ID对应的任务类
        - 过滤指定用户
        - 并行计算指定标签
        - 与MySQL现有标签合并
        - 插入MySQL
        """
        try:
            logger.info(f"🎯 任务化场景: 指定用户跑指定标签任务 (用户: {user_ids}, 标签ID: {tag_ids})")
            start_time = time.time()
            
            # 使用任务引擎执行指定标签任务，传入用户过滤
            result = self.task_engine.execute_specific_tag_tasks(tag_ids, user_ids)
            
            if result is None:
                logger.warning(f"指定用户指定标签任务没有产生结果: 用户{user_ids}, 标签{tag_ids}")
                return None
            
            # 与MySQL现有标签合并
            merged_result = self.unified_merger.advanced_merger.merge_with_existing_tags(
                result, 
                self.cached_existing_tags
            )
            
            # 写入MySQL
            success = self.mysql_writer.write_tag_results(merged_result)
            
            # 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 任务化场景完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return merged_result if success else None
            
        except Exception as e:
            logger.error(f"任务化场景执行失败: {str(e)}")
            return None
    
    def cleanup(self):
        """清理资源"""
        try:
            # 清理任务引擎缓存
            if self.task_engine:
                self.task_engine.cleanup_cache()
            
            # 清理预缓存的标签数据
            if self.cached_existing_tags:
                self.cached_existing_tags.unpersist()
            
            # 停止Spark会话
            if self.spark:
                self.spark.stop()
            
            logger.info("✅ 资源清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 资源清理异常: {str(e)}")    
    def _generate_production_like_data(self, source_name: str = None) -> DataFrame:
        """生成生产环境模拟的测试数据"""
        try:
            logger.info("📊 生成生产环境模拟测试数据...")
            
            import random
            from datetime import datetime, timedelta
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
            from pyspark.sql.functions import lit
            import json
            
            # 生成基础用户数据
            num_users = 300
            
            # 生成用户基础信息
            user_basic_data = []
            for i in range(num_users):
                user_id = f"user_{i:06d}"
                
                # 生成不同类型的用户数据
                if i < 50:  # 高净值用户
                    total_asset = random.uniform(150000, 500000)
                    cash_balance = random.uniform(60000, 150000)
                    user_level = random.choice(["VIP1", "VIP2", "VIP3"])
                    kyc_status = "verified"
                    risk_score = random.uniform(20, 50)
                    age = random.randint(30, 55)
                    trade_count = random.randint(15, 40)
                elif i < 70:  # VIP用户
                    total_asset = random.uniform(80000, 200000)
                    cash_balance = random.uniform(30000, 80000)
                    user_level = random.choice(["VIP2", "VIP3"])
                    kyc_status = "verified"
                    risk_score = random.uniform(15, 35)
                    age = random.randint(25, 50)
                    trade_count = random.randint(10, 25)
                elif i < 100:  # 年轻用户
                    total_asset = random.uniform(5000, 50000)
                    cash_balance = random.uniform(1000, 20000)
                    user_level = random.choice(["Regular", "VIP1"])
                    kyc_status = random.choice(["verified", "pending"])
                    risk_score = random.uniform(25, 60)
                    age = random.randint(18, 30)
                    trade_count = random.randint(5, 20)
                elif i < 180:  # 活跃交易者
                    total_asset = random.uniform(20000, 100000)
                    cash_balance = random.uniform(5000, 40000)
                    user_level = random.choice(["Regular", "VIP1", "VIP2"])
                    kyc_status = "verified"
                    risk_score = random.uniform(30, 70)
                    age = random.randint(25, 45)
                    trade_count = random.randint(16, 50)
                elif i < 205:  # 低风险用户
                    total_asset = random.uniform(30000, 120000)
                    cash_balance = random.uniform(10000, 50000)
                    user_level = random.choice(["Regular", "VIP1"])
                    kyc_status = "verified"
                    risk_score = random.uniform(10, 30)
                    age = random.randint(28, 50)
                    trade_count = random.randint(5, 15)
                elif i < 220:  # 新注册用户
                    total_asset = random.uniform(1000, 20000)
                    cash_balance = random.uniform(500, 10000)
                    user_level = "Regular"
                    kyc_status = random.choice(["pending", "verified"])
                    risk_score = random.uniform(40, 80)
                    age = random.randint(20, 40)
                    trade_count = random.randint(1, 8)
                elif i < 235:  # 最近活跃用户
                    total_asset = random.uniform(15000, 80000)
                    cash_balance = random.uniform(5000, 30000)
                    user_level = random.choice(["Regular", "VIP1"])
                    kyc_status = "verified"
                    risk_score = random.uniform(25, 55)
                    age = random.randint(22, 45)
                    trade_count = random.randint(8, 25)
                else:  # 普通用户
                    total_asset = random.uniform(2000, 40000)
                    cash_balance = random.uniform(500, 15000)
                    user_level = "Regular"
                    kyc_status = random.choice(["verified", "pending"])
                    risk_score = random.uniform(30, 90)
                    age = random.randint(20, 65)
                    trade_count = random.randint(1, 12)
                
                # 设置时间数据
                base_date = datetime.now().date()
                
                # 新注册用户的注册时间在最近30天内
                if i < 220 and i >= 205:
                    registration_date = base_date - timedelta(days=random.randint(1, 30))
                else:
                    registration_date = base_date - timedelta(days=random.randint(30, 365))
                
                # 最近活跃用户的最后登录时间在最近7天内
                if i < 235 and i >= 220:
                    last_login_date = base_date - timedelta(days=random.randint(1, 7))
                else:
                    last_login_date = base_date - timedelta(days=random.randint(7, 90))
                
                user_basic_data.append({
                    "user_id": user_id,
                    "age": age,
                    "user_level": user_level,
                    "kyc_status": kyc_status,
                    "registration_date": registration_date,
                    "risk_score": risk_score,
                    "total_asset_value": total_asset,
                    "cash_balance": cash_balance,
                    "trade_count_30d": trade_count,
                    "last_login_date": last_login_date
                })
            
            # 转换为 DataFrame
            basic_schema = StructType([
                StructField("user_id", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("user_level", StringType(), True),
                StructField("kyc_status", StringType(), True),
                StructField("registration_date", DateType(), True),
                StructField("risk_score", DoubleType(), True),
                StructField("total_asset_value", DoubleType(), True),
                StructField("cash_balance", DoubleType(), True),
                StructField("trade_count_30d", IntegerType(), True),
                StructField("last_login_date", DateType(), True)
            ])
            
            all_data_df = self.spark.createDataFrame(user_basic_data, basic_schema)
            
            # 分别创建三个数据表
            user_basic_info = all_data_df.select(
                "user_id", "age", "user_level", "kyc_status", 
                "registration_date", "risk_score"
            )
            
            user_asset_summary = all_data_df.select(
                "user_id", "total_asset_value", "cash_balance"
            )
            
            user_activity_summary = all_data_df.select(
                "user_id", "trade_count_30d", "last_login_date"
            )
            
            logger.info(f"✅ 生成测试数据完成: {num_users} 个用户")
            
            # 根据source_name返回对应的DataFrame
            if source_name == 'user_basic_info':
                return user_basic_info
            elif source_name == 'user_asset_summary':
                return user_asset_summary
            elif source_name == 'user_activity_summary':
                return user_activity_summary
            else:
                # 如果没有指定source_name，返回合并的完整数据
                return all_data_df
            
        except Exception as e:
            logger.error(f"❌ 生成测试数据失败: {str(e)}")
            return None
