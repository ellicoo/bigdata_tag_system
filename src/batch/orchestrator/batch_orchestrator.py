"""
批处理编排器 - 整合原有的TagScheduler功能
这是重构后的主调度器，负责协调所有批处理组件
"""

import logging
import os
import time
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.common.config.base import BaseConfig
from src.batch.core.data_loader import BatchDataLoader
from src.batch.core.tag_executor import BatchTagExecutor
from src.batch.core.result_merger import BatchResultMerger
from src.batch.core.data_writer import BatchDataWriter

logger = logging.getLogger(__name__)


class BatchOrchestrator:
    """批处理编排器（原TagScheduler重构）"""
    
    def __init__(self, config: BaseConfig, max_workers: int = 4):
        self.config = config
        self.max_workers = max_workers
        self.spark = None
        
        # 核心组件（延迟初始化）
        self.data_loader = None
        self.tag_executor = None
        self.result_merger = None
        self.data_writer = None
        
        # 缓存管理
        self.cached_existing_tags = None
    
    def initialize(self):
        """初始化组件"""
        try:
            logger.info("🚀 初始化批处理编排器...")
            
            # 初始化Spark
            self.spark = self._create_spark_session()
            
            # 初始化核心组件
            self.data_loader = BatchDataLoader(self.spark, self.config)
            self.data_loader.rule_loader.initialize()  # 初始化规则数据
            
            self.tag_executor = BatchTagExecutor(self.spark, self.config, self.data_loader, self.max_workers)
            self.result_merger = BatchResultMerger(self.spark, self.config.mysql)
            self.data_writer = BatchDataWriter(self.spark, self.config.mysql)
            
            # 预缓存现有标签数据
            self._preload_existing_tags()
            
            logger.info("✅ 批处理编排器初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 批处理编排器初始化失败: {str(e)}")
            raise
    
    def _create_spark_session(self) -> SparkSession:
        """创建Spark会话"""
        builder = SparkSession.builder
        
        # 添加基础配置
        for key, value in self.config.spark.to_dict().items():
            builder = builder.config(key, value)
        
        # 添加S3配置
        for key, value in self.config.s3.to_spark_config().items():
            builder = builder.config(key, value)
        
        # 强制添加S3相关配置以确保连接正常
        builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        builder = builder.config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        # 加载JAR文件 - 包含S3相关JAR以支持真实的S3/Hive读取
        if self.config.spark.jars:
            jar_files = self.config.spark.jars.split(',')
            existing_jars = [jar for jar in jar_files if jar and os.path.exists(jar)]
            
            if existing_jars:
                jar_paths = ",".join(existing_jars)
                logger.info(f"📦 加载JAR文件: {[os.path.basename(jar) for jar in existing_jars]}")
                builder = builder.config("spark.jars", jar_paths)
                builder = builder.config("spark.driver.extraClassPath", jar_paths)
                builder = builder.config("spark.executor.extraClassPath", jar_paths)
            else:
                logger.info("📦 JAR文件不存在，跳过JAR加载")
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")  # 更严格的日志级别，只显示错误
        
        # 关闭Spark的详细日志
        try:
            spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger("org").setLevel(
                spark.sparkContext._jvm.org.apache.log4j.Level.ERROR
            )
            spark.sparkContext._jvm.org.apache.log4j.Logger.getLogger("akka").setLevel(
                spark.sparkContext._jvm.org.apache.log4j.Level.ERROR
            )
        except:
            pass  # 忽略日志配置错误
        
        logger.info(f"Spark会话创建成功: {spark.sparkContext.applicationId}")
        return spark
    
    def _preload_existing_tags(self):
        """预缓存MySQL中的现有标签数据"""
        try:
            logger.info("🔄 预缓存MySQL现有用户标签数据...")
            self.cached_existing_tags = self.data_loader.preload_existing_tags()
            
            if self.cached_existing_tags is not None:
                cached_count = self.cached_existing_tags.count()
                logger.info(f"✅ 预缓存完成，共 {cached_count} 条记录")
            else:
                logger.info("MySQL中暂无现有用户标签数据")
            
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
            
            # 2. 检查数据加载器
            if not self.data_loader:
                logger.error("❌ 数据加载器未初始化")
                return False
            
            # 3. 检查标签执行器
            if not self.tag_executor:
                logger.error("❌ 标签执行器未初始化")
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
    
    def execute_full_workflow(self, user_filter: Optional[List[str]] = None) -> bool:
        """
        执行完整批处理工作流（原scenario_task_all_users_all_tags）
        
        Args:
            user_filter: 可选的用户ID过滤列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            start_time = time.time()
            logger.info(f"🎯 开始执行完整批处理工作流")
            logger.info(f"👥 用户过滤: {user_filter if user_filter else '全量用户'}")
            
            # 1. 使用标签执行器执行所有任务
            task_results = self.tag_executor.execute_all_tasks(user_filter)
            
            if task_results is None or task_results.count() == 0:
                logger.info("📊 全量标签任务执行完成 - 无用户符合任何标签条件 (这是正常情况)")
                return True
            
            # 2. 与MySQL已存在标签合并
            logger.info("🔄 开始与MySQL已存在标签合并...")
            final_merged = self.result_merger.merge_with_existing_tags(
                task_results, 
                self.cached_existing_tags
            )
            if final_merged is None:
                logger.error("❌ 与MySQL已存在标签合并失败")
                return False
            
            logger.info(f"✅ 与MySQL已存在标签合并完成，最终影响用户数: {final_merged.count()}")
            
            # 3. 写入MySQL
            logger.info("📝 写入合并后的标签结果到MySQL...")
            success = self.data_writer.write_tag_results(final_merged)
            
            # 4. 统计输出
            end_time = time.time()
            stats = self.data_writer.get_write_statistics()
            
            logger.info(f"""
🎉 完整批处理工作流执行完成！
🏷️  执行了所有已注册的任务类
👥 影响用户数: {final_merged.count()}
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 完整批处理工作流执行失败: {str(e)}")
            return False
    
    def execute_specific_tags_workflow(self, tag_ids: List[int]) -> bool:
        """
        执行指定标签工作流（原scenario_task_all_users_specific_tags）
        
        Args:
            tag_ids: 标签ID列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            logger.info(f"🎯 开始执行指定标签工作流 (标签ID: {tag_ids})")
            start_time = time.time()
            
            # 使用标签执行器执行指定标签任务
            result = self.tag_executor.execute_specific_tasks(tag_ids)
            
            if result is None:
                logger.warning(f"指定标签任务没有产生结果: {tag_ids}")
                return True  # 没有结果也算成功
            
            # 与MySQL现有标签合并
            merged_result = self.result_merger.merge_with_existing_tags(
                result, 
                self.cached_existing_tags
            )
            
            # 写入MySQL
            success = self.data_writer.write_tag_results(merged_result)
            
            # 统计输出
            end_time = time.time()
            stats = self.data_writer.get_write_statistics()
            
            logger.info(f"""
🎉 指定标签工作流执行完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 指定标签工作流执行失败: {str(e)}")
            return False
    
    def execute_specific_users_workflow(self, user_ids: List[str], tag_ids: List[int]) -> bool:
        """
        执行指定用户工作流（原scenario_task_specific_users_specific_tags）
        
        Args:
            user_ids: 用户ID列表
            tag_ids: 标签ID列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            logger.info(f"🎯 开始执行指定用户工作流 (用户: {user_ids}, 标签ID: {tag_ids})")
            start_time = time.time()
            
            # 使用标签执行器执行指定标签任务，传入用户过滤
            result = self.tag_executor.execute_specific_tasks(tag_ids, user_ids)
            
            if result is None:
                logger.warning(f"指定用户指定标签任务没有产生结果: 用户{user_ids}, 标签{tag_ids}")
                return True  # 没有结果也算成功
            
            # 与MySQL现有标签合并
            merged_result = self.result_merger.merge_with_existing_tags(
                result, 
                self.cached_existing_tags
            )
            
            # 写入MySQL
            success = self.data_writer.write_tag_results(merged_result)
            
            # 统计输出
            end_time = time.time()
            stats = self.data_writer.get_write_statistics()
            
            logger.info(f"""
🎉 指定用户工作流执行完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 指定用户工作流执行失败: {str(e)}")
            return False
    
    def get_available_tasks(self) -> Dict[int, str]:
        """获取所有可用的标签任务"""
        from src.batch.tasks.task_factory import TagTaskFactory
        return TagTaskFactory.get_all_available_tasks()
    
    def get_task_summary(self) -> Dict[int, Dict[str, Any]]:
        """获取任务摘要信息"""
        from src.batch.tasks.task_registry import get_task_summary
        return get_task_summary()
    
    def _cleanup_caches(self):
        """清理缓存"""
        try:
            # 清理标签执行器缓存
            if self.tag_executor:
                self.tag_executor.cleanup_cache()
            
            # 清理结果合并器缓存
            if self.result_merger:
                self.result_merger.cleanup_cache()
            
            # 清理数据加载器缓存
            if self.data_loader and self.data_loader.rule_loader:
                self.data_loader.rule_loader.cleanup()
            
            logger.info("🧹 批处理编排器缓存清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 缓存清理异常: {str(e)}")
    
    def cleanup(self):
        """清理资源"""
        try:
            # 清理所有组件缓存
            self._cleanup_caches()
            
            # 清理预缓存的标签数据
            if self.cached_existing_tags:
                self.cached_existing_tags.unpersist()
            
            # 停止Spark会话
            if self.spark:
                self.spark.stop()
            
            logger.info("✅ 批处理编排器资源清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 资源清理异常: {str(e)}")
    
    def _generate_production_like_data(self, source_name: str = None) -> DataFrame:
        """生成生产环境模拟的测试数据（保持兼容性）"""
        try:
            logger.info("📊 生成生产环境模拟测试数据...")
            
            import random
            from datetime import datetime, timedelta
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
            from pyspark.sql.functions import lit
            
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