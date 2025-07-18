import logging
import time
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.config.base import BaseConfig
from src.readers.hive_reader import HiveDataReader
from src.readers.rule_reader import RuleReader
from src.engine.parallel_tag_engine import ParallelTagEngine
from src.merger.tag_merger import UnifiedTagMerger, TagMergeStrategy
from src.writers.optimized_mysql_writer import OptimizedMySQLWriter

logger = logging.getLogger(__name__)


class TagScheduler:
    """标签调度器 - 实现6个功能场景的具体逻辑"""
    
    def __init__(self, config: BaseConfig, max_workers: int = 4):
        self.config = config
        self.max_workers = max_workers
        self.spark = None
        
        # 组件初始化
        self.rule_reader = None
        self.hive_reader = None
        self.parallel_engine = None
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
            self.hive_reader = HiveDataReader(self.spark, self.config.s3)
            self.parallel_engine = ParallelTagEngine(self.spark, self.max_workers, self.config.mysql)
            self.unified_merger = UnifiedTagMerger(self.spark, self.config.mysql)
            self.mysql_writer = OptimizedMySQLWriter(self.spark, self.config.mysql)
            
            # 初始化时预缓存MySQL现有标签数据
            self.cached_existing_tags = None
            
            # 初始化规则数据
            self.rule_reader.initialize()
            
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
        
        for key, value in self.config.s3.to_spark_config().items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark会话创建成功: {spark.sparkContext.applicationId}")
        return spark
    
    def _preload_existing_tags(self):
        """预缓存MySQL中的现有标签数据"""
        try:
            logger.info("🔄 预缓存MySQL现有标签数据...")
            
            existing_df = self.spark.read.jdbc(
                url=self.config.mysql.jdbc_url,
                table="user_tags",
                properties=self.config.mysql.connection_properties
            )
            
            if existing_df.count() == 0:
                logger.info("MySQL中没有现有标签数据")
                self.cached_existing_tags = None
                return
            
            # 将JSON字符串转换为数组类型并缓存到内存+磁盘
            from pyspark.sql.functions import from_json
            from pyspark.sql.types import ArrayType, IntegerType
            from pyspark import StorageLevel
            
            processed_df = existing_df.select(
                "user_id",
                from_json(col("tag_ids"), ArrayType(IntegerType())).alias("tag_ids"),
                "tag_details",
                "created_time",
                "updated_time"
            ).persist(StorageLevel.MEMORY_AND_DISK)
            
            # 触发缓存
            existing_count = processed_df.count()
            self.cached_existing_tags = processed_df
            
            logger.info(f"✅ 成功预缓存 {existing_count} 条现有用户标签（内存+磁盘模式）")
            
        except Exception as e:
            logger.warning(f"预缓存现有标签失败: {str(e)}")
            self.cached_existing_tags = None
    
    # ==================== 6个功能场景实现 ====================
    
    def scenario_1_full_users_full_tags(self) -> bool:
        """
        场景1: 全量用户打全量标签
        - 多标签并行计算
        - 内存合并同用户多标签结果
        - 不与MySQL现有标签合并
        - 直接插入MySQL（利用唯一键约束）
        """
        try:
            logger.info("🎯 场景1: 全量用户打全量标签")
            start_time = time.time()
            
            # 1. 读取全量标签规则
            all_rules = self.rule_reader.read_active_rules()
            if not all_rules:
                logger.warning("没有活跃的标签规则")
                return False
            
            logger.info(f"加载了 {len(all_rules)} 个标签规则")
            
            # 2. 读取全量用户数据
            user_data = self._get_full_user_data()
            user_count = user_data.count()
            logger.info(f"📊 生成用户数据: {user_count} 个用户")
            
            if user_count == 0:
                logger.warning("没有用户数据")
                return False
                
            # 显示数据样例
            logger.info("用户数据样例:")
            user_data.show(3, truncate=False)
            
            # 3. 多标签并行计算 + 内存合并
            merged_result = self.parallel_engine.compute_tags_with_memory_merge(user_data, all_rules)
            if merged_result is None:
                logger.error("标签计算和内存合并失败")
                return False
            
            # 显示计算结果
            result_count = merged_result.count()
            logger.info(f"📊 标签计算结果: {result_count} 个用户有标签")
            
            if result_count > 0:
                logger.info("标签计算结果样例:")
                merged_result.show(3, truncate=False)
            
            # 4. 直接写入MySQL（不与现有标签合并）
            success = self.mysql_writer.write_tag_results(merged_result)
            
            # 5. 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 场景1完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"场景1执行失败: {str(e)}")
            return False
    
    def scenario_2_full_users_specific_tags(self, tag_ids: List[int]) -> bool:
        """
        场景2: 全量用户打指定标签
        - 多标签并行计算
        - 内存合并同用户多标签结果
        - 与MySQL现有标签合并
        - 插入MySQL（利用唯一键约束）
        """
        try:
            logger.info(f"🎯 场景2: 全量用户打指定标签 {tag_ids}")
            start_time = time.time()
            
            # 1. 读取指定标签规则
            all_rules = self.rule_reader.read_active_rules()
            target_rules = [rule for rule in all_rules if rule['tag_id'] in tag_ids]
            
            if not target_rules:
                logger.warning(f"没有找到指定标签的规则: {tag_ids}")
                return False
            
            logger.info(f"加载了 {len(target_rules)} 个指定标签规则")
            
            # 2. 读取全量用户数据
            user_data = self._get_full_user_data()
            
            # 3. 多标签并行计算 + 内存合并
            memory_merged = self.parallel_engine.compute_tags_with_memory_merge(user_data, target_rules)
            if memory_merged is None:
                logger.error("标签计算和内存合并失败")
                return False
            
            # 4. 与MySQL现有标签合并（关键差异）- 使用预缓存数据
            final_merged = self.unified_merger.advanced_merger.merge_with_existing_tags(
                memory_merged, 
                self.cached_existing_tags
            )
            if final_merged is None:
                logger.error("与现有标签合并失败")
                return False
            
            # 5. 写入MySQL
            success = self.mysql_writer.write_tag_results(final_merged)
            
            # 6. 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 场景2完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"场景2执行失败: {str(e)}")
            return False
    
    def scenario_3_incremental_users_full_tags(self, days_back: int = 1) -> bool:
        """
        场景3: 增量用户打全量标签
        - 识别新增用户
        - 多标签并行计算
        - 内存合并同用户多标签结果
        - 不与MySQL现有标签合并（新用户）
        - 直接插入MySQL
        """
        try:
            logger.info(f"🎯 场景3: 增量用户打全量标签（回溯{days_back}天）")
            start_time = time.time()
            
            # 1. 读取全量标签规则
            all_rules = self.rule_reader.read_active_rules()
            if not all_rules:
                logger.warning("没有活跃的标签规则")
                return False
            
            # 2. 识别真正的新增用户
            new_users = self._identify_truly_new_users(days_back)
            new_user_count = new_users.count()
            
            if new_user_count == 0:
                logger.info("没有发现新增用户")
                return True
            
            logger.info(f"发现 {new_user_count} 个新增用户")
            
            # 3. 多标签并行计算 + 内存合并
            merged_result = self.parallel_engine.compute_tags_with_memory_merge(new_users, all_rules)
            if merged_result is None:
                logger.warning("新增用户没有命中任何标签")
                return True
            
            # 4. 直接写入MySQL（新用户不需要与现有标签合并）
            success = self.mysql_writer.write_tag_results(merged_result)
            
            # 5. 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 场景3完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"场景3执行失败: {str(e)}")
            return False
    
    def scenario_4_incremental_users_specific_tags(self, days_back: int, tag_ids: List[int]) -> bool:
        """
        场景4: 增量用户打指定标签
        - 识别新增用户
        - 多标签并行计算
        - 内存合并同用户多标签结果
        - 不与MySQL现有标签合并（新用户）
        - 直接插入MySQL
        """
        try:
            logger.info(f"🎯 场景4: 增量用户打指定标签（回溯{days_back}天，标签{tag_ids}）")
            start_time = time.time()
            
            # 1. 读取指定标签规则
            all_rules = self.rule_reader.read_active_rules()
            target_rules = [rule for rule in all_rules if rule['tag_id'] in tag_ids]
            
            if not target_rules:
                logger.warning(f"没有找到指定标签的规则: {tag_ids}")
                return False
            
            # 2. 识别新增用户
            new_users = self._identify_truly_new_users(days_back)
            new_user_count = new_users.count()
            
            if new_user_count == 0:
                logger.info("没有发现新增用户")
                return True
            
            logger.info(f"发现 {new_user_count} 个新增用户")
            
            # 3. 多标签并行计算 + 内存合并
            merged_result = self.parallel_engine.compute_tags_with_memory_merge(new_users, target_rules)
            if merged_result is None:
                logger.warning("新增用户没有命中指定标签")
                return True
            
            # 4. 直接写入MySQL（新用户不需要与现有标签合并）
            success = self.mysql_writer.write_tag_results(merged_result)
            
            # 5. 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 场景4完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"场景4执行失败: {str(e)}")
            return False
    
    def scenario_5_specific_users_full_tags(self, user_ids: List[str]) -> bool:
        """
        场景5: 指定用户打全量标签
        - 过滤指定用户
        - 多标签并行计算
        - 内存合并同用户多标签结果
        - 不与MySQL现有标签合并
        - 直接插入MySQL
        """
        try:
            logger.info(f"🎯 场景5: 指定用户打全量标签 {user_ids}")
            start_time = time.time()
            
            # 1. 读取全量标签规则
            all_rules = self.rule_reader.read_active_rules()
            if not all_rules:
                logger.warning("没有活跃的标签规则")
                return False
            
            # 2. 获取指定用户数据
            user_data = self._get_specific_user_data(user_ids)
            filtered_count = user_data.count()
            
            if filtered_count == 0:
                logger.warning(f"没有找到指定用户: {user_ids}")
                return False
            
            logger.info(f"找到 {filtered_count} 个指定用户")
            
            # 3. 多标签并行计算 + 内存合并
            merged_result = self.parallel_engine.compute_tags_with_memory_merge(user_data, all_rules)
            if merged_result is None:
                logger.warning("指定用户没有命中任何标签")
                return True
            
            # 4. 直接写入MySQL（指定用户场景不与现有标签合并）
            success = self.mysql_writer.write_tag_results(merged_result)
            
            # 5. 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 场景5完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"场景5执行失败: {str(e)}")
            return False
    
    def scenario_6_specific_users_specific_tags(self, user_ids: List[str], tag_ids: List[int]) -> bool:
        """
        场景6: 指定用户打指定标签
        - 过滤指定用户
        - 多标签并行计算
        - 内存合并同用户多标签结果
        - 与MySQL现有标签合并
        - 插入MySQL
        """
        try:
            logger.info(f"🎯 场景6: 指定用户打指定标签（用户{user_ids}，标签{tag_ids}）")
            start_time = time.time()
            
            # 1. 读取指定标签规则
            all_rules = self.rule_reader.read_active_rules()
            target_rules = [rule for rule in all_rules if rule['tag_id'] in tag_ids]
            
            if not target_rules:
                logger.warning(f"没有找到指定标签的规则: {tag_ids}")
                return False
            
            # 2. 获取指定用户数据
            user_data = self._get_specific_user_data(user_ids)
            filtered_count = user_data.count()
            
            if filtered_count == 0:
                logger.warning(f"没有找到指定用户: {user_ids}")
                return False
            
            logger.info(f"找到 {filtered_count} 个指定用户")
            
            # 3. 多标签并行计算 + 内存合并
            memory_merged = self.parallel_engine.compute_tags_with_memory_merge(user_data, target_rules)
            if memory_merged is None:
                logger.warning("指定用户没有命中指定标签")
                return True
            
            # 4. 与MySQL现有标签合并（关键差异）- 使用预缓存数据
            final_merged = self.unified_merger.advanced_merger.merge_with_existing_tags(
                memory_merged, 
                self.cached_existing_tags
            )
            if final_merged is None:
                logger.error("与现有标签合并失败")
                return False
            
            # 5. 写入MySQL
            success = self.mysql_writer.write_tag_results(final_merged)
            
            # 6. 统计输出
            end_time = time.time()
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 场景6完成！
⏱️  执行时间: {end_time - start_time:.2f}秒
📊 统计信息: {stats}
            """)
            
            return success
            
        except Exception as e:
            logger.error(f"场景6执行失败: {str(e)}")
            return False
    
    # ==================== 辅助方法 ====================
    
    def _get_full_user_data(self):
        """获取全量用户数据"""
        if self.config.environment == 'local':
            return self._generate_production_like_data()
        else:
            # 生产环境从Hive读取
            return self.hive_reader.read_all_user_data()
    
    def _get_specific_user_data(self, user_ids: List[str]):
        """获取指定用户数据"""
        full_data = self._get_full_user_data()
        return full_data.filter(col("user_id").isin(user_ids))
    
    def _identify_truly_new_users(self, days_back: int):
        """识别真正的新增用户"""
        try:
            logger.info(f"🔍 识别最近 {days_back} 天的新增用户...")
            
            # 1. 获取包含新增用户的全量数据
            hive_all_users = self._generate_hive_data_with_new_users(days_back)
            
            # 2. 读取MySQL中已有用户
            mysql_existing_users = self._read_existing_users_from_mysql()
            
            # 3. 找出新增用户（left_anti join）
            new_users = hive_all_users.join(
                mysql_existing_users,
                "user_id",
                "left_anti"
            )
            
            new_user_count = new_users.count()
            logger.info(f"✅ 识别出 {new_user_count} 个新增用户")
            
            return new_users
            
        except Exception as e:
            logger.error(f"识别新增用户失败: {str(e)}")
            raise
    
    def _read_existing_users_from_mysql(self):
        """从MySQL读取已有用户列表"""
        try:
            existing_users_df = self.spark.read.jdbc(
                url=self.config.mysql.jdbc_url,
                table="user_tags",
                properties=self.config.mysql.connection_properties
            ).select("user_id").distinct()
            
            logger.info(f"MySQL中已有 {existing_users_df.count()} 个用户")
            return existing_users_df
            
        except Exception as e:
            logger.warning(f"读取已有用户失败（可能是首次运行）: {str(e)}")
            # 返回空DataFrame
            from pyspark.sql.types import StructType, StructField, StringType
            empty_schema = StructType([StructField("user_id", StringType(), True)])
            return self.spark.createDataFrame([], empty_schema)
    
    def _generate_production_like_data(self):
        """生成生产级模拟数据（复用原有逻辑）"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from pyspark.sql import Row
        from datetime import date, timedelta
        import random
        
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("total_asset_value", DoubleType(), True),
            StructField("trade_count_30d", IntegerType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("registration_date", DateType(), True),
            StructField("user_level", StringType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("cash_balance", DoubleType(), True),
            StructField("last_login_date", DateType(), True)
        ])
        
        test_users = []
        for i in range(100):
            # 确保有足够的高净值用户
            if i < 50:
                total_asset = random.uniform(150000, 500000)
                cash_balance = random.uniform(60000, 150000)
            else:
                total_asset = random.uniform(1000, 80000)
                cash_balance = random.uniform(1000, 40000)
            
            # 确保有VIP用户
            if i < 20:
                user_level = random.choice(["VIP2", "VIP3"])
                kyc_status = "verified"
            else:
                user_level = random.choice(["BRONZE", "SILVER", "GOLD", "VIP1"])
                kyc_status = random.choice(["verified", "pending", "rejected"])
            
            # 年龄分布
            if i < 30:
                age = random.randint(18, 30)
            else:
                age = random.randint(31, 65)
            
            # 交易活跃度
            if i < 80:
                trade_count = random.randint(15, 50)
            else:
                trade_count = random.randint(0, 8)
            
            # 风险评分
            if i < 25:
                risk_score = random.uniform(10, 28)
            else:
                risk_score = random.uniform(35, 80)
            
            # 注册和登录时间
            if i < 15:
                registration_date = date.today() - timedelta(days=random.randint(1, 25))
                last_login_date = date.today() - timedelta(days=random.randint(0, 5))
            else:
                registration_date = date.today() - timedelta(days=random.randint(40, 300))
                last_login_date = date.today() - timedelta(days=random.randint(10, 25))
                
            user_data = Row(
                user_id=f"user_{i+1:06d}",
                age=age,
                total_asset_value=total_asset,
                trade_count_30d=trade_count,
                risk_score=risk_score,
                registration_date=registration_date,
                user_level=user_level,
                kyc_status=kyc_status,
                cash_balance=cash_balance,
                last_login_date=last_login_date
            )
            test_users.append(user_data)
        
        test_df = self.spark.createDataFrame(test_users, schema)
        logger.info(f"生成了 {test_df.count()} 条生产级模拟数据")
        return test_df
    
    def _generate_hive_data_with_new_users(self, days_back: int):
        """生成包含新增用户的Hive模拟数据"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from pyspark.sql import Row
        from datetime import date, timedelta
        import random
        
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("total_asset_value", DoubleType(), True),
            StructField("trade_count_30d", IntegerType(), True),
            StructField("risk_score", DoubleType(), True),
            StructField("registration_date", DateType(), True),
            StructField("user_level", StringType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("cash_balance", DoubleType(), True),
            StructField("last_login_date", DateType(), True)
        ])
        
        all_users = []
        
        # 1. 添加现有用户（与MySQL重复）
        for i in range(1, 6):
            user_data = Row(
                user_id=f"user_{i:06d}",
                age=random.randint(25, 50),
                total_asset_value=random.uniform(50000, 300000),
                trade_count_30d=random.randint(10, 30),
                risk_score=random.uniform(20, 70),
                registration_date=date.today() - timedelta(days=random.randint(30, 365)),
                user_level=random.choice(["SILVER", "GOLD", "VIP1"]),
                kyc_status="verified",
                cash_balance=random.uniform(10000, 100000),
                last_login_date=date.today() - timedelta(days=random.randint(0, 7))
            )
            all_users.append(user_data)
        
        # 2. 添加新增用户
        for i in range(10):
            reg_date = date.today() - timedelta(days=random.randint(1, days_back))
            
            user_data = Row(
                user_id=f"new_user_{i+1:04d}",
                age=random.randint(18, 45),
                total_asset_value=random.uniform(10000, 200000),
                trade_count_30d=random.randint(5, 25),
                risk_score=random.uniform(15, 60),
                registration_date=reg_date,
                user_level=random.choice(["BRONZE", "SILVER", "GOLD", "VIP1"]),
                kyc_status=random.choice(["verified", "pending"]),
                cash_balance=random.uniform(5000, 80000),
                last_login_date=date.today() - timedelta(days=random.randint(0, 3))
            )
            all_users.append(user_data)
        
        hive_df = self.spark.createDataFrame(all_users, schema)
        logger.info(f"生成了 {hive_df.count()} 条Hive模拟数据（包含新老用户）")
        return hive_df
    
    def cleanup(self):
        """清理资源"""
        try:
            logger.info("🧹 清理场景调度器资源...")
            
            # 清理预缓存的标签数据
            if self.cached_existing_tags is not None:
                self.cached_existing_tags.unpersist()
                logger.info("✅ 清理预缓存标签数据完成")
            
            if self.unified_merger:
                self.unified_merger.cleanup()
            
            if self.rule_reader:
                self.rule_reader.cleanup()
            
            if self.spark:
                self.spark.catalog.clearCache()
                self.spark.stop()
            
            logger.info("✅ 场景调度器资源清理完成")
            
        except Exception as e:
            logger.warning(f"资源清理失败: {str(e)}")