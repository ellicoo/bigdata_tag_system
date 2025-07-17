import logging
import time
import json
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession

from src.config.base import BaseConfig
from src.readers.hive_reader import HiveDataReader
from src.readers.rule_reader import RuleReader
from src.engine.tag_computer import TagComputeEngine
from src.writers.optimized_mysql_writer import OptimizedMySQLWriter
from src.merger.tag_merger import TagMerger
from src.scheduler.scenario_scheduler import ScenarioScheduler

logger = logging.getLogger(__name__)


class TagComputeScheduler:
    """标签计算主调度器"""
    
    def __init__(self, config: BaseConfig, parallel_mode=False, atomic_mode=False, max_workers=4):
        self.config = config
        self.spark = None
        self.parallel_mode = parallel_mode
        self.atomic_mode = atomic_mode
        self.max_workers = max_workers
        
        # 组件初始化 - 恢复模块化架构
        self.rule_reader = None
        self.hive_reader = None
        self.tag_engine = None
        self.mysql_writer = None
        self.tag_merger = None
        
        # 新增场景调度器
        self.scenario_scheduler = None
    
    def initialize(self):
        """初始化Spark和各个组件"""
        try:
            logger.info("开始初始化标签计算系统...")
            
            # 初始化Spark
            self.spark = self._create_spark_session()
            
            # 初始化各个组件 - 恢复模块化架构
            self.rule_reader = RuleReader(self.spark, self.config.mysql)
            self.hive_reader = HiveDataReader(self.spark, self.config.s3)
            self.tag_engine = TagComputeEngine(self.spark, self.max_workers)
            self.mysql_writer = OptimizedMySQLWriter(self.spark, self.config.mysql)
            self.tag_merger = TagMerger(self.spark, self.config.mysql)
            
            # 初始化场景调度器
            self.scenario_scheduler = ScenarioScheduler(self.config, self.max_workers)
            self.scenario_scheduler.initialize()
            
            # 一次性初始化规则数据，避免重复连接
            self.rule_reader.initialize()
            
            logger.info("✅ 标签计算系统初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 系统初始化失败: {str(e)}")
            raise
    
    def _create_spark_session(self) -> SparkSession:
        """创建Spark会话"""
        builder = SparkSession.builder
        
        # 应用Spark配置
        for key, value in self.config.spark.to_dict().items():
            builder = builder.config(key, value)
        
        # 应用S3配置
        for key, value in self.config.s3.to_spark_config().items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")  # 减少日志输出
        
        logger.info(f"Spark会话创建成功: {spark.sparkContext.applicationId}")
        return spark
    
    def run_full_tag_compute(self) -> bool:
        """运行完整的标签计算流程"""
        try:
            start_time = time.time()
            logger.info("🚀 开始执行完整标签计算...")
            
            # 1. 从规则读取器获取所有活跃的标签规则（使用persist缓存）
            rules = self.rule_reader.read_active_rules()
            if not rules:
                logger.warning("没有找到活跃的标签规则")
                return False
            
            logger.info(f"共找到 {len(rules)} 个活跃标签规则")
            
            # 2. 简化处理：直接处理所有标签规则
            all_tag_results = []
            
            try:
                # 使用本地生成的测试数据（模拟生产场景数据结构）
                logger.info("生成生产级模拟用户数据...")
                test_data = self._generate_production_like_data()
                
                # 规则已经是字典格式，直接使用
                logger.info(f"开始并行计算 {len(rules)} 个标签...")
                tag_results = self.tag_engine.compute_tags_parallel(test_data, rules)
                
                all_tag_results.extend(tag_results)
                
                logger.info(f"✅ 标签计算完成，成功计算 {len(tag_results)} 个标签")
                
            except Exception as e:
                logger.error(f"❌ 标签计算失败: {str(e)}")
                raise
            
            if not all_tag_results:
                logger.warning("没有成功计算出任何标签")
                return False
            
            # 4. 全量模式：直接使用tag_merger进行完整的标签合并（包含内存合并+数据库合并）
            logger.info("全量模式：使用tag_merger进行完整标签合并...")
            final_merged_result = self.tag_merger.merge_user_tags(all_tag_results)
            
            if final_merged_result is None:
                logger.error("标签合并失败")
                return False
            
            # 5. 写入合并后的标签结果（使用MySQL写入器）
            logger.info("开始写入标签结果...")
            # 全量模式：不需要与现有标签合并
            write_success = self.mysql_writer.write_tag_results(final_merged_result, mode="overwrite", merge_with_existing=False)
            
            if not write_success:
                logger.error("标签结果写入失败")
                return False
            
            # 7. 输出统计信息
            end_time = time.time()
            execution_time = end_time - start_time
            
            stats = self.mysql_writer.get_write_statistics()
            
            logger.info(f"""
🎉 标签计算完成！
⏱️  执行时间: {execution_time:.2f}秒
📊 统计信息:
   - 总用户数: {stats.get('total_users', 'N/A')}
   - 平均标签数/用户: {stats.get('average_tags_per_user', 'N/A')}
   - 最大标签数/用户: {stats.get('max_tags_per_user', 'N/A')}
   - 处理的标签规则数: {len(rules)}
   - 成功计算的标签数: {len(all_tag_results)}
            """)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 标签计算流程执行失败: {str(e)}")
            return False
    
    def run_incremental_compute(self, days_back: int = 1) -> bool:
        """运行增量标签计算 - 正确逻辑：识别真正的新增用户"""
        try:
            logger.info(f"🚀 开始执行增量标签计算，回溯 {days_back} 天...")
            
            # 1. 读取所有活跃的标签规则（新增用户需要计算所有标签）
            rules = self.rule_reader.read_active_rules()
            if not rules:
                logger.warning("没有找到活跃的标签规则")
                return False
            
            logger.info(f"增量模式：对新增用户计算所有 {len(rules)} 个标签规则")
            
            # 2. 识别真正的新增用户
            new_users_data = self._identify_truly_new_users(days_back)
            
            if new_users_data.count() == 0:
                logger.info("没有发现新增用户")
                return True
            
            logger.info(f"发现 {new_users_data.count()} 个真正的新增用户")
            
            # 3. 对新增用户计算所有标签规则
            all_tag_results = []
            try:
                # 计算所有标签（新用户需要全量打标签）
                tag_results = self.tag_engine.compute_tags_parallel(new_users_data, rules)
                all_tag_results.extend(tag_results)
                
                logger.info(f"✅ 为新增用户计算完成 {len(tag_results)} 个标签")
                
                # 调试：检查标签计算结果
                total_tag_records = sum(df.count() for df in tag_results)
                logger.info(f"🔍 标签计算产生 {total_tag_records} 条标签记录")
                if total_tag_records > 0 and tag_results:
                    logger.info("标签计算结果示例:")
                    tag_results[0].select("user_id", "tag_id").show(5, truncate=False)
                
            except Exception as e:
                logger.error(f"❌ 新增用户标签计算失败: {str(e)}")
                raise
            
            if not all_tag_results:
                logger.info("新增用户没有命中任何标签")
                return True
            
            # 4. 内存合并新用户的多个标签
            logger.info("内存合并新增用户多标签...")
            merged_result = self._merge_user_multi_tags_in_memory(all_tag_results)
            
            if merged_result is None:
                logger.error("新增用户标签内存合并失败")
                return False
            
            # 调试：检查合并结果
            merged_count = merged_result.count()
            logger.info(f"🔍 内存合并后得到 {merged_count} 个有标签的新增用户")
            if merged_count > 0:
                logger.info("新增用户标签示例:")
                merged_result.select("user_id", "tag_ids").show(5, truncate=False)
            
            # 5. 直接追加写入（新用户不存在于数据库中）
            logger.info("新增用户标签直接追加到数据库...")
            # 增量模式：不需要与现有标签合并（新用户）
            return self.mysql_writer.write_tag_results(merged_result, mode="append", merge_with_existing=False)
            
        except Exception as e:
            logger.error(f"增量计算失败: {str(e)}")
            return False
    
    def run_specific_tags(self, tag_ids: List[int]) -> bool:
        """运行指定标签的计算"""
        try:
            logger.info(f"🎯 开始计算指定标签: {tag_ids}")
            
            # 读取指定标签的规则
            all_rules = self.rule_reader.read_active_rules()
            target_rules = [rule for rule in all_rules if rule['tag_id'] in tag_ids]
            
            if not target_rules:
                logger.warning(f"没有找到指定标签的规则: {tag_ids}")
                return False
            
            # 简化处理：直接计算指定标签
            all_tag_results = []
            
            try:
                # 生成测试用户数据
                test_data = self._generate_test_user_data()
                
                # 计算指定标签
                tag_results = self.tag_engine.compute_batch_tags(test_data, target_rules)
                all_tag_results.extend(tag_results)
                
            except Exception as e:
                logger.error(f"计算指定标签失败: {str(e)}")
                raise
            
            if not all_tag_results:
                logger.warning("指定标签没有计算出结果")
                return False
            
            # 合并和写入
            merged_result = self.tag_merger.merge_user_tags(all_tag_results)
            if merged_result is None:
                return False
            
            # 指定标签模式：需要与现有标签合并
            return self.mysql_writer.write_tag_results(merged_result, mode="overwrite", merge_with_existing=True)
            
        except Exception as e:
            logger.error(f"指定标签计算失败: {str(e)}")
            return False
    
    def cleanup(self):
        """清理资源"""
        try:
            # 清理规则读取器的persist缓存
            if self.rule_reader:
                self.rule_reader.cleanup()
            
            if self.spark:
                self.spark.stop()
                
            logger.info("✅ 资源清理完成")
            
        except Exception as e:
            logger.warning(f"资源清理失败: {str(e)}")
    
    def _generate_production_like_data(self):
        """生成生产级模拟用户数据（符合生产场景的数据分布）"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from pyspark.sql import Row
        from datetime import date, timedelta
        import random
        
        # 定义完整的业务数据schema
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
        
        # 生成100个用户，确保能命中所有标签规则
        test_users = []
        for i in range(100):
            # 确保有足够的高净值用户
            if i < 50:  # 前50个用户都是高净值
                total_asset = random.uniform(150000, 500000)  # 确保 >= 100000
                cash_balance = random.uniform(60000, 150000)  # 确保 >= 50000
            else:
                total_asset = random.uniform(1000, 80000)
                cash_balance = random.uniform(1000, 40000)
            
            # 确保有VIP用户且KYC已验证
            if i < 20:  # 前20个是VIP客户
                user_level = random.choice(["VIP2", "VIP3"])
                kyc_status = "verified"
            else:
                user_level = random.choice(["BRONZE", "SILVER", "GOLD", "VIP1"])
                kyc_status = random.choice(["verified", "pending", "rejected"])
            
            # 确保能命中年轻用户标签
            if i < 30:
                age = random.randint(18, 30)  # 年轻用户
            else:
                age = random.randint(31, 65)
            
            # 确保有活跃交易者
            if i < 80:  # 80%是活跃交易者
                trade_count = random.randint(15, 50)  # > 10
            else:
                trade_count = random.randint(0, 8)
            
            # 确保有低风险用户
            if i < 25:
                risk_score = random.uniform(10, 28)  # <= 30
            else:
                risk_score = random.uniform(35, 80)
            
            # 确保有新注册和最近活跃用户
            if i < 15:
                registration_date = date.today() - timedelta(days=random.randint(1, 25))  # 最近30天
                last_login_date = date.today() - timedelta(days=random.randint(0, 5))    # 最近7天
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
        
        # 创建DataFrame
        test_df = self.spark.createDataFrame(test_users, schema)
        logger.info(f"生成了 {test_df.count()} 条生产级模拟数据")
        return test_df

    def _generate_test_user_data(self):
        """生成测试用户数据"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from pyspark.sql import Row
        from datetime import date, timedelta
        import random
        
        # 定义用户数据schema
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
        
        # 生成测试数据
        test_users = []
        for i in range(100):
            user_data = Row(
                user_id=f"user_{i:04d}",
                age=random.randint(18, 65),
                total_asset_value=random.uniform(1000, 500000),
                trade_count_30d=random.randint(0, 50),
                risk_score=random.uniform(10, 80),
                registration_date=date.today() - timedelta(days=random.randint(1, 365)),
                user_level=random.choice(["VIP1", "VIP2", "VIP3", "NORMAL"]),
                kyc_status=random.choice(["verified", "unverified"]),
                cash_balance=random.uniform(100, 100000),
                last_login_date=date.today() - timedelta(days=random.randint(0, 30))
            )
            test_users.append(user_data)
        
        # 创建DataFrame
        test_df = self.spark.createDataFrame(test_users, schema)
        logger.info(f"生成了 {test_df.count()} 条测试用户数据")
        return test_df
    
    def _identify_truly_new_users(self, days_back: int):
        """识别真正的新增用户：Hive中有但MySQL中没有的用户"""
        try:
            logger.info(f"🔍 开始识别最近 {days_back} 天的真正新增用户...")
            
            # 1. 获取模拟的全量Hive数据（包含新增用户）
            hive_all_users = self._generate_hive_data_with_new_users(days_back)
            
            # 2. 读取MySQL中已有的用户列表
            mysql_existing_users = self._read_existing_users_from_mysql()
            
            # 3. 对比找出新增用户（left_anti join）
            new_users = hive_all_users.join(
                mysql_existing_users,
                "user_id",
                "left_anti"  # 找出Hive中有但MySQL中没有的用户
            )
            
            new_user_count = new_users.count()
            logger.info(f"✅ 识别出 {new_user_count} 个真正的新增用户")
            
            if new_user_count > 0:
                logger.info("新增用户示例:")
                new_users.select("user_id", "registration_date").show(5, truncate=False)
            
            return new_users
            
        except Exception as e:
            logger.error(f"识别新增用户失败: {str(e)}")
            raise
    
    def _generate_hive_data_with_new_users(self, days_back: int):
        """生成包含新增用户的Hive模拟数据"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from pyspark.sql import Row
        from datetime import date, timedelta
        import random
        
        logger.info(f"生成包含新增用户的Hive模拟数据...")
        
        # 定义用户数据schema
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
        
        # 1. 添加一些现有用户（与MySQL中重复，模拟老用户）
        for i in range(1, 6):  # user_000001 到 user_000005
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
        
        # 2. 添加新增用户（MySQL中不存在）
        for i in range(10):  # 10个新用户
            # 新用户注册时间在指定天数内
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
    
    def _read_existing_users_from_mysql(self):
        """从MySQL读取已有的用户列表"""
        try:
            logger.info("📖 从MySQL读取已有用户列表...")
            
            existing_users_df = self.spark.read.jdbc(
                url=self.config.mysql.jdbc_url,
                table="user_tags",
                properties=self.config.mysql.connection_properties
            ).select("user_id").distinct()
            
            existing_count = existing_users_df.count()
            logger.info(f"MySQL中已有 {existing_count} 个用户")
            
            return existing_users_df
            
        except Exception as e:
            logger.warning(f"读取MySQL已有用户失败（可能是首次运行）: {str(e)}")
            # 返回空DataFrame
            from pyspark.sql.types import StructType, StructField, StringType
            empty_schema = StructType([StructField("user_id", StringType(), True)])
            return self.spark.createDataFrame([], empty_schema)
    
    def _generate_new_users_data(self, days_back: int):
        """生成新增用户数据（已废弃，保留兼容性）"""
        logger.warning("该方法已废弃，请使用 _identify_truly_new_users")
        return self._identify_truly_new_users(days_back)
    
    def _merge_user_multi_tags_in_memory(self, tag_results_list: List):
        """统一的用户多标签内存合并 - 将一个用户的多个并行标签在内存中合并"""
        try:
            if not tag_results_list:
                logger.warning("没有标签结果需要合并")
                return None
            
            logger.info(f"开始内存合并 {len(tag_results_list)} 个标签结果...")
            
            from pyspark.sql.functions import col, collect_list, struct, lit
            from pyspark.sql.types import ArrayType, IntegerType
            from datetime import date
            from functools import reduce
            
            # 1. 合并所有标签计算结果
            all_tags = reduce(lambda df1, df2: df1.union(df2), tag_results_list)
            
            if all_tags.count() == 0:
                logger.warning("合并后没有标签数据")
                return None
            
            logger.info(f"合并前总记录数: {all_tags.count()}")
            
            # 调试：检查是否在计算阶段就有重复
            logger.info("🔍 检查标签计算阶段是否有重复...")
            user_tag_counts = all_tags.groupBy("user_id", "tag_id").count()
            duplicates = user_tag_counts.filter(user_tag_counts["count"] > 1)
            duplicate_count = duplicates.count()
            if duplicate_count > 0:
                logger.warning(f"⚠️ 发现标签计算阶段就有重复！重复记录数: {duplicate_count}")
                duplicates.show(10, truncate=False)
            else:
                logger.info("✅ 标签计算阶段无重复")
            
            # 2. 获取标签定义信息
            enriched_tags = self._enrich_tags_with_definition_info(all_tags)
            
            # 3. 先去重，再按用户聚合（关键修复：避免标签重复）
            from pyspark.sql.functions import array_distinct, collect_set
            
            # 先去除每个用户的重复标签
            deduplicated_tags = enriched_tags.dropDuplicates(["user_id", "tag_id"])
            
            # 然后聚合成数组
            user_aggregated_tags = deduplicated_tags.groupBy("user_id").agg(
                collect_list("tag_id").alias("tag_ids_raw"),
                collect_list(struct("tag_id", "tag_name", "tag_category")).alias("tag_info_list")
            )
            
            # 对标签ID数组进行去重和排序
            user_aggregated_tags = user_aggregated_tags.select(
                "user_id",
                array_distinct("tag_ids_raw").alias("tag_ids"),
                "tag_info_list"
            )
            
            # 4. 格式化为标准的用户标签格式
            formatted_result = self._format_user_tags_output(user_aggregated_tags)
            
            logger.info(f"✅ 用户多标签内存合并完成，影响 {formatted_result.count()} 个用户")
            return formatted_result
            
        except Exception as e:
            logger.error(f"用户多标签内存合并失败: {str(e)}")
            return None
    
    def _merge_new_user_tags_in_memory(self, new_tag_results: List):
        """增量模式专用：新用户标签内存合并（复用统一逻辑）"""
        return self._merge_user_multi_tags_in_memory(new_tag_results)
    
    def _enrich_tags_with_definition_info(self, tags_df):
        """统一的标签定义信息补充方法"""
        try:
            from pyspark.sql.functions import col, lit
            
            # 读取标签定义
            tag_definitions = self.spark.read.jdbc(
                url=self.config.mysql.jdbc_url,
                table="tag_definition",
                properties=self.config.mysql.connection_properties
            ).select("tag_id", "tag_name", "tag_category")
            
            # 关联标签定义信息
            enriched_df = tags_df.join(
                tag_definitions,
                "tag_id",
                "left"
            ).select(
                "user_id",
                "tag_id", 
                col("tag_name").alias("tag_name"),
                col("tag_category").alias("tag_category"),
                "tag_detail"
            )
            
            return enriched_df
            
        except Exception as e:
            logger.error(f"丰富标签信息失败: {str(e)}")
            # 降级处理：使用默认值
            from pyspark.sql.functions import lit
            return tags_df.select(
                "user_id",
                "tag_id",
                lit("unknown").alias("tag_name"),
                lit("unknown").alias("tag_category"),
                "tag_detail"
            )
    
    def _format_user_tags_output(self, user_tags_df):
        """统一的用户标签输出格式化方法"""
        from pyspark.sql.functions import udf, col, lit
        from pyspark.sql.types import StringType
        from datetime import date
        import json
        
        # 使用UDF构建标签详情
        @udf(returnType=StringType())
        def build_tag_details(tag_info_list):
            if not tag_info_list:
                return "{}"
            
            tag_details = {}
            for tag_info in tag_info_list:
                tag_id = str(tag_info['tag_id'])
                tag_details[tag_id] = {
                    'tag_name': tag_info['tag_name'],
                    'tag_category': tag_info['tag_category']
                }
            return json.dumps(tag_details, ensure_ascii=False)
        
        formatted_df = user_tags_df.select(
            col("user_id"),
            col("tag_ids"),
            build_tag_details(col("tag_info_list")).alias("tag_details"),
            lit(date.today()).alias("computed_date")
        )
        
        return formatted_df
    
    def _enrich_new_user_tags_with_info(self, tags_df):
        """为新用户标签补充标签定义信息"""
        try:
            from pyspark.sql.functions import col, lit
            
            # 读取标签定义
            tag_definitions = self.spark.read.jdbc(
                url=self.config.mysql.jdbc_url,
                table="tag_definition",
                properties=self.config.mysql.connection_properties
            ).select("tag_id", "tag_name", "tag_category")
            
            # 关联标签定义信息
            enriched_df = tags_df.join(
                tag_definitions,
                "tag_id",
                "left"
            ).select(
                "user_id",
                "tag_id", 
                col("tag_name").alias("tag_name"),
                col("tag_category").alias("tag_category"),
                "tag_detail"
            )
            
            return enriched_df
            
        except Exception as e:
            logger.error(f"丰富新用户标签信息失败: {str(e)}")
            # 降级处理：使用默认值
            from pyspark.sql.functions import lit
            return tags_df.select(
                "user_id",
                "tag_id",
                lit("unknown").alias("tag_name"),
                lit("unknown").alias("tag_category"),
                "tag_detail"
            )
    
    def _format_new_user_final_output(self, user_tags_df):
        """格式化新用户最终输出"""
        from pyspark.sql.functions import udf, col, lit
        from pyspark.sql.types import StringType
        from datetime import date
        import json
        
        # 使用UDF构建标签详情
        @udf(returnType=StringType())
        def build_tag_details(tag_info_list):
            if not tag_info_list:
                return "{}"
            
            tag_details = {}
            for tag_info in tag_info_list:
                tag_id = str(tag_info['tag_id'])
                tag_details[tag_id] = {
                    'tag_name': tag_info['tag_name'],
                    'tag_category': tag_info['tag_category']
                }
            return json.dumps(tag_details, ensure_ascii=False)
        
        formatted_df = user_tags_df.select(
            col("user_id"),
            col("tag_ids"),
            build_tag_details(col("tag_info_list")).alias("tag_details"),
            lit(date.today()).alias("computed_date")
        )
        
        return formatted_df
    
    def _generate_incremental_data_for_table(self, table_name: str, days_back: int):
        """为指定表生成增量数据 - 本地环境专用（遗留方法，保持兼容性）"""
        # 为了简化，所有表都使用相同的用户数据结构
        # 在生产环境中，不同表会有不同的schema
        logger.info(f"为表 {table_name} 生成最近 {days_back} 天的增量数据")
        
        # 生成较少的用户数据模拟增量（比如20%的用户有变化）
        incremental_data = self._generate_production_like_data()
        
        # 模拟增量：只取部分用户，模拟最近有变化的用户
        sample_ratio = min(0.3, 1.0)  # 最多30%的用户有增量变化
        incremental_sample = incremental_data.sample(fraction=sample_ratio, seed=42)
        
        logger.info(f"表 {table_name} 增量数据包含 {incremental_sample.count()} 个用户")
        return incremental_sample
    
    # 移除了遗留的data_manager相关方法
    # 现在使用tag_merger.merge_user_tags()替代
    def health_check(self) -> bool:
        """系统健康检查"""
        try:
            logger.info("开始系统健康检查...")
            
            # 检查Spark连接
            if not self.spark or self.spark._sc._jsc is None:
                logger.error("Spark连接异常")
                return False
            
            # 检查MySQL连接
            test_df = self.spark.read.jdbc(
                url=self.config.mysql.jdbc_url,
                table="(SELECT 1 as test) as tmp",
                properties=self.config.mysql.connection_properties
            )
            
            if test_df.count() != 1:
                logger.error("MySQL连接异常")
                return False
            
            # 检查S3连接（仅在非本地环境）
            if self.config.environment != 'local':
                try:
                    test_schemas = self.hive_reader.get_table_schema("user_basic_info")
                    if not test_schemas:
                        logger.warning("S3连接或数据访问可能有问题")
                except:
                    logger.warning("S3连接检查失败")
            else:
                logger.info("💡 本地环境跳过S3连接检查，使用内存数据生成")
            
            logger.info("✅ 系统健康检查通过")
            return True
            
        except Exception as e:
            logger.error(f"❌ 系统健康检查失败: {str(e)}")
            return False
    
    def cleanup(self):
        """清理所有资源"""
        try:
            logger.info("🧹 开始清理系统资源...")
            
            # 清理Spark缓存
            if self.spark:
                try:
                    self.spark.catalog.clearCache()
                    logger.info("✅ Spark缓存已清理")
                except Exception as e:
                    logger.warning(f"⚠️ Spark缓存清理失败: {e}")
                
                # 停止Spark Session
                try:
                    self.spark.stop()
                    logger.info("✅ Spark Session已停止")
                except Exception as e:
                    logger.warning(f"⚠️ Spark Session停止失败: {e}")
            
            # 清理组件引用
            if self.scenario_scheduler:
                self.scenario_scheduler.cleanup()
            
            self.data_manager = None
            self.hive_reader = None
            self.tag_engine = None
            self.scenario_scheduler = None
            self.spark = None
            
            logger.info("✅ 系统资源清理完成")
            
        except Exception as e:
            logger.error(f"❌ 资源清理异常: {str(e)}")
            # 强制清理
            try:
                if self.spark:
                    self.spark.stop()
            except:
                pass
    
    def run_specific_users(self, user_ids: List[str]) -> bool:
        """运行指定用户的全量标签计算"""
        try:
            logger.info(f"🎯 开始计算指定用户的全量标签: {user_ids}")
            
            # 读取所有激活的标签规则
            all_rules = self.rule_reader.read_active_rules()
            if not all_rules:
                logger.warning("没有找到激活的标签规则")
                return False
            
            # 生成测试用户数据
            test_data = self._generate_test_user_data()
            
            # 过滤出指定的用户
            from pyspark.sql.functions import col
            filtered_data = test_data.filter(col("user_id").isin(user_ids))
            
            filtered_count = filtered_data.count()
            if filtered_count == 0:
                logger.warning(f"没有找到指定的用户: {user_ids}")
                return False
            
            logger.info(f"找到 {filtered_count} 个指定用户")
            
            # 计算所有标签
            all_tag_results = []
            tag_results = self.tag_engine.compute_batch_tags(filtered_data, all_rules)
            all_tag_results.extend(tag_results)
            
            if not all_tag_results:
                logger.warning("指定用户没有计算出任何标签")
                return False
            
            # 合并和写入
            merged_result = self.tag_merger.merge_user_tags(all_tag_results)
            if merged_result is None:
                return False
            
            # 指定用户模式：需要与现有标签合并
            return self.mysql_writer.write_tag_results(merged_result, mode="overwrite", merge_with_existing=True)
            
        except Exception as e:
            logger.error(f"指定用户标签计算失败: {str(e)}")
            return False
    
    def run_specific_user_tags(self, user_ids: List[str], tag_ids: List[int]) -> bool:
        """运行指定用户的指定标签计算"""
        try:
            logger.info(f"🎯 开始计算指定用户的指定标签: 用户{user_ids}, 标签{tag_ids}")
            
            # 读取指定标签的规则
            all_rules = self.rule_reader.read_active_rules()
            target_rules = [rule for rule in all_rules if rule['tag_id'] in tag_ids]
            
            if not target_rules:
                logger.warning(f"没有找到指定标签的规则: {tag_ids}")
                return False
            
            # 生成测试用户数据
            test_data = self._generate_test_user_data()
            
            # 过滤出指定的用户
            from pyspark.sql.functions import col
            filtered_data = test_data.filter(col("user_id").isin(user_ids))
            
            filtered_count = filtered_data.count()
            if filtered_count == 0:
                logger.warning(f"没有找到指定的用户: {user_ids}")
                return False
            
            logger.info(f"找到 {filtered_count} 个指定用户")
            
            # 计算指定标签
            all_tag_results = []
            tag_results = self.tag_engine.compute_batch_tags(filtered_data, target_rules)
            all_tag_results.extend(tag_results)
            
            if not all_tag_results:
                logger.warning("指定用户和标签没有计算出结果")
                return False
            
            # 合并和写入
            merged_result = self.tag_merger.merge_user_tags(all_tag_results)
            if merged_result is None:
                return False
            
            # 指定用户指定标签模式：需要与现有标签合并
            return self.mysql_writer.write_tag_results(merged_result, mode="overwrite", merge_with_existing=True)
            
        except Exception as e:
            logger.error(f"指定用户指定标签计算失败: {str(e)}")
            return False
    
    def run_incremental_specific_tags(self, days_back: int, tag_ids: List[int]) -> bool:
        """运行增量指定标签计算（新增用户，指定标签）"""
        try:
            logger.info(f"🎯 开始增量指定标签计算，回溯{days_back}天，标签: {tag_ids}")
            
            # 读取指定标签的规则
            all_rules = self.rule_reader.read_active_rules()
            target_rules = [rule for rule in all_rules if rule['tag_id'] in tag_ids]
            
            if not target_rules:
                logger.warning(f"没有找到指定标签的规则: {tag_ids}")
                return False
            
            # 识别新增用户
            new_users = self._identify_truly_new_users(days_back)
            new_user_count = new_users.count()
            
            if new_user_count == 0:
                logger.info("没有找到新增用户，跳过计算")
                return True
            
            logger.info(f"找到 {new_user_count} 个新增用户")
            
            # 计算指定标签
            all_tag_results = []
            tag_results = self.tag_engine.compute_batch_tags(new_users, target_rules)
            all_tag_results.extend(tag_results)
            
            if not all_tag_results:
                logger.warning("新增用户没有计算出指定标签")
                return True  # 没有结果也算成功
            
            # 方案2：独立内存处理 - 合并多个标签结果
            merged_result = self._merge_user_multi_tags_in_memory(all_tag_results)
            if merged_result is None:
                return False
            
            # 写入数据库（追加模式）
            # 增量指定标签模式：不需要与现有标签合并（新用户）
            return self.mysql_writer.write_tag_results(merged_result, mode="append", merge_with_existing=False)
            
        except Exception as e:
            logger.error(f"增量指定标签计算失败: {str(e)}")
            return False
    
    # ==================== 新增6个功能场景方法 ====================
    
    def run_scenario_1_full_users_full_tags(self) -> bool:
        """场景1: 全量用户打全量标签"""
        try:
            logger.info("🎯 执行场景1: 全量用户打全量标签")
            return self.scenario_scheduler.scenario_1_full_users_full_tags()
        except Exception as e:
            logger.error(f"场景1执行失败: {str(e)}")
            return False
    
    def run_scenario_2_full_users_specific_tags(self, tag_ids: List[int]) -> bool:
        """场景2: 全量用户打指定标签"""
        try:
            logger.info(f"🎯 执行场景2: 全量用户打指定标签 {tag_ids}")
            return self.scenario_scheduler.scenario_2_full_users_specific_tags(tag_ids)
        except Exception as e:
            logger.error(f"场景2执行失败: {str(e)}")
            return False
    
    def run_scenario_3_incremental_users_full_tags(self, days_back: int = 1) -> bool:
        """场景3: 增量用户打全量标签"""
        try:
            logger.info(f"🎯 执行场景3: 增量用户打全量标签（回溯{days_back}天）")
            return self.scenario_scheduler.scenario_3_incremental_users_full_tags(days_back)
        except Exception as e:
            logger.error(f"场景3执行失败: {str(e)}")
            return False
    
    def run_scenario_4_incremental_users_specific_tags(self, days_back: int, tag_ids: List[int]) -> bool:
        """场景4: 增量用户打指定标签"""
        try:
            logger.info(f"🎯 执行场景4: 增量用户打指定标签（回溯{days_back}天，标签{tag_ids}）")
            return self.scenario_scheduler.scenario_4_incremental_users_specific_tags(days_back, tag_ids)
        except Exception as e:
            logger.error(f"场景4执行失败: {str(e)}")
            return False
    
    def run_scenario_5_specific_users_full_tags(self, user_ids: List[str]) -> bool:
        """场景5: 指定用户打全量标签"""
        try:
            logger.info(f"🎯 执行场景5: 指定用户打全量标签 {user_ids}")
            return self.scenario_scheduler.scenario_5_specific_users_full_tags(user_ids)
        except Exception as e:
            logger.error(f"场景5执行失败: {str(e)}")
            return False
    
    def run_scenario_6_specific_users_specific_tags(self, user_ids: List[str], tag_ids: List[int]) -> bool:
        """场景6: 指定用户打指定标签"""
        try:
            logger.info(f"🎯 执行场景6: 指定用户打指定标签（用户{user_ids}，标签{tag_ids}）")
            return self.scenario_scheduler.scenario_6_specific_users_specific_tags(user_ids, tag_ids)
        except Exception as e:
            logger.error(f"场景6执行失败: {str(e)}")
            return False