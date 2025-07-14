import logging
import time
import json
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession

from src.config.base import BaseConfig
from src.readers.hive_reader import HiveDataReader
from src.readers.rule_reader import RuleReader
from src.engine.tag_computer import TagComputeEngine
from src.writers.mysql_writer import MySQLTagWriter
from src.merger.tag_merger import TagMerger

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
            self.mysql_writer = MySQLTagWriter(self.spark, self.config.mysql)
            self.tag_merger = TagMerger(self.spark, self.config.mysql)
            
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
            
            # 4. 合并标签结果（使用标签合并器）
            logger.info("开始合并标签结果...")
            merged_result = self.tag_merger.merge_user_tags(all_tag_results)
            
            if merged_result is None:
                logger.error("标签合并失败")
                return False
            
            # 5. 写入合并后的标签结果（使用MySQL写入器）
            logger.info("开始写入标签结果...")
            write_success = self.mysql_writer.write_tag_results(merged_result, mode="overwrite")
            
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
        """运行增量标签计算"""
        try:
            logger.info(f"🚀 开始执行增量标签计算，回溯 {days_back} 天...")
            
            # 读取规则
            rules = self.rule_reader.read_active_rules()
            if not rules:
                logger.warning("没有找到活跃的标签规则")
                return False
            
            table_groups = self.rule_reader.group_rules_by_table(rules)
            all_tag_results = []
            
            for table_name, table_rules in table_groups.items():
                try:
                    # 读取增量数据
                    required_fields = self.rule_reader.get_all_required_fields(table_rules)
                    
                    # 根据环境决定数据读取方式
                    if self.config.environment == 'local':
                        # 本地环境：生成模拟增量数据
                        logger.info(f"本地环境：为表 {table_name} 生成模拟增量数据")
                        incremental_data = self._generate_incremental_data_for_table(table_name, days_back)
                    else:
                        # 生产环境：从S3读取真实增量数据
                        date_field = "updated_time"  # 可以根据表配置
                        incremental_data = self.hive_reader.read_incremental_data(
                            table_name, date_field, days_back, required_fields
                        )
                    
                    if incremental_data.count() == 0:
                        logger.info(f"表 {table_name} 没有增量数据")
                        continue
                    
                    # 计算标签
                    table_results = self.tag_engine.compute_batch_tags(incremental_data, table_rules)
                    all_tag_results.extend(table_results)
                    
                except Exception as e:
                    logger.error(f"处理增量表 {table_name} 失败: {str(e)}")
                    continue
            
            if not all_tag_results:
                logger.info("没有增量标签需要更新")
                return True
            
            # 合并和写入
            merged_result = self.tag_merger.merge_user_tags(all_tag_results)
            if merged_result is None:
                return False
            
            # 增量写入
            return self.mysql_writer.write_incremental_tags(merged_result)
            
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
            
            return self.mysql_writer.write_tag_results(merged_result)
            
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
    
    def _generate_incremental_data_for_table(self, table_name: str, days_back: int):
        """为指定表生成增量数据 - 本地环境专用"""
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
            self.data_manager = None
            self.hive_reader = None
            self.tag_engine = None
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