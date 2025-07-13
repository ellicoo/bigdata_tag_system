import logging
import time
from typing import List, Dict, Any
from pyspark.sql import SparkSession

from ..config.base_config import TagSystemConfig
from ..readers.rule_reader import TagRuleReader
from ..readers.hive_reader import HiveDataReader
from ..engine.tag_computer import TagComputeEngine
from ..merger.tag_merger import TagMerger
from ..writers.mysql_writer import MySQLTagWriter

logger = logging.getLogger(__name__)


class TagComputeScheduler:
    """标签计算主调度器"""
    
    def __init__(self, config: TagSystemConfig):
        self.config = config
        self.spark = None
        
        # 组件初始化
        self.rule_reader = None
        self.hive_reader = None
        self.tag_engine = None
        self.tag_merger = None
        self.mysql_writer = None
    
    def initialize(self):
        """初始化Spark和各个组件"""
        try:
            logger.info("开始初始化标签计算系统...")
            
            # 初始化Spark
            self.spark = self._create_spark_session()
            
            # 初始化各个组件
            self.rule_reader = TagRuleReader(self.spark, self.config.mysql)
            self.hive_reader = HiveDataReader(self.spark, self.config.s3)
            self.tag_engine = TagComputeEngine(self.spark)
            self.tag_merger = TagMerger(self.spark, self.config.mysql)
            self.mysql_writer = MySQLTagWriter(self.spark, self.config.mysql)
            
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
            
            # 1. 读取所有活跃的标签规则
            rules = self.rule_reader.read_active_rules()
            if not rules:
                logger.warning("没有找到活跃的标签规则")
                return False
            
            logger.info(f"共找到 {len(rules)} 个活跃标签规则")
            
            # 2. 按数据表分组规则
            table_groups = self.rule_reader.group_rules_by_table(rules)
            
            # 3. 按表批量处理标签
            all_tag_results = []
            
            for table_name, table_rules in table_groups.items():
                logger.info(f"开始处理表: {table_name}, 标签数: {len(table_rules)}")
                
                try:
                    # 读取业务数据
                    required_fields = self.rule_reader.get_all_required_fields(table_rules)
                    business_data = self.hive_reader.read_table_data(table_name, required_fields)
                    
                    # 验证数据质量
                    if not self.hive_reader.validate_data_quality(business_data, table_name):
                        logger.warning(f"表 {table_name} 数据质量检查失败，跳过")
                        continue
                    
                    # 批量计算标签
                    table_results = self.tag_engine.compute_batch_tags(business_data, table_rules)
                    all_tag_results.extend(table_results)
                    
                    logger.info(f"✅ 表 {table_name} 处理完成，成功计算 {len(table_results)} 个标签")
                    
                except Exception as e:
                    logger.error(f"❌ 处理表 {table_name} 失败: {str(e)}")
                    continue
            
            if not all_tag_results:
                logger.warning("没有成功计算出任何标签")
                return False
            
            # 4. 合并标签结果
            logger.info("开始合并标签结果...")
            merged_result = self.tag_merger.merge_user_tags(all_tag_results)
            
            if merged_result is None:
                logger.error("标签合并失败")
                return False
            
            # 5. 验证合并结果
            if not self.tag_merger.validate_merge_result(merged_result):
                logger.error("合并结果验证失败")
                return False
            
            # 6. 写入MySQL
            logger.info("开始写入标签结果...")
            write_success = self.mysql_writer.write_tag_results(merged_result)
            
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
                    
                    # 假设各表都有updated_time或created_time字段
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
            
            # 按表分组并处理
            table_groups = self.rule_reader.group_rules_by_table(target_rules)
            all_tag_results = []
            
            for table_name, table_rules in table_groups.items():
                try:
                    required_fields = self.rule_reader.get_all_required_fields(table_rules)
                    business_data = self.hive_reader.read_table_data(table_name, required_fields)
                    
                    table_results = self.tag_engine.compute_batch_tags(business_data, table_rules)
                    all_tag_results.extend(table_results)
                    
                except Exception as e:
                    logger.error(f"处理指定标签表 {table_name} 失败: {str(e)}")
                    continue
            
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
            if self.hive_reader:
                self.hive_reader.clear_cache()
            
            if self.spark:
                self.spark.stop()
                
            logger.info("✅ 资源清理完成")
            
        except Exception as e:
            logger.warning(f"资源清理失败: {str(e)}")
    
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
            
            # 检查S3连接
            try:
                test_schemas = self.hive_reader.get_table_schema("user_basic_info")
                if not test_schemas:
                    logger.warning("S3连接或数据访问可能有问题")
            except:
                logger.warning("S3连接检查失败")
            
            logger.info("✅ 系统健康检查通过")
            return True
            
        except Exception as e:
            logger.error(f"❌ 系统健康检查失败: {str(e)}")
            return False