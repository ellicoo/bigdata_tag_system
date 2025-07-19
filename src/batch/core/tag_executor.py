"""
标签执行器 - 整合原有的TaskBasedParallelEngine功能
"""

import logging
import json
from typing import List, Dict, Any, Optional, Set
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, collect_list, array_distinct, struct, current_timestamp, size
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from src.batch.core.data_loader import BatchDataLoader
from src.batch.core.rule_processor import RuleProcessor
from src.batch.tasks.base_tag_task import BaseTagTask
from src.batch.tasks.task_factory import TagTaskFactory
from src.batch.tasks.task_registry import register_all_tasks
from src.common.config.base import BaseConfig

logger = logging.getLogger(__name__)


class BatchTagExecutor:
    """批处理标签执行器（原TaskBasedParallelEngine功能）"""
    
    def __init__(self, spark: SparkSession, config: BaseConfig, data_loader: BatchDataLoader, max_workers: int = 4):
        self.spark = spark
        self.config = config
        self.data_loader = data_loader
        self.max_workers = max_workers
        self.rule_processor = RuleProcessor()
        self.data_cache = {}  # 数据源缓存
        
        # 确保任务已注册
        register_all_tasks()
        
        # 初始化并预缓存所有标签规则
        self._preload_all_rules()
    
    def execute_all_tasks(self, user_filter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        执行所有已注册的标签任务（全量标签）
        
        Args:
            user_filter: 可选的用户ID过滤列表
            
        Returns:
            合并后的标签结果DataFrame
        """
        try:
            # 获取所有已注册的任务ID
            all_task_ids = list(TagTaskFactory.get_all_available_tasks().keys())
            
            if not all_task_ids:
                logger.warning("没有已注册的任务，无法执行全量标签计算")
                return None
            
            logger.info(f"🎯 开始全量标签任务化计算")
            logger.info(f"📋 将执行所有已注册任务: {all_task_ids} ({len(all_task_ids)} 个任务)")
            logger.info(f"👥 用户过滤: {user_filter if user_filter else '全量用户'}")
            
            # 调用现有的执行逻辑
            return self._execute_tasks_internal(all_task_ids, user_filter)
            
        except Exception as e:
            logger.error(f"❌ 全量标签任务化计算失败: {str(e)}")
            return None
    
    def execute_specific_tasks(self, tag_ids: List[int], user_filter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        执行指定的标签任务
        
        Args:
            tag_ids: 要执行的标签任务ID列表
            user_filter: 可选的用户ID过滤列表
            
        Returns:
            合并后的标签结果DataFrame
        """
        try:
            logger.info(f"🎯 开始指定标签任务化计算")
            logger.info(f"📋 指定标签任务: {tag_ids} ({len(tag_ids)} 个任务)")
            logger.info(f"👥 用户过滤: {user_filter if user_filter else '全量用户'}")
            
            # 调用内部执行逻辑
            return self._execute_tasks_internal(tag_ids, user_filter)
                
        except Exception as e:
            logger.error(f"❌ 指定标签任务化计算失败: {str(e)}")
            return None
    
    def _execute_tasks_internal(self, task_ids: List[int], user_filter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """内部任务执行逻辑"""
        try:
            # 1. 验证指定的标签ID是否存在并显示标签ID到任务类的映射
            valid_task_ids = []
            logger.info("🔍 标签ID到任务类映射验证:")
            
            # 从DataFrame获取规则（已persist缓存）
            rules_df = self.data_loader.rule_loader.get_active_rules_df()
            
            for tag_id in task_ids:
                rule_rows = rules_df.filter(rules_df.tag_id == tag_id).collect()
                
                if rule_rows:
                    rule = self._convert_rule_row_to_config(rule_rows[0])
                    # 获取对应的任务类
                    try:
                        task_class = TagTaskFactory.get_task_class(tag_id)
                        task_class_name = task_class.__name__ if task_class else "未注册任务类"
                    except:
                        task_class_name = "未注册任务类"
                    
                    valid_task_ids.append(tag_id)
                    logger.info(f"✅ 标签ID {tag_id} → 任务类: {task_class_name}")
                    logger.info(f"   📋 标签名称: {rule['tag_name']}")
                    logger.info(f"   🏷️  标签分类: {rule.get('tag_category', 'unknown')}")
                    logger.info(f"   📜 规则条件: {json.dumps(rule.get('rule_conditions', {}), ensure_ascii=False, indent=2)}")
                    logger.info(f"   ─" * 60)
                else:
                    logger.warning(f"⚠️ 标签ID {tag_id} 不存在或未激活，跳过")
            
            if not valid_task_ids:
                logger.warning("没有有效的标签任务可执行")
                return None
            
            # 2. 只为有效的标签ID创建任务实例
            tasks = self._create_tasks(valid_task_ids)
            if not tasks:
                logger.warning("创建任务实例失败")
                return None
            
            logger.info(f"📊 成功创建 {len(tasks)} 个自包含任务实例，将并行执行")
            
            # 3. 并行执行任务（使用新的自包含架构，任务自己加载数据）
            task_results = self._execute_tasks_with_new_architecture(tasks, user_filter)
            
            # 6. 合并结果
            if task_results:
                merged_result = self._merge_task_results(task_results)
                result_count = merged_result.count() if merged_result else 0
                logger.info(f"✅ 任务化标签计算完成 - 影响用户: {result_count}")
                return merged_result
            else:
                logger.info("📊 任务化标签计算完成 - 所有任务均无符合条件的用户 (这是正常情况)")
                return None
                
        except Exception as e:
            logger.error(f"❌ 任务化标签计算失败: {str(e)}")
            return None
    
    def _preload_all_rules(self):
        """预加载标签规则"""
        try:
            # 使用data_loader的rule_loader
            self.data_loader.rule_loader.initialize()
            
            # 获取DataFrame级别的规则缓存（已persist），无需额外字典缓存
            rules_df = self.data_loader.rule_loader.get_active_rules_df()
            rule_count = rules_df.count()
            
            logger.info(f"✅ 任务引擎使用RuleReader的DataFrame缓存，{rule_count} 个标签规则可用")
            
        except Exception as e:
            logger.error(f"❌ 任务引擎预加载标签规则失败: {str(e)}")
    
    def _create_tasks(self, task_ids: List[int]) -> List[BaseTagTask]:
        """创建任务实例"""
        tasks = []
        
        # 从RuleReader获取规则DataFrame（已persist缓存）
        rules_df = self.data_loader.rule_loader.get_active_rules_df()
        
        for tag_id in task_ids:
            try:
                # 从DataFrame中过滤指定tag_id的规则
                rule_rows = rules_df.filter(rules_df.tag_id == tag_id).collect()
                
                if rule_rows:
                    rule_row = rule_rows[0]
                    # 转换为任务配置字典
                    tag_config = self._convert_rule_row_to_config(rule_row)
                    # 使用新的自包含任务架构
                    task = TagTaskFactory.create_task(tag_id, tag_config, self.spark, self.config)
                    tasks.append(task)
                    logger.debug(f"✅ 创建任务: {task.tag_name} (ID: {tag_id})")
                else:
                    logger.warning(f"⚠️ 标签ID {tag_id} 不存在或未激活")
                    
            except Exception as e:
                logger.error(f"❌ 创建任务失败 {tag_id}: {str(e)}")
        
        return tasks
    
    def _convert_rule_row_to_config(self, rule_row) -> Dict[str, Any]:
        """将DataFrame行转换为任务配置字典"""
        rule_dict = rule_row.asDict()
        # 解析JSON规则条件
        try:
            rule_dict['rule_conditions'] = json.loads(rule_dict['rule_conditions'])
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"规则 {rule_dict['rule_id']} 的条件格式错误")
            rule_dict['rule_conditions'] = {}
        
        return rule_dict
    
    def _analyze_data_requirements(self, tasks: List[BaseTagTask]) -> Dict[str, Set[str]]:
        """分析数据需求，优化读取"""
        requirements = {}
        
        for task in tasks:
            try:
                sources = task.get_data_sources()
                fields = set(task.get_required_fields())
                
                for source_name, source_path in sources.items():
                    if source_path:  # 过滤掉None值
                        if source_path not in requirements:
                            requirements[source_path] = set()
                        requirements[source_path].update(fields)
                        
            except Exception as e:
                logger.warning(f"分析任务 {task.tag_name} 数据需求失败: {str(e)}")
        
        # 转换为字典格式
        return {source: list(fields) for source, fields in requirements.items()}
    
    def _batch_load_data(self, data_requirements: Dict[str, List[str]], user_filter: Optional[List[str]] = None) -> Dict[str, DataFrame]:
        """批量读取数据，支持缓存和字段优化"""
        loaded_data = {}
        
        for source_name, required_fields in data_requirements.items():
            try:
                # 检查缓存
                cache_key = f"{source_name}_{hash(tuple(sorted(required_fields)))}"
                if cache_key in self.data_cache:
                    logger.debug(f"📦 使用缓存数据: {source_name}")
                    loaded_data[source_name] = self.data_cache[cache_key]
                    continue
                
                # 确保user_id字段包含在内
                if 'user_id' not in required_fields:
                    required_fields = ['user_id'] + required_fields
                
                logger.info(f"📖 加载数据源: {source_name}, 字段: {required_fields}")
                
                # 从数据源读取数据
                data_df = self._load_single_data_source(source_name, required_fields)
                
                if data_df is None:
                    logger.warning(f"⚠️ 数据源 {source_name} 加载失败")
                    continue
                
                # 用户过滤
                if user_filter:
                    data_df = data_df.filter(col('user_id').isin(user_filter))
                    logger.debug(f"🔍 用户过滤后记录数: {data_df.count()}")
                
                # 缓存数据
                data_df.cache()
                self.data_cache[cache_key] = data_df
                loaded_data[source_name] = data_df
                
                logger.info(f"✅ 数据源 {source_name} 加载完成")
                
            except Exception as e:
                logger.error(f"❌ 加载数据源失败 {source_name}: {str(e)}")
        
        return loaded_data
    
    def _load_single_data_source(self, source_name: str, required_fields: List[str]) -> Optional[DataFrame]:
        """加载单个数据源"""
        try:
            # 根据环境配置选择数据读取方式
            if self.config.environment == 'local':
                # 本地环境：使用内置数据生成器
                return self._load_local_data_source(source_name, required_fields)
            else:
                # Glue环境：从S3读取
                return self.data_loader.hive_loader.read_hive_table(source_name, required_fields)
                
        except Exception as e:
            logger.error(f"加载数据源失败 {source_name}: {str(e)}")
            return None
    
    def _load_local_data_source(self, source_name: str, required_fields: List[str]) -> Optional[DataFrame]:
        """本地环境数据加载"""
        # 这里可以集成现有的本地数据生成逻辑
        from src.batch.orchestrator.batch_orchestrator import BatchOrchestrator
        
        # 创建临时调度器实例来生成测试数据
        temp_orchestrator = BatchOrchestrator(self.config)
        temp_orchestrator.spark = self.spark
        
        if source_name in ['user_basic_info', 'user_asset_summary', 'user_activity_summary']:
            return temp_orchestrator._generate_production_like_data(source_name)
        else:
            logger.warning(f"未知的本地数据源: {source_name}")
            return None
    
    def _execute_tasks_parallel(self, tasks: List[BaseTagTask], loaded_data: Dict[str, DataFrame]) -> List[DataFrame]:
        """并行执行任务"""
        results = []
        failed_tasks = []
        
        logger.info(f"🚀 开始并行执行 {len(tasks)} 个标签任务")
        
        for task in tasks:
            try:
                logger.info(f"🔄 执行任务类: {task.__class__.__name__} (标签ID: {task.tag_id})")
                logger.info(f"   📋 标签名称: {task.tag_name}")
                
                # 从缓存中获取并显示MySQL规则
                mysql_rule = self._get_task_rule(task.tag_id)
                if not mysql_rule or 'rule_conditions' not in mysql_rule:
                    logger.warning(f"⚠️ 任务类 {task.__class__.__name__} 无法从缓存获取MySQL规则")
                    failed_tasks.append(task.__class__.__name__)
                    continue
                
                logger.info(f"   📜 从缓存获取标签规则 (ID: {task.tag_id}):")
                logger.info(f"   📋 规则JSON: {json.dumps(mysql_rule['rule_conditions'], ensure_ascii=False, indent=6)}")
                
                # 获取任务所需数据
                task_data = self._prepare_task_data(task, loaded_data)
                if task_data is None:
                    logger.warning(f"⚠️ 任务类 {task.__class__.__name__} 无法获取所需数据")
                    failed_tasks.append(task.__class__.__name__)
                    continue
                
                # 验证数据
                if not task.validate_data(task_data):
                    logger.warning(f"⚠️ 任务类 {task.__class__.__name__} 数据验证失败")
                    failed_tasks.append(task.__class__.__name__)
                    continue
                
                # 预处理数据
                processed_data = task.preprocess_data(task_data)
                logger.info(f"   📊 数据预处理完成，记录数: {processed_data.count()}")
                
                # 执行标签计算
                tagged_users = self._compute_single_task(processed_data, task, mysql_rule['rule_conditions'])
                
                # 后处理
                if tagged_users and tagged_users.count() > 0:
                    final_result = task.post_process_result(tagged_users)
                    results.append(final_result)
                    logger.info(f"✅ 任务类 {task.__class__.__name__} 执行成功，命中用户: {final_result.count()}")
                    
            except Exception as e:
                logger.error(f"❌ 任务类 {task.__class__.__name__} 执行失败: {str(e)}")
                failed_tasks.append(task.__class__.__name__)
        
        logger.info(f"🎉 并行执行完成 - 成功: {len(results)}, 失败: {len(failed_tasks)}")
        if failed_tasks:
            logger.warning(f"失败的任务: {failed_tasks}")
        
        return results
    
    def _execute_tasks_with_new_architecture(self, tasks: List[BaseTagTask], user_filter: Optional[List[str]]) -> List[DataFrame]:
        """使用新的自包含任务架构并行执行任务"""
        results = []
        failed_tasks = []
        
        logger.info(f"🚀 开始并行执行 {len(tasks)} 个自包含任务")
        
        for task in tasks:
            try:
                logger.info(f"🔄 执行自包含任务: {task.__class__.__name__} (标签ID: {task.tag_id})")
                
                # 直接调用任务的execute方法，任务自己负责数据加载和规则处理
                result = task.execute(user_filter)
                
                if result is not None and result.count() > 0:
                    results.append(result)
                    logger.info(f"✅ 任务 {task.__class__.__name__} 执行成功，命中用户: {result.count()}")
                else:
                    logger.info(f"📊 任务 {task.__class__.__name__} 无用户命中条件")
                    
            except Exception as e:
                logger.error(f"❌ 任务 {task.__class__.__name__} 执行失败: {str(e)}")
                failed_tasks.append(task.__class__.__name__)
        
        logger.info(f"🎉 自包含任务并行执行完成 - 成功: {len(results)}, 失败: {len(failed_tasks)}")
        if failed_tasks:
            logger.warning(f"失败的任务: {failed_tasks}")
        
        return results
    
    def _get_task_rule(self, tag_id: int) -> Optional[Dict[str, Any]]:
        """从DataFrame缓存中获取任务对应的标签规则"""
        try:
            rules_df = self.data_loader.rule_loader.get_active_rules_df()
            rule_rows = rules_df.filter(rules_df.tag_id == tag_id).collect()
            
            if rule_rows:
                return self._convert_rule_row_to_config(rule_rows[0])
            else:
                return None
                
        except Exception as e:
            logger.error(f"获取标签规则失败 {tag_id}: {str(e)}")
            return None
    
    def _prepare_task_data(self, task: BaseTagTask, loaded_data: Dict[str, DataFrame]) -> Optional[DataFrame]:
        """为任务准备数据"""
        try:
            data_sources = task.get_data_sources()
            primary_source = data_sources.get('primary')
            
            if not primary_source or primary_source not in loaded_data:
                logger.error(f"任务 {task.tag_name} 的主数据源 {primary_source} 不可用")
                return None
            
            # 获取主数据源
            task_data = loaded_data[primary_source]
            
            # 如果有辅助数据源，进行JOIN
            secondary_source = data_sources.get('secondary')
            if secondary_source and secondary_source in loaded_data:
                secondary_data = loaded_data[secondary_source]
                task_data = task_data.join(secondary_data, 'user_id', 'left')
                logger.debug(f"任务 {task.tag_name} 合并了辅助数据源")
            
            return task_data
            
        except Exception as e:
            logger.error(f"准备任务数据失败 {task.tag_name}: {str(e)}")
            return None
    
    def _compute_single_task(self, data_df: DataFrame, task: BaseTagTask, rules: Dict[str, Any]) -> Optional[DataFrame]:
        """计算单个任务"""
        try:
            logger.info(f"   🔍 开始计算任务类: {task.__class__.__name__} (标签ID: {task.tag_id})")
            
            # 解析规则条件
            condition_sql = self.rule_processor.parse_rule_conditions(rules)
            logger.info(f"   📝 生成SQL条件: {condition_sql}")
            
            # 执行标签计算
            tagged_users = data_df.filter(condition_sql)
            
            hit_count = tagged_users.count()
            total_users = data_df.count()
            
            logger.info(f"   📊 标签计算结果:")
            logger.info(f"      👥 总用户数: {total_users}")
            logger.info(f"      ✅ 符合条件: {hit_count}")
            logger.info(f"      📈 命中率: {(hit_count/total_users*100):.2f}%" if total_users > 0 else "      📈 命中率: 0%")
            
            if hit_count == 0:
                logger.info(f"   💡 任务类 {task.__class__.__name__}: 无用户满足标签条件 (正常业务结果)")
                return None
            
            # 添加标签信息
            result_df = tagged_users.select('user_id').withColumn('tag_id', lit(task.tag_id))
            
            # 生成标签详情
            result_df = self._add_task_details(result_df, task)
            
            logger.info(f"   ✅ 任务类 {task.__class__.__name__} 计算完成，命中用户: {hit_count}")
            return result_df.select('user_id', 'tag_id', 'tag_detail')
            
        except Exception as e:
            logger.error(f"   ❌ 任务类 {task.__class__.__name__} 计算失败: {str(e)}")
            return None
    
    def _add_task_details(self, result_df: DataFrame, task: BaseTagTask) -> DataFrame:
        """为任务结果添加详细信息"""
        @udf(returnType=StringType())
        def generate_task_detail():
            """生成任务详细信息的UDF"""
            detail = {
                'tag_name': task.tag_name,
                'tag_category': task.tag_category,
                'source': 'BATCH_ENGINE'
            }
            return json.dumps(detail, ensure_ascii=False)
        
        return result_df.withColumn('tag_detail', generate_task_detail())
    
    def _merge_task_results(self, task_results: List[DataFrame]) -> Optional[DataFrame]:
        """合并多个并行任务的内存标签结果"""
        try:
            if not task_results:
                return None
            
            logger.info(f"🔄 开始合并 {len(task_results)} 个并行任务的内存标签结果...")
            
            # 1. 使用内置的内存合并逻辑
            merged_result = self._merge_user_tags_in_memory(task_results)
            
            if merged_result:
                logger.info(f"✅ 多个并行任务的内存标签合并完成，影响 {merged_result.count()} 个用户")
                return merged_result
            else:
                logger.info("📊 多个并行任务的内存标签合并完成，无用户数据")
                return None
            
        except Exception as e:
            logger.error(f"❌ 合并多个并行任务的内存标签失败: {str(e)}")
            return None
    
    def _merge_user_tags_in_memory(self, tag_results: List[DataFrame]) -> Optional[DataFrame]:
        """内存合并：将同一用户的多个标签合并为一条记录"""
        try:
            if not tag_results:
                return None
                
            logger.info(f"开始内存合并 {len(tag_results)} 个标签结果...")
            
            # 记录每个任务的标签结果用于追踪
            for i, task_df in enumerate(tag_results):
                task_count = task_df.count()
                if task_count > 0:
                    logger.info(f"   📋 任务 {i+1}: {task_count} 个用户命中")
                else:
                    logger.info(f"   📋 任务 {i+1}: 无用户命中")
            
            from functools import reduce
            from datetime import date
            
            # 1. 合并所有标签结果
            all_tags = reduce(lambda df1, df2: df1.union(df2), tag_results)
            
            if all_tags.count() == 0:
                logger.warning("合并后没有标签数据")
                return None
            
            # 2. 去重：移除同一用户的重复标签
            deduplicated_tags = all_tags.dropDuplicates(["user_id", "tag_id"])
            
            # 记录去重前后的对比
            original_count = all_tags.count()
            deduplicated_count = deduplicated_tags.count()
            if original_count != deduplicated_count:
                logger.info(f"   🔄 去重处理: {original_count} → {deduplicated_count} 条记录 (去除 {original_count - deduplicated_count} 重复)")
            else:
                logger.info(f"   ✅ 无重复标签: {deduplicated_count} 条记录")
            
            # 3. 丰富标签信息
            enriched_tags = self._enrich_with_tag_info(deduplicated_tags)
            
            # 4. 按用户聚合：将用户的多个标签合并为数组
            user_aggregated = enriched_tags.groupBy("user_id").agg(
                collect_list("tag_id").alias("tag_ids_raw"),
                collect_list(struct("tag_id", "tag_name", "tag_category")).alias("tag_info_list")
            )
            
            # 5. 确保标签数组去重
            user_aggregated = user_aggregated.select(
                "user_id",
                array_distinct("tag_ids_raw").alias("tag_ids"),
                "tag_info_list"
            )
            
            # 5.5. 统计用户和多标签用户情况
            total_users = user_aggregated.count()
            multi_tag_users = user_aggregated.filter(size("tag_ids") > 1)
            multi_tag_count = multi_tag_users.count()
            
            logger.info(f"📊 总共命中标签的用户数: {total_users} 个")
            if multi_tag_count > 0:
                logger.info(f"📊 命中多个标签的用户数: {multi_tag_count} 个")
                
                # 抽样显示命中多个标签的用户
                sample_multi_users = multi_tag_users.limit(3).collect()
                logger.info("📋 抽样显示命中多个标签的用户:")
                for user_row in sample_multi_users:
                    user_id = user_row.user_id
                    tag_ids = user_row.tag_ids
                    logger.info(f"   👤 用户 {user_id}: 标签ID {tag_ids}")
            else:
                logger.info("📊 本轮计算中所有用户都只命中单个标签")
            
            # 6. 内存合并过程详细追踪
            logger.info("📊 内存合并详细过程（前3个用户）:")
            sample_memory_merged = user_aggregated.limit(3).collect()
            
            for user_row in sample_memory_merged:
                user_id = user_row.user_id
                final_tag_ids = user_row.tag_ids
                
                # 显示该用户各个任务的原始标签
                logger.info(f"   👤 用户 {user_id} 内存合并过程:")
                
                # 查找该用户在各个任务中的标签
                user_task_tags = []
                for i, task_df in enumerate(tag_results):
                    user_tags_in_task = task_df.filter(col("user_id") == user_id).collect()
                    if user_tags_in_task:
                        # 去重处理：使用set去除重复的tag_id
                        task_tag_ids = list(set([row.tag_id for row in user_tags_in_task]))
                        user_task_tags.extend(task_tag_ids)
                        logger.info(f"      📋 任务 {i+1} 原始标签: {task_tag_ids}")
                    else:
                        logger.info(f"      📋 任务 {i+1} 原始标签: 无")
                
                logger.info(f"      🔄 合并前所有标签: {user_task_tags}")
                logger.info(f"      ✅ 内存合并后标签: {final_tag_ids}")
                
                # 验证合并逻辑
                expected_merged = sorted(list(set(user_task_tags)))
                actual_merged = sorted(final_tag_ids)
                if expected_merged == actual_merged:
                    logger.info(f"      ✅ 内存合并逻辑正确")
                else:
                    logger.info(f"      ❌ 内存合并逻辑异常 - 期望: {expected_merged}, 实际: {actual_merged}")
                
                logger.info(f"      ─" * 50)
            
            # 7. 格式化输出
            final_result = self._format_memory_merge_output(user_aggregated)
            
            return final_result
            
        except Exception as e:
            logger.error(f"内存合并失败: {str(e)}")
            return None
    
    def _enrich_with_tag_info(self, tags_df: DataFrame) -> DataFrame:
        """丰富标签信息"""
        try:
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
            # 降级处理
            return tags_df.select(
                "user_id",
                "tag_id",
                lit("unknown").alias("tag_name"),
                lit("unknown").alias("tag_category"),
                "tag_detail"
            )
    
    def _format_memory_merge_output(self, user_tags_df: DataFrame) -> DataFrame:
        """格式化内存合并输出"""
        from datetime import date
        
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
    
    def cleanup_cache(self):
        """清理数据缓存"""
        for cache_key, cached_df in self.data_cache.items():
            try:
                cached_df.unpersist()
            except:
                pass
        
        self.data_cache.clear()
        logger.info("🧹 任务引擎数据缓存已清理")