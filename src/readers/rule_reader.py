import json
import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class RuleReader:
    """标签规则读取器 - 使用DataFrame + persist机制优化性能"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
        # 使用DataFrame缓存，支持persist机制（JOIN结果已包含完整信息）
        self._rules_df = None  # 已包含标签定义信息的完整规则DataFrame
        self._initialized = False
    
    def initialize(self):
        """一次性初始化所有规则数据，使用persist缓存"""
        if self._initialized:
            logger.info("规则读取器已初始化，使用缓存数据")
            return
        
        logger.info("🔄 开始一次性加载完整规则数据（JOIN包含标签定义）...")
        
        try:
            # 加载标签规则（JOIN已包含标签定义，无需单独缓存）
            self._load_rules_df()
            
            self._initialized = True
            logger.info("✅ 规则读取器初始化完成")
            
        except Exception as e:
            logger.error(f"❌ 规则读取器初始化失败: {str(e)}")
            raise
    
    def _load_rules_df(self):
        """加载标签规则DataFrame并persist"""
        logger.info("📖 加载标签规则...")
        
        query = """
        (SELECT 
            tr.rule_id,
            tr.tag_id,
            tr.rule_conditions,
            tr.is_active as rule_active,
            td.tag_name,
            td.tag_category,
            td.description as tag_description,
            td.is_active as tag_active
         FROM tag_rules tr 
         JOIN tag_definition td ON tr.tag_id = td.tag_id 
         WHERE tr.is_active = 1 AND td.is_active = 1) as active_rules
        """
        
        self._rules_df = self.spark.read.jdbc(
            url=self.mysql_config.jdbc_url,
            table=query,
            properties=self.mysql_config.connection_properties
        ).persist(StorageLevel.MEMORY_AND_DISK)
        
        # 触发持久化并获取统计
        rule_count = self._rules_df.count()
        logger.info(f"✅ 完整规则DataFrame已persist(内存&磁盘)，共 {rule_count} 条（包含标签定义）")
    
    def get_active_rules_df(self) -> DataFrame:
        """获取活跃标签规则DataFrame"""
        if not self._initialized:
            self.initialize()
        return self._rules_df
    
    def get_tag_definitions_df(self) -> DataFrame:
        """获取标签定义DataFrame - 直接从完整规则DataFrame提取"""
        if not self._initialized:
            self.initialize()
        # 从完整规则DataFrame中提取标签定义信息
        return self._rules_df.select("tag_id", "tag_name", "tag_category", "tag_description").distinct()
    
    def read_active_rules(self) -> List[Dict[str, Any]]:
        """读取所有启用的标签规则 - 兼容原接口，内部使用DataFrame缓存"""
        try:
            # 使用persist的DataFrame，避免重复数据库连接
            rules_df = self.get_active_rules_df()
            rules_list = rules_df.collect()
            
            logger.info(f"从缓存获取 {len(rules_list)} 条活跃标签规则")
            
            # 转换为字典列表
            result = []
            for row in rules_list:
                rule_dict = row.asDict()
                # 解析JSON规则条件
                try:
                    rule_dict['rule_conditions'] = json.loads(rule_dict['rule_conditions'])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"规则 {rule_dict['rule_id']} 的条件格式错误，跳过")
                    continue
                result.append(rule_dict)
            
            return result
            
        except Exception as e:
            logger.error(f"读取标签规则失败: {str(e)}")
            raise
    
    def read_rules_by_category(self, category_name: str) -> List[Dict[str, Any]]:
        """按分类读取标签规则"""
        all_rules = self.read_active_rules()
        return [rule for rule in all_rules if rule['tag_category'] == category_name]
    
    def get_all_required_fields(self, rules: List[Dict[str, Any]]) -> str:
        """获取规则需要的所有字段，用于数据裁剪"""
        fields_set = set(['user_id'])  # user_id是必需字段
        
        for rule in rules:
            # 从规则条件中提取字段
            try:
                conditions = rule['rule_conditions']['conditions']
                for condition in conditions:
                    if 'field' in condition:
                        fields_set.add(condition['field'])
            except (KeyError, TypeError):
                continue
        
        return ','.join(sorted(fields_set))
    
    def group_rules_by_table(self, rules: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """按数据表分组规则 - 根据规则条件中的字段推断数据源表"""
        table_groups = {}
        
        # 定义字段到表的映射关系
        field_to_table_mapping = {
            # 用户基本信息表字段
            'user_id': 'user_basic_info',
            'age': 'user_basic_info', 
            'registration_date': 'user_basic_info',
            'user_level': 'user_basic_info',
            'kyc_status': 'user_basic_info',
            'last_login_date': 'user_basic_info',
            
            # 用户资产汇总表字段
            'total_asset_value': 'user_asset_summary',
            'cash_balance': 'user_asset_summary',
            'total_deposit_amount': 'user_asset_summary',
            
            # 用户活动汇总表字段
            'trade_count_30d': 'user_activity_summary',
            'last_30d_trading_volume': 'user_activity_summary',
            'login_count_7d': 'user_activity_summary',
            'risk_score': 'user_activity_summary'
        }
        
        for rule in rules:
            try:
                # 分析规则条件中使用的字段
                conditions = rule['rule_conditions']['conditions']
                rule_tables = set()
                
                for condition in conditions:
                    field = condition.get('field', '')
                    if field in field_to_table_mapping:
                        rule_tables.add(field_to_table_mapping[field])
                
                # 如果规则跨多表，选择主表（这里简化为选择第一个表）
                # 在实际生产中，可能需要更复杂的合并逻辑
                if rule_tables:
                    primary_table = list(rule_tables)[0]
                else:
                    # 默认表（如果无法推断）
                    primary_table = 'user_basic_info'
                
                if primary_table not in table_groups:
                    table_groups[primary_table] = []
                
                table_groups[primary_table].append(rule)
                
            except (KeyError, TypeError) as e:
                logger.warning(f"规则 {rule.get('rule_id', 'unknown')} 分组失败: {str(e)}")
                # 默认分组
                if 'user_basic_info' not in table_groups:
                    table_groups['user_basic_info'] = []
                table_groups['user_basic_info'].append(rule)
        
        logger.info(f"规则按表分组结果: {[(table, len(rules)) for table, rules in table_groups.items()]}")
        return table_groups
    
    def validate_rule_format(self, rule: Dict[str, Any]) -> bool:
        """验证规则格式是否正确"""
        required_fields = ['rule_id', 'tag_id', 'rule_conditions', 'tag_name']
        
        for field in required_fields:
            if field not in rule:
                logger.warning(f"规则缺少必要字段: {field}")
                return False
        
        # 验证规则条件格式
        try:
            conditions = rule['rule_conditions']
            if not isinstance(conditions, dict):
                return False
            
            if 'conditions' not in conditions:
                return False
            
            for condition in conditions['conditions']:
                if not all(key in condition for key in ['field', 'operator', 'value']):
                    return False
            
            return True
            
        except Exception as e:
            logger.warning(f"规则格式验证失败: {str(e)}")
            return False
    
    def cleanup(self):
        """清理缓存，释放资源"""
        logger.info("🧹 清理规则读取器缓存...")
        
        try:
            if self._rules_df is not None:
                logger.info("🧹 释放规则DataFrame persist缓存")
                self._rules_df.unpersist()
                
            if self._tag_definitions_df is not None:
                logger.info("🧹 释放标签定义DataFrame persist缓存")
                self._tag_definitions_df.unpersist()
                
            # 清空引用
            self._rules_df = None
            self._tag_definitions_df = None
            self._initialized = False
            
            logger.info("✅ 规则读取器缓存清理完成")
            
        except Exception as e:
            logger.warning(f"⚠️ 规则读取器缓存清理异常: {str(e)}")
    
    def get_statistics(self) -> dict:
        """获取统计信息"""
        return {
            "total_rules": self._rules_df.count() if self._rules_df else 0,
            "total_tag_definitions": self._tag_definitions_df.count() if self._tag_definitions_df else 0,
            "initialized": self._initialized
        }