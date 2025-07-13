import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from .rule_parser import RuleConditionParser

logger = logging.getLogger(__name__)


class TagComputeEngine:
    """标签计算引擎 - 核心标签计算逻辑"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.rule_parser = RuleConditionParser()
    
    def compute_single_tag(self, data_df: DataFrame, rule: Dict[str, Any]) -> Optional[DataFrame]:
        """
        计算单个标签
        
        Args:
            data_df: 业务数据DataFrame
            rule: 标签规则字典
            
        Returns:
            包含user_id, tag_id, tag_detail的DataFrame
        """
        try:
            tag_id = rule['tag_id']
            tag_name = rule['tag_name']
            rule_conditions = rule['rule_conditions']
            
            logger.info(f"开始计算标签: {tag_name} (ID: {tag_id})")
            
            # 解析规则条件
            condition_sql = self.rule_parser.parse_rule_conditions(rule_conditions)
            logger.debug(f"生成的SQL条件: {condition_sql}")
            
            # 获取命中条件需要的字段
            hit_fields = self.rule_parser.get_condition_fields(rule_conditions)
            
            # 执行标签计算 - 筛选符合条件的用户
            tagged_users = data_df.filter(condition_sql)
            
            if tagged_users.count() == 0:
                logger.info(f"标签 {tag_name} 没有命中任何用户")
                return None
            
            # 选择需要的字段
            select_fields = ['user_id'] + [f for f in hit_fields if f in data_df.columns]
            result_df = tagged_users.select(*select_fields)
            
            # 添加标签ID
            result_df = result_df.withColumn('tag_id', F.lit(tag_id))
            
            # 生成标签详细信息
            result_df = self._add_tag_details(result_df, rule, hit_fields)
            
            hit_count = result_df.count()
            logger.info(f"✅ 标签 {tag_name} 计算完成，命中用户数: {hit_count}")
            
            return result_df.select('user_id', 'tag_id', 'tag_detail')
            
        except Exception as e:
            logger.error(f"❌ 计算标签失败: {rule.get('tag_name', 'Unknown')}, 错误: {str(e)}")
            return None
    
    def compute_batch_tags(self, data_df: DataFrame, rules: List[Dict[str, Any]]) -> List[DataFrame]:
        """
        批量计算多个标签
        
        Args:
            data_df: 业务数据DataFrame
            rules: 标签规则列表
            
        Returns:
            标签结果DataFrame列表
        """
        results = []
        
        for rule in rules:
            try:
                result_df = self.compute_single_tag(data_df, rule)
                if result_df is not None:
                    results.append(result_df)
                    
            except Exception as e:
                logger.error(f"批量计算中单个标签失败: {rule.get('tag_name', 'Unknown')}, 错误: {str(e)}")
                continue
        
        logger.info(f"批量计算完成，成功计算 {len(results)}/{len(rules)} 个标签")
        return results
    
    def _add_tag_details(self, result_df: DataFrame, rule: Dict[str, Any], hit_fields: List[str]) -> DataFrame:
        """为标签结果添加详细信息"""
        
        @F.udf(returnType=StringType())
        def generate_tag_detail(*hit_values):
            """生成标签详细信息的UDF"""
            try:
                # 生成命中原因
                reason = self._generate_hit_reason(rule['rule_conditions'], hit_fields, hit_values)
                
                # 构建标签详细信息
                detail = {
                    'value': str(hit_values[0]) if hit_values and hit_values[0] is not None else "",
                    'reason': reason,
                    'source': 'AUTO',
                    'hit_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'rule_version': '1.0',
                    'tag_name': rule['tag_name']
                }
                
                return json.dumps(detail, ensure_ascii=False)
                
            except Exception as e:
                logger.error(f"生成标签详情失败: {str(e)}")
                return json.dumps({'error': str(e)})
        
        # 获取用于生成详情的字段列
        detail_columns = []
        for field in hit_fields:
            if field in result_df.columns:
                detail_columns.append(F.col(field))
        
        # 如果没有可用字段，使用空值
        if not detail_columns:
            detail_columns = [F.lit(None)]
        
        # 添加标签详情列
        result_df = result_df.withColumn('tag_detail', generate_tag_detail(*detail_columns))
        
        return result_df
    
    def _generate_hit_reason(self, rule_conditions: Dict[str, Any], hit_fields: List[str], hit_values: tuple) -> str:
        """生成命中原因说明"""
        try:
            conditions = rule_conditions.get('conditions', [])
            logic = rule_conditions.get('logic', 'AND')
            
            reasons = []
            
            for i, condition in enumerate(conditions):
                field = condition.get('field', '')
                operator = condition.get('operator', '')
                threshold = condition.get('value', '')
                
                # 获取对应的命中值
                hit_value = ""
                if i < len(hit_values) and hit_values[i] is not None:
                    hit_value = str(hit_values[i])
                elif field in hit_fields:
                    field_index = hit_fields.index(field)
                    if field_index < len(hit_values) and hit_values[field_index] is not None:
                        hit_value = str(hit_values[field_index])
                
                # 生成单个条件的原因
                reason = self._format_single_reason(field, operator, threshold, hit_value)
                if reason:
                    reasons.append(reason)
            
            # 组合原因
            if logic.upper() == 'OR':
                return " 或 ".join(reasons)
            elif logic.upper() == 'NOT':
                return f"不满足: {' 且 '.join(reasons)}"
            else:
                return " 且 ".join(reasons)
                
        except Exception as e:
            logger.error(f"生成命中原因失败: {str(e)}")
            return "系统自动计算"
    
    def _format_single_reason(self, field: str, operator: str, threshold: Any, hit_value: str) -> str:
        """格式化单个条件的原因"""
        try:
            # 字段名映射（可以根据需要扩展）
            field_map = {
                'total_asset_value': '总资产',
                'last_30d_trading_volume': '近30日交易额',
                'login_count_7d': '近7日登录次数',
                'register_days': '注册天数',
                'total_deposit_amount': '累计充值金额',
                'kyc_status': 'KYC状态',
                'user_level': '用户等级'
            }
            
            field_name = field_map.get(field, field)
            
            if operator == '>=':
                return f"{field_name}{hit_value} ≥ {threshold}"
            elif operator == '>':
                return f"{field_name}{hit_value} > {threshold}"
            elif operator == '<=':
                return f"{field_name}{hit_value} ≤ {threshold}"
            elif operator == '<':
                return f"{field_name}{hit_value} < {threshold}"
            elif operator == '=':
                return f"{field_name}={hit_value}"
            elif operator == '!=':
                return f"{field_name}≠{threshold}"
            elif operator == 'in':
                return f"{field_name}属于{threshold}"
            elif operator == 'not_in':
                return f"{field_name}不属于{threshold}"
            elif operator == 'in_range':
                if isinstance(threshold, list) and len(threshold) == 2:
                    return f"{field_name}{hit_value}在{threshold[0]}-{threshold[1]}范围内"
            elif operator == 'recent_days':
                return f"{field_name}在最近{threshold}天内"
            elif operator == 'contains':
                return f"{field_name}包含{threshold}"
            elif operator == 'is_not_null':
                return f"{field_name}不为空"
            elif operator == 'is_null':
                return f"{field_name}为空"
            else:
                return f"{field_name} {operator} {threshold}"
                
        except Exception as e:
            logger.error(f"格式化原因失败: {str(e)}")
            return f"{field} {operator} {threshold}"
    
    def validate_data_for_rule(self, data_df: DataFrame, rule: Dict[str, Any]) -> bool:
        """验证数据是否满足规则计算要求"""
        try:
            # 检查必需字段
            if 'user_id' not in data_df.columns:
                logger.error("数据缺少user_id字段")
                return False
            
            # 检查规则需要的字段
            required_fields = self.rule_parser.get_condition_fields(rule['rule_conditions'])
            missing_fields = [f for f in required_fields if f not in data_df.columns]
            
            if missing_fields:
                logger.warning(f"数据缺少标签计算所需字段: {missing_fields}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"数据验证失败: {str(e)}")
            return False