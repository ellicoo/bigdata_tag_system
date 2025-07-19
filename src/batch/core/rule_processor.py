"""
规则处理器 - 整合原有的RuleConditionParser功能
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RuleProcessor:
    """规则处理器（原RuleConditionParser功能）"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def parse_rule_conditions(self, rule_conditions: Dict[str, Any]) -> str:
        """
        解析JSON规则条件为SQL WHERE子句
        
        Args:
            rule_conditions: JSON格式的规则条件
            
        Returns:
            SQL WHERE子句字符串
        """
        try:
            if not rule_conditions or 'conditions' not in rule_conditions:
                return "1=1"  # 默认条件（所有记录）
            
            logic = rule_conditions.get('logic', 'AND')
            conditions = rule_conditions.get('conditions', [])
            
            if not conditions:
                return "1=1"
            
            # 解析每个条件
            condition_sqls = []
            for condition in conditions:
                condition_sql = self._parse_single_condition(condition)
                if condition_sql:
                    condition_sqls.append(condition_sql)
            
            if not condition_sqls:
                return "1=1"
            
            # 使用逻辑操作符连接条件
            if logic.upper() == 'OR':
                return f"({' OR '.join(condition_sqls)})"
            else:  # 默认AND
                return f"({' AND '.join(condition_sqls)})"
                
        except Exception as e:
            self.logger.error(f"解析规则条件失败: {str(e)}")
            return "1=1"  # 返回默认条件
    
    def _parse_single_condition(self, condition: Dict[str, Any]) -> str:
        """
        解析单个条件
        
        Args:
            condition: 单个条件字典
            
        Returns:
            SQL条件字符串
        """
        try:
            field = condition.get('field')
            operator = condition.get('operator')
            value = condition.get('value')
            data_type = condition.get('type', 'string')
            
            if not field or not operator:
                return ""
            
            # 根据操作符类型处理
            if operator == '=':
                return self._build_equals_condition(field, value, data_type)
            elif operator == '!=':
                return self._build_not_equals_condition(field, value, data_type)
            elif operator in ['>', '<', '>=', '<=']:
                return self._build_comparison_condition(field, operator, value, data_type)
            elif operator == 'in':
                return self._build_in_condition(field, value)
            elif operator == 'not_in':
                return self._build_not_in_condition(field, value)
            elif operator == 'in_range':
                return self._build_range_condition(field, value)
            elif operator == 'contains':
                return self._build_contains_condition(field, value)
            elif operator == 'recent_days':
                return self._build_recent_days_condition(field, value)
            elif operator == 'is_null':
                return f"{field} IS NULL"
            elif operator == 'is_not_null':
                return f"{field} IS NOT NULL"
            else:
                self.logger.warning(f"未知操作符: {operator}")
                return ""
                
        except Exception as e:
            self.logger.error(f"解析单个条件失败: {str(e)}")
            return ""
    
    def _build_equals_condition(self, field: str, value: Any, data_type: str) -> str:
        """构建等于条件"""
        if data_type == 'string':
            return f"{field} = '{value}'"
        else:
            return f"{field} = {value}"
    
    def _build_not_equals_condition(self, field: str, value: Any, data_type: str) -> str:
        """构建不等于条件"""
        if data_type == 'string':
            return f"{field} != '{value}'"
        else:
            return f"{field} != {value}"
    
    def _build_comparison_condition(self, field: str, operator: str, value: Any, data_type: str) -> str:
        """构建比较条件"""
        if data_type == 'string':
            return f"{field} {operator} '{value}'"
        else:
            return f"{field} {operator} {value}"
    
    def _build_in_condition(self, field: str, values: List[Any]) -> str:
        """构建IN条件"""
        if not values:
            return "1=0"  # 空列表返回false
        
        # 处理字符串值
        if isinstance(values[0], str):
            value_list = "', '".join(str(v) for v in values)
            return f"{field} IN ('{value_list}')"
        else:
            value_list = ", ".join(str(v) for v in values)
            return f"{field} IN ({value_list})"
    
    def _build_not_in_condition(self, field: str, values: List[Any]) -> str:
        """构建NOT IN条件"""
        if not values:
            return "1=1"  # 空列表返回true
        
        # 处理字符串值
        if isinstance(values[0], str):
            value_list = "', '".join(str(v) for v in values)
            return f"{field} NOT IN ('{value_list}')"
        else:
            value_list = ", ".join(str(v) for v in values)
            return f"{field} NOT IN ({value_list})"
    
    def _build_range_condition(self, field: str, range_values: Dict[str, Any]) -> str:
        """构建范围条件"""
        min_val = range_values.get('min')
        max_val = range_values.get('max')
        
        conditions = []
        if min_val is not None:
            conditions.append(f"{field} >= {min_val}")
        if max_val is not None:
            conditions.append(f"{field} <= {max_val}")
        
        if conditions:
            return f"({' AND '.join(conditions)})"
        else:
            return "1=1"
    
    def _build_contains_condition(self, field: str, value: str) -> str:
        """构建包含条件"""
        return f"{field} LIKE '%{value}%'"
    
    def _build_recent_days_condition(self, field: str, days: int) -> str:
        """构建最近N天条件"""
        # 计算N天前的日期
        target_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        return f"{field} >= '{target_date}'"
    
    def apply_rules(self, data, rule_conditions: Dict[str, Any]):
        """
        应用规则条件对数据进行过滤
        
        Args:
            data: PySpark DataFrame
            rule_conditions: 规则条件字典
            
        Returns:
            PySpark DataFrame: 过滤后的数据
        """
        try:
            # 解析规则条件为SQL WHERE子句
            where_clause = self.parse_rule_conditions(rule_conditions)
            
            if where_clause == "1=1":
                # 没有有效条件，返回所有数据
                self.logger.info("规则条件为空，返回所有数据")
                return data
            
            # 应用过滤条件
            self.logger.info(f"应用规则条件: {where_clause}")
            filtered_data = data.filter(where_clause)
            
            return filtered_data
            
        except Exception as e:
            self.logger.error(f"应用规则过滤失败: {str(e)}")
            # 出错时返回空数据而不是抛出异常
            return data.filter("1=0")
    
    def validate_rule_format(self, rule: Dict[str, Any]) -> bool:
        """验证规则格式是否正确"""
        required_fields = ['rule_id', 'tag_id', 'rule_conditions', 'tag_name']
        
        for field in required_fields:
            if field not in rule:
                self.logger.warning(f"规则缺少必要字段: {field}")
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
            self.logger.warning(f"规则格式验证失败: {str(e)}")
            return False