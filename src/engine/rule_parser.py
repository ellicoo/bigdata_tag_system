import logging
from typing import Dict, Any, List
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class RuleConditionParser:
    """规则条件解析器 - 将JSON规则条件转换为Spark SQL表达式"""
    
    def __init__(self):
        self.operator_mapping = {
            '=': '=',
            '!=': '!=', 
            '>': '>',
            '<': '<',
            '>=': '>=',
            '<=': '<=',
            'in': 'IN',
            'not_in': 'NOT IN',
            'is_null': 'IS NULL',
            'is_not_null': 'IS NOT NULL',
            'contains': 'LIKE',
            'not_contains': 'NOT LIKE',
            'starts_with': 'LIKE',
            'ends_with': 'LIKE',
            'in_range': 'BETWEEN',
            'not_in_range': 'NOT BETWEEN',
            'recent_days': '>=',
            'days_ago': '<=',
            'days_ago_between': 'BETWEEN'
        }
    
    def parse_rule_conditions(self, rule_conditions: Dict[str, Any]) -> str:
        """
        解析规则条件为Spark SQL WHERE子句
        
        Args:
            rule_conditions: JSON格式的规则条件
            
        Returns:
            SQL WHERE条件字符串
        """
        try:
            logic = rule_conditions.get('logic', 'AND')
            conditions = rule_conditions.get('conditions', [])
            
            if not conditions:
                logger.warning("规则条件为空")
                return "1=1"  # 返回恒真条件
            
            condition_sqls = []
            for condition in conditions:
                condition_sql = self._parse_single_condition(condition)
                if condition_sql:
                    condition_sqls.append(condition_sql)
            
            if not condition_sqls:
                return "1=1"
            
            # 组合条件
            if logic.upper() == 'OR':
                return f"({' OR '.join(condition_sqls)})"
            elif logic.upper() == 'NOT':
                return f"NOT ({' AND '.join(condition_sqls)})"
            else:  # 默认AND
                return f"({' AND '.join(condition_sqls)})"
                
        except Exception as e:
            logger.error(f"解析规则条件失败: {str(e)}")
            return "1=1"
    
    def _parse_single_condition(self, condition: Dict[str, Any]) -> str:
        """解析单个条件"""
        try:
            field = condition.get('field', '')
            operator = condition.get('operator', '')
            value = condition.get('value')
            data_type = condition.get('type', 'string')
            
            if not field or not operator:
                logger.warning(f"条件字段或操作符为空: {condition}")
                return ""
            
            return self._build_sql_condition(field, operator, value, data_type)
            
        except Exception as e:
            logger.error(f"解析单个条件失败: {condition}, 错误: {str(e)}")
            return ""
    
    def _build_sql_condition(self, field: str, operator: str, value: Any, data_type: str) -> str:
        """构建SQL条件"""
        try:
            # 数值类型条件
            if operator in ['=', '!=', '>', '<', '>=', '<=']:
                if data_type in ['number', 'int', 'float']:
                    return f"{field} {operator} {value}"
                else:
                    return f"{field} {operator} '{value}'"
            
            # 范围条件
            elif operator == 'in_range':
                if isinstance(value, list) and len(value) == 2:
                    return f"{field} BETWEEN {value[0]} AND {value[1]}"
            
            elif operator == 'not_in_range':
                if isinstance(value, list) and len(value) == 2:
                    return f"{field} NOT BETWEEN {value[0]} AND {value[1]}"
            
            # 包含条件
            elif operator == 'in':
                if isinstance(value, list):
                    value_str = "', '".join(str(v) for v in value)
                    return f"{field} IN ('{value_str}')"
                else:
                    return f"{field} IN ('{value}')"
            
            elif operator == 'not_in':
                if isinstance(value, list):
                    value_str = "', '".join(str(v) for v in value)
                    return f"{field} NOT IN ('{value_str}')"
                else:
                    return f"{field} NOT IN ('{value}')"
            
            # 空值条件
            elif operator == 'is_null':
                return f"{field} IS NULL"
            
            elif operator == 'is_not_null':
                return f"{field} IS NOT NULL"
            
            # 字符串匹配
            elif operator == 'contains':
                return f"{field} LIKE '%{value}%'"
            
            elif operator == 'not_contains':
                return f"{field} NOT LIKE '%{value}%'"
            
            elif operator == 'starts_with':
                return f"{field} LIKE '{value}%'"
            
            elif operator == 'ends_with':
                return f"{field} LIKE '%{value}'"
            
            # 时间相关条件
            elif operator == 'recent_days':
                return f"{field} >= date_sub(current_date(), {value})"
            
            elif operator == 'days_ago':
                return f"{field} <= date_sub(current_date(), {value})"
            
            elif operator == 'days_ago_between':
                if isinstance(value, list) and len(value) == 2:
                    return f"{field} BETWEEN date_sub(current_date(), {value[1]}) AND date_sub(current_date(), {value[0]})"
            
            # 日期区间
            elif operator == 'date_between':
                if isinstance(value, list) and len(value) == 2:
                    return f"{field} BETWEEN '{value[0]}' AND '{value[1]}'"
            
            else:
                logger.warning(f"不支持的操作符: {operator}")
                return ""
                
        except Exception as e:
            logger.error(f"构建SQL条件失败: field={field}, operator={operator}, value={value}, 错误: {str(e)}")
            return ""
    
    def validate_condition(self, condition: Dict[str, Any]) -> bool:
        """验证条件格式"""
        required_fields = ['field', 'operator']
        
        for field in required_fields:
            if field not in condition:
                return False
        
        # 检查操作符是否支持
        operator = condition.get('operator', '')
        if operator not in self.operator_mapping:
            logger.warning(f"不支持的操作符: {operator}")
            return False
        
        return True
    
    def get_condition_fields(self, rule_conditions: Dict[str, Any]) -> List[str]:
        """获取规则条件中涉及的所有字段"""
        fields = []
        
        try:
            conditions = rule_conditions.get('conditions', [])
            for condition in conditions:
                field = condition.get('field', '')
                if field and field not in fields:
                    fields.append(field)
        except Exception as e:
            logger.error(f"获取条件字段失败: {str(e)}")
        
        return fields