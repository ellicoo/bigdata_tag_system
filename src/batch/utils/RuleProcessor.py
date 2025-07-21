"""
规则处理器 - 整合原有的RuleConditionParser功能
重构为驼峰命名风格，类名与文件名一致
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RuleProcessor:
    """规则处理器（原RuleConditionParser功能）"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def parseRuleConditions(self, ruleConditions: Dict[str, Any]) -> str:
        """
        解析JSON规则条件为SQL WHERE子句
        
        Args:
            ruleConditions: JSON格式的规则条件
            
        Returns:
            SQL WHERE子句字符串
        """
        try:
            if not ruleConditions or 'conditions' not in ruleConditions:
                return "1=1"  # 默认条件（所有记录）
            
            logic = ruleConditions.get('logic', 'AND')
            conditions = ruleConditions.get('conditions', [])
            
            if not conditions:
                return "1=1"
            
            # 解析每个条件
            conditionSqls = []
            for condition in conditions:
                conditionSql = self._parseSingleCondition(condition)
                if conditionSql:
                    conditionSqls.append(conditionSql)
            
            if not conditionSqls:
                return "1=1"
            
            # 使用逻辑操作符连接条件
            if logic.upper() == 'OR':
                return f"({' OR '.join(conditionSqls)})"
            else:  # 默认AND
                return f"({' AND '.join(conditionSqls)})"
                
        except Exception as e:
            self.logger.error(f"解析规则条件失败: {str(e)}")
            return "1=1"  # 返回默认条件
    
    def _parseSingleCondition(self, condition: Dict[str, Any]) -> str:
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
            dataType = condition.get('type', 'string')
            
            if not field or not operator:
                return ""
            
            # 根据操作符类型处理
            if operator == '=':
                return self._buildEqualsCondition(field, value, dataType)
            elif operator == '!=':
                return self._buildNotEqualsCondition(field, value, dataType)
            elif operator in ['>', '<', '>=', '<=']:
                return self._buildComparisonCondition(field, operator, value, dataType)
            elif operator == 'in':
                return self._buildInCondition(field, value)
            elif operator == 'not_in':
                return self._buildNotInCondition(field, value)
            elif operator == 'in_range':
                return self._buildRangeCondition(field, value)
            elif operator == 'contains':
                return self._buildContainsCondition(field, value)
            elif operator == 'recent_days':
                return self._buildRecentDaysCondition(field, value)
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
    
    def _buildEqualsCondition(self, field: str, value: Any, dataType: str) -> str:
        """构建等于条件"""
        if dataType == 'string':
            return f"{field} = '{value}'"
        else:
            return f"{field} = {value}"
    
    def _buildNotEqualsCondition(self, field: str, value: Any, dataType: str) -> str:
        """构建不等于条件"""
        if dataType == 'string':
            return f"{field} != '{value}'"
        else:
            return f"{field} != {value}"
    
    def _buildComparisonCondition(self, field: str, operator: str, value: Any, dataType: str) -> str:
        """构建比较条件"""
        if dataType == 'string':
            return f"{field} {operator} '{value}'"
        else:
            return f"{field} {operator} {value}"
    
    def _buildInCondition(self, field: str, values: List[Any]) -> str:
        """构建IN条件"""
        if not values:
            return "1=0"  # 空列表返回false
        
        # 处理字符串值
        if isinstance(values[0], str):
            valueList = "', '".join(str(v) for v in values)
            return f"{field} IN ('{valueList}')"
        else:
            valueList = ", ".join(str(v) for v in values)
            return f"{field} IN ({valueList})"
    
    def _buildNotInCondition(self, field: str, values: List[Any]) -> str:
        """构建NOT IN条件"""
        if not values:
            return "1=1"  # 空列表返回true
        
        # 处理字符串值
        if isinstance(values[0], str):
            valueList = "', '".join(str(v) for v in values)
            return f"{field} NOT IN ('{valueList}')"
        else:
            valueList = ", ".join(str(v) for v in values)
            return f"{field} NOT IN ({valueList})"
    
    def _buildRangeCondition(self, field: str, rangeValues: Dict[str, Any]) -> str:
        """构建范围条件"""
        minVal = rangeValues.get('min')
        maxVal = rangeValues.get('max')
        
        conditions = []
        if minVal is not None:
            conditions.append(f"{field} >= {minVal}")
        if maxVal is not None:
            conditions.append(f"{field} <= {maxVal}")
        
        if conditions:
            return f"({' AND '.join(conditions)})"
        else:
            return "1=1"
    
    def _buildContainsCondition(self, field: str, value: str) -> str:
        """构建包含条件"""
        return f"{field} LIKE '%{value}%'"
    
    def _buildRecentDaysCondition(self, field: str, days: int) -> str:
        """构建最近N天条件"""
        # 计算N天前的日期
        targetDate = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        return f"{field} >= '{targetDate}'"
    
    def applyRules(self, data, ruleConditions: Dict[str, Any]):
        """
        应用规则条件对数据进行过滤
        
        Args:
            data: PySpark DataFrame
            ruleConditions: 规则条件字典
            
        Returns:
            PySpark DataFrame: 过滤后的数据
        """
        try:
            # 解析规则条件为SQL WHERE子句
            whereClause = self.parseRuleConditions(ruleConditions)
            
            if whereClause == "1=1":
                # 没有有效条件，返回所有数据
                self.logger.info("规则条件为空，返回所有数据")
                return data
            
            # 应用过滤条件
            self.logger.info(f"应用规则条件: {whereClause}")
            filteredData = data.filter(whereClause)
            
            return filteredData
            
        except Exception as e:
            self.logger.error(f"应用规则过滤失败: {str(e)}")
            # 出错时返回空数据而不是抛出异常
            return data.filter("1=0")
    
    def validateRuleFormat(self, rule: Dict[str, Any]) -> bool:
        """验证规则格式是否正确"""
        requiredFields = ['rule_id', 'tag_id', 'rule_conditions', 'tag_name']
        
        for field in requiredFields:
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