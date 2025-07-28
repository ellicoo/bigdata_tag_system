#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签规则解析器
负责分析标签规则依赖、智能分组、SQL生成等核心逻辑
"""
import json
from typing import List, Dict, Set, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


class TagRuleParser:
    """标签规则解析器
    
    职责：
    1. 分析标签规则的表依赖关系
    2. 智能分组：相同表依赖的标签归为一组
    3. 提取所需字段信息
    4. 生成SQL查询条件
    """
    
    def __init__(self):
        """初始化规则解析器"""
        print("🔍 TagRuleParser初始化完成")
    
    def analyzeDependencies(self, rulesDF: DataFrame) -> Dict[int, Set[str]]:
        """分析所有标签规则的表依赖关系
        
        Args:
            rulesDF: 标签规则DataFrame，包含tag_id和rule_conditions字段
            
        Returns:
            Dict[int, Set[str]]: 标签ID到依赖表集合的映射
        """
        print("🔍 分析标签规则依赖关系...")
        
        dependencies = {}
        
        # 收集所有规则到Driver进行分析
        rules = rulesDF.select("tag_id", "rule_conditions").collect()
        
        for row in rules:
            tagId = row['tag_id']
            ruleJson = row['rule_conditions']
            
            tables = self._extractTablesFromRule(ruleJson)
            dependencies[tagId] = tables
            
            print(f"   📋 标签 {tagId}: 依赖表 {list(tables)}")
        
        print(f"✅ 依赖分析完成: {len(dependencies)} 个标签")
        return dependencies
    
    def analyzeFieldDependencies(self, rulesDF: DataFrame) -> Dict[str, Set[str]]:
        """分析字段依赖关系
        
        Args:
            rulesDF: 标签规则DataFrame
            
        Returns:
            Dict[str, Set[str]]: 表名到字段集合的映射
        """
        print("🔍 分析字段依赖关系...")
        
        fieldDependencies = {}
        
        # 收集所有规则进行分析
        rules = rulesDF.select("tag_id", "rule_conditions").collect()
        
        for row in rules:
            ruleJson = row['rule_conditions']
            tableFields = self._extractFieldsFromRule(ruleJson)
            
            # 合并字段依赖
            for tableName, fields in tableFields.items():
                if tableName not in fieldDependencies:
                    fieldDependencies[tableName] = set()
                fieldDependencies[tableName].update(fields)
        
        # 确保每个表都包含user_id（JOIN需要）
        for tableName in fieldDependencies:
            fieldDependencies[tableName].add("user_id")
        
        print(f"✅ 字段依赖分析完成: {len(fieldDependencies)} 个表")
        for tableName, fields in fieldDependencies.items():
            print(f"   📊 {tableName}: {list(fields)}")
        
        return fieldDependencies
    
    def groupTagsByTables(self, dependencies: Dict[int, Set[str]]) -> List['TagGroup']:
        """智能分组：相同表依赖的标签归为一组
        
        Args:
            dependencies: 标签依赖关系字典
            
        Returns:
            List[TagGroup]: 标签分组列表
        """
        print("🎯 智能分组标签...")
        
        # 按表组合进行分组
        tableGroups = {}
        
        for tagId, tables in dependencies.items():
            # 使用排序后的表名组合作为分组key
            tableKey = tuple(sorted(tables))
            
            if tableKey not in tableGroups:
                tableGroups[tableKey] = []
            
            tableGroups[tableKey].append(tagId)
        
        # 创建TagGroup对象
        from ..engine.TagGroup import TagGroup
        groups = []
        
        for tableKey, tagIds in tableGroups.items():
            group = TagGroup(tagIds, list(tableKey))
            groups.append(group)
            
            print(f"   🏷️  组 {len(groups)}: 标签{tagIds} → 表{list(tableKey)}")
        
        print(f"✅ 分组完成: {len(groups)} 个计算组")
        return groups
    
    def parseRuleToSql(self, ruleJson: str) -> str:
        """将JSON规则解析为SQL WHERE条件
        
        Args:
            ruleJson: JSON格式的规则字符串
            
        Returns:
            str: SQL WHERE条件
        """
        if not ruleJson:
            return "1=0"
        
        try:
            rule = json.loads(ruleJson)
            return self._parseRuleToSqlLogic(rule)
        except Exception as e:
            print(f"❌ 规则解析失败: {e}")
            return "1=0"
    
    def _extractTablesFromRule(self, ruleJson: str) -> Set[str]:
        """从规则JSON中提取所需的表名"""
        if not ruleJson:
            return set()
        
        try:
            rule = json.loads(ruleJson)
            tables = set()
            self._extractTablesRecursive(rule, tables)
            return tables
        except Exception as e:
            print(f"❌ 提取表名失败: {e}")
            return set()
    
    def _extractFieldsFromRule(self, ruleJson: str) -> Dict[str, Set[str]]:
        """从规则JSON中提取字段依赖"""
        if not ruleJson:
            return {}
        
        try:
            rule = json.loads(ruleJson)
            tableFields = {}
            self._extractFieldsRecursive(rule, tableFields)
            return tableFields
        except Exception as e:
            print(f"❌ 提取字段失败: {e}")
            return {}
    
    def _extractTablesRecursive(self, rule: dict, tables: Set[str]):
        """递归提取规则中的所有表名"""
        conditions = rule.get("conditions", [])
        
        for condition in conditions:
            if "condition" in condition:
                # 嵌套条件，递归处理
                self._extractTablesRecursive(condition["condition"], tables)
            elif "fields" in condition:
                # 字段条件，提取表名
                for field in condition["fields"]:
                    tableName = field.get("table")
                    if tableName:
                        tables.add(tableName)
    
    def _extractFieldsRecursive(self, rule: dict, tableFields: Dict[str, Set[str]]):
        """递归提取规则中的字段依赖"""
        conditions = rule.get("conditions", [])
        
        for condition in conditions:
            if "condition" in condition:
                # 嵌套条件，递归处理
                self._extractFieldsRecursive(condition["condition"], tableFields)
            elif "fields" in condition:
                # 字段条件，提取表名和字段名
                for field in condition["fields"]:
                    tableName = field.get("table")
                    fieldName = field.get("field")
                    
                    if tableName and fieldName:
                        if tableName not in tableFields:
                            tableFields[tableName] = set()
                        tableFields[tableName].add(fieldName)
    
    def _parseRuleToSqlLogic(self, rule: dict) -> str:
        """核心：将JSON规则解析为SQL逻辑"""
        logic = rule.get("logic", "AND")
        conditions = rule.get("conditions", [])
        
        if not conditions:
            return "1=1"
        
        sqlParts = []
        for condition in conditions:
            if "condition" in condition:
                # 嵌套条件
                nestedSql = self._parseRuleToSqlLogic(condition["condition"])
                sqlParts.append(f"({nestedSql})")
            elif "fields" in condition:
                # 字段条件
                fieldSqls = []
                for field in condition["fields"]:
                    fieldSql = self._parseFieldToSql(field)
                    fieldSqls.append(fieldSql)
                if fieldSqls:
                    sqlParts.append(f"({' AND '.join(fieldSqls)})")
        
        if not sqlParts:
            return "1=1"
        
        connector = f" {logic} "
        return connector.join(sqlParts)
    
    def _parseFieldToSql(self, field: dict) -> str:
        """解析单个字段条件为SQL"""
        table = field.get("table", "")
        fieldName = field.get("field", "")
        operator = field.get("operator", "=")
        value = field.get("value")
        fieldType = field.get("type", "string")
        
        if not fieldName:
            return "1=1"
        
        # 构建完整字段名
        fullField = f"`{table}`.`{fieldName}`" if table else f"`{fieldName}`"
        
        # 根据操作符生成SQL
        if operator == "=":
            return f"{fullField} = {self._formatValue(value, fieldType)}"
        elif operator == "!=":
            return f"{fullField} != {self._formatValue(value, fieldType)}"
        elif operator == ">":
            return f"{fullField} > {self._formatValue(value, fieldType)}"
        elif operator == ">=":
            return f"{fullField} >= {self._formatValue(value, fieldType)}"
        elif operator == "<":
            return f"{fullField} < {self._formatValue(value, fieldType)}"
        elif operator == "<=":
            return f"{fullField} <= {self._formatValue(value, fieldType)}"
        elif operator == "in_range":
            if isinstance(value, list) and len(value) == 2:
                minVal, maxVal = value
                return f"{fullField} BETWEEN {minVal} AND {maxVal}"
            return "1=1"
        elif operator == "contains":
            return f"{fullField} LIKE '%{value}%'"
        elif operator == "starts_with":
            return f"{fullField} LIKE '{value}%'"
        elif operator == "ends_with":
            return f"{fullField} LIKE '%{value}'"
        elif operator == "belongs_to":
            if isinstance(value, list):
                valueList = [self._formatValue(v, fieldType) for v in value]
                return f"{fullField} IN ({','.join(valueList)})"
            return "1=1"
        elif operator == "not_belongs_to":
            if isinstance(value, list):
                valueList = [self._formatValue(v, fieldType) for v in value]
                return f"{fullField} NOT IN ({','.join(valueList)})"
            return "1=1"
        elif operator == "is_null":
            return f"{fullField} IS NULL"
        elif operator == "is_not_null":
            return f"{fullField} IS NOT NULL"
        elif operator == "is_true":
            return f"{fullField} = TRUE"
        elif operator == "is_false":
            return f"{fullField} = FALSE"
        else:
            return "1=1"
    
    def _formatValue(self, value, fieldType: str) -> str:
        """格式化值为SQL格式"""
        if value is None:
            return "NULL"
        
        if fieldType == "string":
            # 转义单引号
            escapedValue = str(value).replace("'", "''")
            return f"'{escapedValue}'"
        elif fieldType == "number":
            return str(value)
        elif fieldType == "date":
            return f"'{value}'"
        elif fieldType == "boolean":
            return "TRUE" if value else "FALSE"
        else:
            return f"'{value}'"