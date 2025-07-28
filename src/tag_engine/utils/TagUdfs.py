#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签系统UDF函数集合
核心业务逻辑通过UDF实现，保持简洁高效
"""
import json
from typing import List, Dict, Any
from pyspark.sql.functions import udf
from pyspark.sql.types import *


class TagUdfs:
    """标签系统UDF函数管理器"""
    
    def __init__(self):
        """初始化并注册所有UDF函数"""
        self._registerUdfs()
    
    def _registerUdfs(self):
        """注册所有UDF函数"""
        
        # 1. 标签合并UDF - 内存合并多个标签
        @udf(returnType=ArrayType(IntegerType()))
        def mergeUserTags(tagList):
            """合并单个用户的多个标签：去重+排序
            
            Args:
                tagList: List[int] 或 Array[int] - 标签ID列表
                
            Returns:
                Array[int] - 去重排序后的标签数组
            """
            if not tagList:
                return []
            
            # 处理不同的输入类型
            if isinstance(tagList, list):
                flatTags = tagList
            else:
                # 处理嵌套数组的情况
                flatTags = []
                for item in tagList:
                    if isinstance(item, (list, tuple)):
                        flatTags.extend(item)
                    else:
                        flatTags.append(item)
            
            # 过滤None值，去重并排序
            validTags = [tag for tag in flatTags if tag is not None]
            uniqueTags = list(set(validTags))
            uniqueTags.sort()
            return uniqueTags
        
        # 2. 与现有标签合并UDF
        @udf(returnType=ArrayType(IntegerType()))
        def mergeWithExistingTags(newTags, existingTags):
            """新标签与MySQL现有标签合并"""
            if not newTags:
                newTags = []
            if not existingTags:
                existingTags = []
            
            # 合并、去重、排序
            allTags = list(set(newTags + existingTags))
            allTags.sort()
            return allTags
        
        # 3. 数组转JSON UDF
        @udf(returnType=StringType())
        def arrayToJson(arr):
            """将数组转换为JSON字符串"""
            if not arr:
                return "[]"
            return json.dumps(arr)
        
        # 4. JSON转数组UDF
        @udf(returnType=ArrayType(IntegerType()))
        def jsonToArray(jsonStr):
            """将JSON字符串转换为数组"""
            if not jsonStr:
                return []
            try:
                return json.loads(jsonStr)
            except:
                return []
        
        # 存储UDF引用
        self.mergeUserTags = mergeUserTags
        self.mergeWithExistingTags = mergeWithExistingTags
        self.arrayToJson = arrayToJson
        self.jsonToArray = jsonToArray


# 全局实例
tagUdfs = TagUdfs()