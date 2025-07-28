#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算引擎包
简化的PySpark实现，基于Spark DSL + UDF架构
"""

from .engine import TagEngine, TagGroup
from .meta import HiveMeta, MysqlMeta
from .parser import TagRuleParser
from .utils import tagUdfs

__all__ = [
    "TagEngine",
    "TagGroup",
    "HiveMeta", 
    "MysqlMeta",
    "TagRuleParser",
    "tagUdfs"
]

__version__ = "1.0.0"
__author__ = "Tag System Team"
__description__ = "简化PySpark标签计算系统 - 纯Spark DSL + UDF实现"