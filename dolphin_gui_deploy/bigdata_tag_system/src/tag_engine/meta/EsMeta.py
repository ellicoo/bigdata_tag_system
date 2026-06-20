#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Elasticsearch数据源管理类
负责现有标签数据的读取和标签结果的写入
"""
import time
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from ..utils.SparkUdfs import json_to_array


class EsMeta:
    """Elasticsearch数据源管理器
    
    职责：
    1. 加载现有用户标签数据（从ES读取）
    2. 写入标签计算结果（写入ES）
    3. 管理ES连接和配置
    
    注意：标签规则数据仍从MySQL读取，只有用户标签关系数据存储在ES
    """
    
    def __init__(self, spark: SparkSession, esConfig: Dict[str, any]):
        """初始化Elasticsearch数据源管理器
        
        Args:
            spark: Spark会话
            esConfig: ES连接配置
        """
        self.spark = spark
        self.esConfig = esConfig
        self.index = esConfig.get('index', 'user_tag_relation')
        self.hosts = esConfig.get('hosts', ['http://localhost:9200'])
        
        print(f"🔗 ES Hosts: {self.hosts}")
        print(f"📊 ES Index: {self.index}")
        
        # 测试ES连接
        # try:
        #     connection_ok = self.testConnection()
        #     if connection_ok:
        #         print("✅ ES连接测试通过")
        #     else:
        #         print("❌ ES连接测试失败，但继续初始化")
        # except Exception as e:
        #     print(f"❌ ES连接测试异常: {e}")
        #     print("⚠️  将继续初始化，但可能影响后续操作")
        
        print("🗄️  EsMeta初始化完成")
    
    def loadExistingTags(self) -> DataFrame:
        """从ES加载现有用户标签DataFrame
        
        Returns:
            DataFrame: 现有标签DataFrame，包含字段：user_id, existing_tag_ids(Array)
        """
        print("📖 从ES加载现有用户标签数据...")
        
        try:
            # 使用elasticsearch-hadoop连接器读取ES
            esDF = self.spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", ",".join([h.replace("http://", "").replace("https://", "") for h in self.hosts])) \
                .option("es.port", "80") \
                .option("es.net.http.auth.user", self.esConfig['user']) \
                .option("es.net.http.auth.pass", self.esConfig['password']) \
                .option("es.resource", self.index) \
                .option("es.read.field.include", "user_id,tag_list") \
                .option("es.nodes.wan.only", "true") \
                .option("es.scroll.size", "5000") \
                .load()
            
            # 转换tag_id_list JSON字符串为数组
            existingDF = esDF.withColumn(
                "existing_tag_ids",
                json_to_array(col("tag_list"))
            ).select("user_id", "existing_tag_ids")
            
            count = existingDF.count()
            print(f"✅ 从ES加载现有标签数据完成: {count} 个用户")
            return existingDF
            
        except Exception as e:
            print(f"❌ 从ES加载现有标签数据失败: {e}")
            import traceback
            traceback.print_exc()
            return self._createEmptyExistingTagsDataFrame()
    
    def writeTagResults(self, resultsDF: DataFrame) -> bool:
        """写入标签计算结果到ES
        
        使用Spark的elasticsearch-hadoop连接器批量写入
        
        Args:
            resultsDF: 结果DataFrame，包含字段：user_id, final_tag_ids_json
            
        Returns:
            bool: 写入是否成功
        """
        print("💾 开始写入标签结果到ES...")
        
        try:
            # 先检查是否有数据需要写入
            totalCount = resultsDF.count()
            if totalCount == 0:
                print("⚠️  没有结果需要写入")
                return True
            
            print(f"📤 准备写入 {totalCount} 条标签记录到ES...")
            
            # 准备写入ES的数据格式
            # ES需要的字段: user_id, tag_id_list, updated_time
            writeDF = resultsDF.select(
                col("user_id"),
                col("final_tag_ids_json").alias("tag_list"),
                current_timestamp().alias("updated_time")
            )
            
            write_start_time = time.time()
            
            # 基础写入配置
            writeDF.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", ",".join([h.replace("http://", "").replace("https://", "") for h in self.hosts])) \
                .option("es.port", "80") \
                .option("es.resource", self.index) \
                .option("es.mapping.id", "user_id") \
                .option("es.write.operation", "index") \
                .option("es.nodes.wan.only", "true") \
                .option("es.batch.size.entries", "5000") \
                .option("es.batch.size.bytes", "10mb") \
                .option("es.batch.write.refresh", "false") \
                .option("es.net.http.auth.user", self.esConfig['user']) \
                .option("es.net.http.auth.pass", self.esConfig['password']) \
                .mode("append").save()
            
            write_time = time.time() - write_start_time
            print(f"✅ 标签结果写入ES完成: {totalCount} 条记录，耗时 {write_time:.2f} 秒")
            return True
            
        except Exception as e:
            print(f"❌ 写入ES失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def testConnection(self) -> bool:
        """测试ES连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 尝试使用elasticsearch-python库测试连接
            from elasticsearch import Elasticsearch
            
            # 构建ES客户端
            es_client_config = {
                'hosts': self.hosts,
                'timeout': self.esConfig.get('timeout', 60),
                'max_retries': self.esConfig.get('max_retries', 3),
                'retry_on_timeout': True
            }
            
            # 如果有认证信息
            if self.esConfig.get('user') and self.esConfig.get('password'):
                es_client_config['http_auth'] = (
                    self.esConfig['user'], 
                    self.esConfig['password']
                )
            
            es_client = Elasticsearch(**es_client_config)
            
            # 测试连接
            if es_client.ping():
                print("✅ ES连接测试成功")
                
                # 检查索引是否存在
                if es_client.indices.exists(index=self.index):
                    print(f"✅ ES索引 '{self.index}' 存在")
                else:
                    print(f"⚠️  ES索引 '{self.index}' 不存在，将在写入时自动创建")
                
                return True
            else:
                print("❌ ES连接失败")
                return False
                
        except ImportError:
            print("⚠️  未安装elasticsearch库，跳过连接测试")
            print("   提示: pip install elasticsearch")
            return True  # 不因为缺少库而失败
        except Exception as e:
            print(f"❌ ES连接测试失败: {e}")
            return False
    
    def _createEmptyExistingTagsDataFrame(self) -> DataFrame:
        """创建空的现有标签DataFrame"""
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("existing_tag_ids", ArrayType(IntegerType()), True)
        ])
        
        return self.spark.createDataFrame([], schema)
