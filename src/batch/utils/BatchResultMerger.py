"""
批处理结果合并器 - 重构为驼峰命名风格
实现与MySQL现有标签的智能合并，支持标签数组合并和去重
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, array_union, array_distinct, when, isnan, isnull, size
from pyspark import StorageLevel

from src.batch.config.BaseConfig import MySQLConfig

logger = logging.getLogger(__name__)


class BatchResultMerger:
    """批处理结果合并器（原AdvancedTagMerger和UnifiedTagMerger功能）"""
    
    def __init__(self, mysqlConfig: MySQLConfig):
        self.mysqlConfig = mysqlConfig
        self.logger = logging.getLogger(__name__)
    
    def mergeWithExistingTags(self, newTagsDataFrame: DataFrame, 
                             existingTagsDataFrame: Optional[DataFrame] = None) -> DataFrame:
        """
        将新计算的标签与MySQL现有标签进行智能合并
        
        Args:
            newTagsDataFrame: 新计算的标签数据 (user_id, tag_ids)
            existingTagsDataFrame: 现有标签数据，None时自动加载
            
        Returns:
            DataFrame: 合并后的标签数据
        """
        try:
            self.logger.info("🔄 开始标签合并...")
            
            # 验证新标签数据格式
            if not self._validateDataFrameFormat(newTagsDataFrame, ['user_id', 'tag_ids']):
                raise ValueError("新标签数据格式不正确")
            
            # 加载现有标签数据
            if existingTagsDataFrame is None:
                existingTagsDataFrame = self._loadExistingTags()
            
            # 如果没有现有标签，直接返回新标签
            if existingTagsDataFrame is None or existingTagsDataFrame.count() == 0:
                self.logger.info("ℹ️ 没有现有标签，直接使用新标签")
                return self._formatFinalResult(newTagsDataFrame)
            
            # 执行智能合并
            mergedDataFrame = self._performIntelligentMerge(newTagsDataFrame, existingTagsDataFrame)
            
            # 记录合并统计
            self._logMergeStatistics(newTagsDataFrame, existingTagsDataFrame, mergedDataFrame)
            
            return mergedDataFrame
            
        except Exception as e:
            self.logger.error(f"❌ 标签合并失败: {str(e)}")
            raise
    
    def _loadExistingTags(self) -> Optional[DataFrame]:
        """
        加载现有标签数据
        
        Returns:
            DataFrame: 现有标签数据，失败时返回None
        """
        try:
            from src.batch.utils.RuleReader import RuleReader
            
            spark = SparkSession.getActiveSession()
            ruleReader = RuleReader(spark, self.mysqlConfig)
            
            existingDataFrame = ruleReader.loadExistingUserTags()
            
            if existingDataFrame is not None:
                # 只保留需要的列并重命名
                cleanedDataFrame = existingDataFrame.select(
                    col("user_id"),
                    col("tag_ids_array").alias("existing_tag_ids")
                )
                
                recordCount = cleanedDataFrame.count()
                self.logger.info(f"✅ 加载现有标签: {recordCount} 个用户")
                return cleanedDataFrame
            else:
                self.logger.info("ℹ️ 没有现有标签数据")
                return None
                
        except Exception as e:
            self.logger.warning(f"⚠️ 加载现有标签失败: {str(e)}")
            return None
    
    def _performIntelligentMerge(self, newTagsDataFrame: DataFrame, 
                                existingTagsDataFrame: DataFrame) -> DataFrame:
        """
        执行智能标签合并
        
        Args:
            newTagsDataFrame: 新标签数据
            existingTagsDataFrame: 现有标签数据
            
        Returns:
            DataFrame: 合并后的数据
        """
        try:
            self.logger.info("🧩 执行智能标签合并...")
            
            # Left join: 保留所有新标签用户，合并现有标签
            joinedDataFrame = newTagsDataFrame.join(
                existingTagsDataFrame,
                on="user_id",
                how="left"
            )
            
            # 智能合并逻辑
            mergedDataFrame = joinedDataFrame.select(
                col("user_id"),
                when(col("existing_tag_ids").isNull(), col("tag_ids"))
                .otherwise(array_distinct(array_union(col("tag_ids"), col("existing_tag_ids"))))
                .alias("tag_ids")
            )
            
            # 过滤掉空标签的用户
            finalDataFrame = mergedDataFrame.filter(
                col("tag_ids").isNotNull() & (size(col("tag_ids")) > 0)
            )
            
            self.logger.info("✅ 智能标签合并完成")
            return finalDataFrame
            
        except Exception as e:
            self.logger.error(f"❌ 智能标签合并失败: {str(e)}")
            raise
    
    def _formatFinalResult(self, dataFrame: DataFrame) -> DataFrame:
        """
        格式化最终结果
        
        Args:
            dataFrame: 输入数据
            
        Returns:
            DataFrame: 格式化后的数据
        """
        try:
            # 确保标签数组去重并过滤空值
            formattedDataFrame = dataFrame.select(
                col("user_id"),
                array_distinct(col("tag_ids")).alias("tag_ids")
            ).filter(
                col("tag_ids").isNotNull() & (size(col("tag_ids")) > 0)
            )
            
            return formattedDataFrame
            
        except Exception as e:
            self.logger.error(f"❌ 格式化结果失败: {str(e)}")
            raise
    
    def _validateDataFrameFormat(self, dataFrame: DataFrame, requiredColumns: List[str]) -> bool:
        """
        验证DataFrame格式
        
        Args:
            dataFrame: 要验证的DataFrame
            requiredColumns: 必需的列名列表
            
        Returns:
            bool: 格式是否正确
        """
        try:
            if dataFrame is None:
                return False
            
            missingColumns = set(requiredColumns) - set(dataFrame.columns)
            if missingColumns:
                self.logger.error(f"❌ 缺少必需列: {missingColumns}")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ DataFrame格式验证失败: {str(e)}")
            return False
    
    def _logMergeStatistics(self, newTagsDataFrame: DataFrame, 
                           existingTagsDataFrame: DataFrame, 
                           mergedDataFrame: DataFrame):
        """
        记录合并统计信息
        
        Args:
            newTagsDataFrame: 新标签数据
            existingTagsDataFrame: 现有标签数据  
            mergedDataFrame: 合并后数据
        """
        try:
            newUsers = newTagsDataFrame.count()
            existingUsers = existingTagsDataFrame.count()
            finalUsers = mergedDataFrame.count()
            
            # 计算合并用户数（同时有新标签和旧标签的用户）
            mergeJoinDataFrame = newTagsDataFrame.join(
                existingTagsDataFrame.select("user_id").distinct(),
                on="user_id",
                how="inner"
            )
            mergedUsers = mergeJoinDataFrame.count()
            
            statistics = {
                'new_tagged_users': newUsers,
                'existing_tagged_users': existingUsers,
                'merged_users': mergedUsers,
                'final_tagged_users': finalUsers,
                'new_only_users': newUsers - mergedUsers
            }
            
            self.logger.info(f"📊 合并统计: {statistics}")
            
            # 详细日志
            self.logger.info(f"   👥 新增标签用户: {newUsers}")
            self.logger.info(f"   📚 现有标签用户: {existingUsers}")
            self.logger.info(f"   🔄 合并用户数: {mergedUsers}")
            self.logger.info(f"   ✨ 仅新标签用户: {statistics['new_only_users']}")
            self.logger.info(f"   🎯 最终用户总数: {finalUsers}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 记录合并统计失败: {str(e)}")
    
    def mergeMultipleTagResults(self, tagResultsList: List[DataFrame]) -> DataFrame:
        """
        合并多个标签计算结果
        
        Args:
            tagResultsList: 标签结果DataFrame列表
            
        Returns:
            DataFrame: 合并后的结果
        """
        try:
            if not tagResultsList:
                raise ValueError("标签结果列表为空")
            
            self.logger.info(f"🔗 合并 {len(tagResultsList)} 个标签结果...")
            
            # 验证所有DataFrame格式
            for i, df in enumerate(tagResultsList):
                if not self._validateDataFrameFormat(df, ['user_id', 'tag_id']):
                    raise ValueError(f"第 {i+1} 个标签结果格式不正确")
            
            # 合并所有结果
            allResults = None
            for tagResult in tagResultsList:
                if allResults is None:
                    allResults = tagResult
                else:
                    allResults = allResults.union(tagResult)
            
            # 按用户聚合标签
            from pyspark.sql.functions import collect_list, array_distinct
            
            aggregatedResult = allResults.groupBy("user_id").agg(
                array_distinct(collect_list("tag_id")).alias("tag_ids")
            )
            
            finalCount = aggregatedResult.count()
            self.logger.info(f"✅ 多标签结果合并完成: {finalCount} 个用户")
            
            return aggregatedResult
            
        except Exception as e:
            self.logger.error(f"❌ 多标签结果合并失败: {str(e)}")
            raise
    
    def getDetailedMergeTrace(self, newTagsDataFrame: DataFrame, 
                             existingTagsDataFrame: Optional[DataFrame] = None,
                             sampleCount: int = 5) -> Dict[str, Any]:
        """
        获取详细的合并追踪信息
        
        Args:
            newTagsDataFrame: 新标签数据
            existingTagsDataFrame: 现有标签数据
            sampleCount: 抽样用户数量
            
        Returns:
            Dict[str, Any]: 详细追踪信息
        """
        try:
            self.logger.info(f"🔍 生成详细合并追踪（抽样 {sampleCount} 个用户）...")
            
            # 加载现有标签数据
            if existingTagsDataFrame is None:
                existingTagsDataFrame = self._loadExistingTags()
            
            # 抽样选择有标签的用户进行追踪
            sampleUsers = newTagsDataFrame.select("user_id").limit(sampleCount).rdd.map(
                lambda row: row['user_id']
            ).collect()
            
            traceInfo = {
                'sample_users': sampleUsers,
                'merge_details': []
            }
            
            for userId in sampleUsers:
                # 获取用户的新标签
                newUserTags = newTagsDataFrame.filter(col("user_id") == userId).select("tag_ids").first()
                newTags = newUserTags['tag_ids'] if newUserTags else []
                
                # 获取用户的现有标签
                existingTags = []
                if existingTagsDataFrame is not None:
                    existingUserTags = existingTagsDataFrame.filter(col("user_id") == userId).select("existing_tag_ids").first()
                    existingTags = existingUserTags['existing_tag_ids'] if existingUserTags else []
                
                # 计算合并后标签
                mergedTags = list(set(newTags + existingTags)) if existingTags else newTags
                
                userTrace = {
                    'user_id': userId,
                    'new_tags': newTags,
                    'existing_tags': existingTags,
                    'merged_tags': mergedTags
                }
                
                traceInfo['merge_details'].append(userTrace)
                
                self.logger.info(f"   👤 用户 {userId}:")
                self.logger.info(f"      🆕 新标签: {newTags}")
                self.logger.info(f"      📚 现有标签: {existingTags}")
                self.logger.info(f"      🔄 合并后: {mergedTags}")
            
            return traceInfo
            
        except Exception as e:
            self.logger.error(f"❌ 生成合并追踪失败: {str(e)}")
            return {}
    
    def validateMergeLogic(self, newTagsDataFrame: DataFrame) -> bool:
        """
        验证合并逻辑是否正确
        
        Args:
            newTagsDataFrame: 新标签数据
            
        Returns:
            bool: 合并逻辑是否正确
        """
        try:
            self.logger.info("🔍 验证合并逻辑...")
            
            # 执行合并
            mergedResult = self.mergeWithExistingTags(newTagsDataFrame)
            
            # 基本验证：合并后用户数不应该少于新标签用户数
            newUserCount = newTagsDataFrame.count()
            mergedUserCount = mergedResult.count()
            
            if mergedUserCount < newUserCount:
                self.logger.error(f"❌ 合并后用户数减少: {newUserCount} -> {mergedUserCount}")
                return False
            
            # 验证标签数组格式
            invalidTags = mergedResult.filter(
                col("tag_ids").isNull() | (size(col("tag_ids")) == 0)
            ).count()
            
            if invalidTags > 0:
                self.logger.error(f"❌ 发现 {invalidTags} 个用户的标签数组无效")
                return False
            
            self.logger.info("✅ 合并逻辑验证通过")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 合并逻辑验证失败: {str(e)}")
            return False