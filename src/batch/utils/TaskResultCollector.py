"""
任务结果收集器 - 驼峰命名风格
收集和管理多个标签任务的执行结果，支持结果验证和统计
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min

logger = logging.getLogger(__name__)


class TaskResultCollector:
    """任务结果收集器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._results = []
        self._statistics = {}
    
    def addTaskResult(self, tagId: int, tagName: str, result: DataFrame) -> bool:
        """
        添加任务结果
        
        Args:
            tagId: 标签ID
            tagName: 标签名称
            result: 任务执行结果DataFrame
            
        Returns:
            bool: 添加是否成功
        """
        try:
            if result is None:
                self.logger.warning(f"⚠️ 标签 {tagId}({tagName}) 结果为None，跳过")
                return False
            
            # 验证结果格式
            if not self._validateResultFormat(result):
                self.logger.error(f"❌ 标签 {tagId}({tagName}) 结果格式不正确")
                return False
            
            userCount = result.count()
            
            resultInfo = {
                'tag_id': tagId,
                'tag_name': tagName,
                'result_dataframe': result,
                'user_count': userCount,
                'timestamp': self._getCurrentTimestamp()
            }
            
            self._results.append(resultInfo)
            self.logger.info(f"✅ 添加标签 {tagId}({tagName}) 结果: {userCount} 个用户")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 添加标签 {tagId}({tagName}) 结果失败: {str(e)}")
            return False
    
    def collectAllResults(self) -> List[DataFrame]:
        """
        收集所有结果DataFrame
        
        Returns:
            List[DataFrame]: 所有任务的结果DataFrame列表
        """
        try:
            self.logger.info(f"📦 收集 {len(self._results)} 个任务结果...")
            
            results = []
            for resultInfo in self._results:
                if resultInfo['result_dataframe'] is not None:
                    results.append(resultInfo['result_dataframe'])
            
            self.logger.info(f"✅ 收集完成: {len(results)} 个有效结果")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 收集结果失败: {str(e)}")
            return []
    
    def getDetailedStatistics(self) -> Dict[str, Any]:
        """
        获取详细统计信息
        
        Returns:
            Dict[str, Any]: 详细统计信息
        """
        try:
            if not self._results:
                return {
                    'total_tasks': 0,
                    'total_users': 0,
                    'task_details': []
                }
            
            totalTasks = len(self._results)
            totalUsers = sum(result['user_count'] for result in self._results)
            
            # 计算用户数统计
            userCounts = [result['user_count'] for result in self._results]
            avgUsers = round(sum(userCounts) / len(userCounts), 2) if userCounts else 0
            maxUsers = max(userCounts) if userCounts else 0
            minUsers = min(userCounts) if userCounts else 0
            
            # 任务详细信息
            taskDetails = []
            for result in self._results:
                taskDetails.append({
                    'tag_id': result['tag_id'],
                    'tag_name': result['tag_name'],
                    'user_count': result['user_count'],
                    'timestamp': result['timestamp']
                })
            
            statistics = {
                'total_tasks': totalTasks,
                'total_users': totalUsers,
                'avg_users_per_task': avgUsers,
                'max_users_per_task': maxUsers,
                'min_users_per_task': minUsers,
                'task_details': taskDetails
            }
            
            self.logger.info(f"📊 统计信息: {totalTasks}个任务, {totalUsers}个用户")
            return statistics
            
        except Exception as e:
            self.logger.error(f"❌ 获取统计信息失败: {str(e)}")
            return {}
    
    def validateAllResults(self) -> Tuple[bool, List[str]]:
        """
        验证所有结果的完整性
        
        Returns:
            Tuple[bool, List[str]]: (验证是否通过, 错误信息列表)
        """
        try:
            self.logger.info("🔍 验证所有任务结果...")
            
            errors = []
            
            if not self._results:
                errors.append("没有任务结果需要验证")
                return False, errors
            
            # 验证每个结果
            for result in self._results:
                tagId = result['tag_id']
                df = result['result_dataframe']
                
                # 检查DataFrame是否为空
                if df is None:
                    errors.append(f"标签 {tagId} 的DataFrame为None")
                    continue
                
                # 检查必需列
                if not self._validateResultFormat(df):
                    errors.append(f"标签 {tagId} 的DataFrame格式不正确")
                    continue
                
                # 检查数据完整性
                userCount = result['user_count']
                actualCount = df.count()
                if userCount != actualCount:
                    errors.append(f"标签 {tagId} 用户数不一致: 记录{userCount} vs 实际{actualCount}")
            
            isValid = len(errors) == 0
            
            if isValid:
                self.logger.info("✅ 所有任务结果验证通过")
            else:
                self.logger.warning(f"⚠️ 发现 {len(errors)} 个验证错误")
                for error in errors:
                    self.logger.warning(f"   - {error}")
            
            return isValid, errors
            
        except Exception as e:
            self.logger.error(f"❌ 验证任务结果失败: {str(e)}")
            return False, [f"验证过程异常: {str(e)}"]
    
    def getResultsByTagIds(self, tagIds: List[int]) -> List[DataFrame]:
        """
        根据标签ID列表获取结果
        
        Args:
            tagIds: 标签ID列表
            
        Returns:
            List[DataFrame]: 对应的结果DataFrame列表
        """
        try:
            results = []
            for result in self._results:
                if result['tag_id'] in tagIds:
                    results.append(result['result_dataframe'])
            
            self.logger.info(f"🎯 根据标签ID {tagIds} 获取到 {len(results)} 个结果")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 根据标签ID获取结果失败: {str(e)}")
            return []
    
    def getTopPerformingTasks(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        获取用户数最多的任务
        
        Args:
            limit: 返回的任务数量限制
            
        Returns:
            List[Dict[str, Any]]: 按用户数排序的任务信息
        """
        try:
            sortedResults = sorted(self._results, 
                                 key=lambda x: x['user_count'], 
                                 reverse=True)
            
            topTasks = []
            for result in sortedResults[:limit]:
                topTasks.append({
                    'tag_id': result['tag_id'],
                    'tag_name': result['tag_name'],
                    'user_count': result['user_count']
                })
            
            self.logger.info(f"🏆 获取前 {len(topTasks)} 个高效任务")
            return topTasks
            
        except Exception as e:
            self.logger.error(f"❌ 获取高效任务失败: {str(e)}")
            return []
    
    def _validateResultFormat(self, df: DataFrame) -> bool:
        """
        验证结果DataFrame格式
        
        Args:
            df: 要验证的DataFrame
            
        Returns:
            bool: 格式是否正确
        """
        try:
            if df is None:
                return False
            
            # 检查必需的列
            requiredColumns = ['user_id', 'tag_id']
            actualColumns = df.columns
            
            for col in requiredColumns:
                if col not in actualColumns:
                    self.logger.error(f"❌ 缺少必需列: {col}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 验证DataFrame格式失败: {str(e)}")
            return False
    
    def _getCurrentTimestamp(self) -> str:
        """
        获取当前时间戳
        
        Returns:
            str: 时间戳字符串
        """
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def clearResults(self):
        """清理所有结果"""
        try:
            self.logger.info(f"🧹 清理 {len(self._results)} 个任务结果...")
            
            # 清理DataFrame缓存
            for result in self._results:
                if result['result_dataframe'] is not None:
                    try:
                        result['result_dataframe'].unpersist()
                    except Exception:
                        pass  # 忽略unpersist错误
            
            self._results.clear()
            self._statistics.clear()
            
            self.logger.info("✅ 任务结果清理完成")
            
        except Exception as e:
            self.logger.warning(f"⚠️ 清理任务结果异常: {str(e)}")
    
    def isEmpty(self) -> bool:
        """
        检查是否有结果
        
        Returns:
            bool: 是否为空
        """
        return len(self._results) == 0
    
    def getResultCount(self) -> int:
        """
        获取结果数量
        
        Returns:
            int: 结果数量
        """
        return len(self._results)
    
    def __len__(self):
        return len(self._results)
    
    def __str__(self) -> str:
        return f"TaskResultCollector(tasks={len(self._results)})"
    
    def __repr__(self) -> str:
        return self.__str__()