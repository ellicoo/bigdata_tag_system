"""
VIP客户标签任务 - 使用新的BaseTagTask抽象
展示如何子类指定S3 Hive表地址和字段
"""

from typing import Dict, List
from pyspark.sql import DataFrame
import logging

from src.batch.tasks.base.BaseTagTask import BaseTagTask

logger = logging.getLogger(__name__)


class VIPUserTask(BaseTagTask):
    """
    VIP客户标签任务
    
    业务规则：
    - 用户等级为 VIP2 或 VIP3
    - KYC状态已验证
    
    标签ID：5
    标签类别：生命周期类
    """
    
    def getRequiredFields(self) -> List[str]:
        """
        返回该标签需要的数据字段
        
        Returns:
            List[str]: VIP客户任务需要的字段
        """
        return [
            "user_id",      # 用户ID
            "user_level",   # 用户等级
            "kyc_status"    # KYC状态
        ]
    
    def getHiveTableConfig(self) -> Dict[str, str]:
        """
        返回该标签需要的Hive表配置
        子类指定具体的S3 Hive表地址
        
        Returns:
            Dict[str, str]: Hive表配置，指定需要的表和路径
        """
        return {
            'user_basic_info': 'user_basic_info'  # 用户基础信息表
        }
    
    def preprocessData(self, rawData: DataFrame) -> DataFrame:
        """
        VIP客户任务的数据预处理
        
        Args:
            rawData: 原始用户基础信息数据
            
        Returns:
            DataFrame: 预处理后的数据
        """
        # 过滤掉空值和无效数据
        cleanedData = rawData.filter(
            (rawData.user_level.isNotNull()) & 
            (rawData.kyc_status.isNotNull()) &
            (rawData.user_level != "") &
            (rawData.kyc_status != "")
        )
        
        logger.info(f"VIP客户任务数据预处理: 原始{rawData.count()}条 -> 清洗后{cleanedData.count()}条")
        return cleanedData
    
    def postProcessResult(self, taggedUsers: DataFrame) -> DataFrame:
        """
        VIP客户任务的结果后处理
        
        Args:
            taggedUsers: 标签计算结果
            
        Returns:
            DataFrame: 后处理的结果
        """
        # 可以在这里添加特定的业务逻辑，比如按用户等级排序
        return taggedUsers.orderBy("user_id")
    
    def validateData(self, data: DataFrame) -> bool:
        """
        验证VIP客户任务的数据
        
        Args:
            data: 输入数据
            
        Returns:
            bool: 数据是否有效
        """
        # 调用父类验证
        if not super().validateData(data):
            return False
        
        # VIP客户任务特定的验证
        if data.count() == 0:
            logger.warning("VIP客户任务: 没有用户基础信息数据")
            return False
        
        # 检查数据质量
        nullLevelCount = data.filter(data.user_level.isNull()).count()
        if nullLevelCount > 0:
            logger.warning(f"VIP客户任务: 发现{nullLevelCount}条用户等级为空的记录")
        
        nullKycCount = data.filter(data.kyc_status.isNull()).count()
        if nullKycCount > 0:
            logger.warning(f"VIP客户任务: 发现{nullKycCount}条KYC状态为空的记录")
        
        return True