"""
标签结果数据模型 - Bean类
用于封装标签计算的结果数据，提供标准化的数据结构
"""

from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass


@dataclass
class TagResult:
    """
    标签结果数据模型
    封装单个用户的标签计算结果
    """
    userId: str                          # 用户ID
    tagId: int                          # 标签ID
    tagName: str                        # 标签名称
    tagCategory: str                    # 标签分类
    tagDetail: Optional[str] = None     # 标签详细信息
    confidence: Optional[float] = None   # 置信度分数
    computeTime: Optional[datetime] = None  # 计算时间
    ruleVersion: Optional[str] = None   # 规则版本
    
    def toDict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'user_id': self.userId,
            'tag_id': self.tagId,
            'tag_name': self.tagName,
            'tag_category': self.tagCategory,
            'tag_detail': self.tagDetail,
            'confidence': self.confidence,
            'compute_time': self.computeTime.isoformat() if self.computeTime else None,
            'rule_version': self.ruleVersion
        }
    
    @classmethod
    def fromDict(cls, data: Dict[str, Any]) -> 'TagResult':
        """从字典创建TagResult实例"""
        computeTime = None
        if data.get('compute_time'):
            computeTime = datetime.fromisoformat(data['compute_time'])
        
        return cls(
            userId=data['user_id'],
            tagId=data['tag_id'],
            tagName=data['tag_name'],
            tagCategory=data['tag_category'],
            tagDetail=data.get('tag_detail'),
            confidence=data.get('confidence'),
            computeTime=computeTime,
            ruleVersion=data.get('rule_version')
        )
    
    def isValid(self) -> bool:
        """验证数据是否有效"""
        return (
            self.userId is not None and self.userId.strip() != "" and
            self.tagId is not None and self.tagId > 0 and
            self.tagName is not None and self.tagName.strip() != ""
        )
    
    def __str__(self) -> str:
        return f"TagResult(user={self.userId}, tag={self.tagId}:{self.tagName})"
    
    def __repr__(self) -> str:
        return self.__str__()