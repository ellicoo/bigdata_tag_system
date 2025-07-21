"""
用户标签汇总数据模型 - Bean类
封装单个用户的所有标签信息
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

from src.batch.bean.TagResult import TagResult


@dataclass
class UserTagsSummary:
    """
    用户标签汇总数据模型
    封装单个用户的所有标签信息
    """
    userId: str                         # 用户ID
    tagIds: List[int]                   # 标签ID列表
    tagDetails: Dict[int, TagResult]    # 详细标签信息
    totalTags: int                      # 标签总数
    lastUpdated: Optional[datetime] = None  # 最后更新时间
    
    def addTag(self, tagResult: TagResult):
        """添加标签"""
        if tagResult.userId != self.userId:
            raise ValueError(f"用户ID不匹配: {tagResult.userId} != {self.userId}")
        
        if tagResult.tagId not in self.tagIds:
            self.tagIds.append(tagResult.tagId)
        
        self.tagDetails[tagResult.tagId] = tagResult
        self.totalTags = len(self.tagIds)
        self.lastUpdated = datetime.now()
    
    def removeTag(self, tagId: int):
        """移除标签"""
        if tagId in self.tagIds:
            self.tagIds.remove(tagId)
            self.tagDetails.pop(tagId, None)
            self.totalTags = len(self.tagIds)
            self.lastUpdated = datetime.now()
    
    def hasTag(self, tagId: int) -> bool:
        """检查是否包含指定标签"""
        return tagId in self.tagIds
    
    def getTagsByCategory(self, category: str) -> List[TagResult]:
        """按类别获取标签"""
        return [
            tagResult for tagResult in self.tagDetails.values()
            if tagResult.tagCategory == category
        ]
    
    def toDict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'user_id': self.userId,
            'tag_ids': self.tagIds,
            'tag_details': {
                str(tagId): tagResult.toDict() 
                for tagId, tagResult in self.tagDetails.items()
            },
            'total_tags': self.totalTags,
            'last_updated': self.lastUpdated.isoformat() if self.lastUpdated else None
        }
    
    @classmethod
    def fromDict(cls, data: Dict[str, Any]) -> 'UserTagsSummary':
        """从字典创建UserTagsSummary实例"""
        tagDetails = {}
        for tagIdStr, tagData in data.get('tag_details', {}).items():
            tagId = int(tagIdStr)
            tagDetails[tagId] = TagResult.fromDict(tagData)
        
        lastUpdated = None
        if data.get('last_updated'):
            lastUpdated = datetime.fromisoformat(data['last_updated'])
        
        return cls(
            userId=data['user_id'],
            tagIds=data.get('tag_ids', []),
            tagDetails=tagDetails,
            totalTags=data.get('total_tags', 0),
            lastUpdated=lastUpdated
        )
    
    def __str__(self) -> str:
        return f"UserTagsSummary(user={self.userId}, tags={self.totalTags})"
    
    def __repr__(self) -> str:
        return self.__str__()