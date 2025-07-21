"""
Hive表元数据 - Bean类
封装Hive表的元数据信息，包括表结构、路径、统计等
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class HiveTableMeta:
    """
    Hive表元数据模型
    封装Hive表的结构和统计信息
    """
    tableName: str                           # 表名
    tablePath: str                          # S3路径
    columns: List[str] = field(default_factory=list)  # 列名列表
    partitions: List[str] = field(default_factory=list)  # 分区列表
    recordCount: Optional[int] = None        # 记录数
    sizeBytes: Optional[int] = None         # 文件大小(字节)
    lastModified: Optional[datetime] = None  # 最后修改时间
    fileFormat: str = "parquet"             # 文件格式
    compressionType: Optional[str] = None    # 压缩类型
    
    # 表统计信息
    statistics: Dict[str, Any] = field(default_factory=dict)  # 统计信息
    sampleData: List[Dict[str, Any]] = field(default_factory=list)  # 样本数据
    
    def addColumn(self, columnName: str):
        """添加列"""
        if columnName not in self.columns:
            self.columns.append(columnName)
    
    def hasColumn(self, columnName: str) -> bool:
        """检查是否包含指定列"""
        return columnName in self.columns
    
    def getRequiredColumns(self, requiredFields: List[str]) -> List[str]:
        """获取表中存在的必需字段"""
        return [field for field in requiredFields if field in self.columns]
    
    def getMissingColumns(self, requiredFields: List[str]) -> List[str]:
        """获取表中缺失的必需字段"""
        return [field for field in requiredFields if field not in self.columns]
    
    def updateStatistics(self, stats: Dict[str, Any]):
        """更新统计信息"""
        self.statistics.update(stats)
    
    def addSampleRow(self, row: Dict[str, Any]):
        """添加样本数据行"""
        if len(self.sampleData) < 10:  # 最多保存10行样本
            self.sampleData.append(row)
    
    def getFormattedSize(self) -> str:
        """获取格式化的文件大小"""
        if self.sizeBytes is None:
            return "未知"
        
        if self.sizeBytes < 1024:
            return f"{self.sizeBytes} B"
        elif self.sizeBytes < 1024 * 1024:
            return f"{self.sizeBytes / 1024:.1f} KB"
        elif self.sizeBytes < 1024 * 1024 * 1024:
            return f"{self.sizeBytes / (1024 * 1024):.1f} MB"
        else:
            return f"{self.sizeBytes / (1024 * 1024 * 1024):.1f} GB"
    
    def isHealthy(self) -> bool:
        """检查表是否健康"""
        return (
            self.tableName is not None and
            self.tablePath is not None and
            len(self.columns) > 0 and
            self.recordCount is not None and self.recordCount > 0
        )
    
    def toDict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'table_name': self.tableName,
            'table_path': self.tablePath,
            'columns': self.columns,
            'partitions': self.partitions,
            'record_count': self.recordCount,
            'size_bytes': self.sizeBytes,
            'formatted_size': self.getFormattedSize(),
            'last_modified': self.lastModified.isoformat() if self.lastModified else None,
            'file_format': self.fileFormat,
            'compression_type': self.compressionType,
            'statistics': self.statistics,
            'sample_data': self.sampleData,
            'is_healthy': self.isHealthy()
        }
    
    @classmethod
    def fromBasicInfo(cls, tableName: str, tablePath: str, columns: List[str]) -> 'HiveTableMeta':
        """从基础信息创建HiveTableMeta实例"""
        return cls(
            tableName=tableName,
            tablePath=tablePath,
            columns=columns.copy()
        )
    
    @classmethod
    def fromDict(cls, data: Dict[str, Any]) -> 'HiveTableMeta':
        """从字典创建HiveTableMeta实例"""
        lastModified = None
        if data.get('last_modified'):
            lastModified = datetime.fromisoformat(data['last_modified'])
        
        return cls(
            tableName=data['table_name'],
            tablePath=data['table_path'],
            columns=data.get('columns', []),
            partitions=data.get('partitions', []),
            recordCount=data.get('record_count'),
            sizeBytes=data.get('size_bytes'),
            lastModified=lastModified,
            fileFormat=data.get('file_format', 'parquet'),
            compressionType=data.get('compression_type'),
            statistics=data.get('statistics', {}),
            sampleData=data.get('sample_data', [])
        )
    
    def __str__(self) -> str:
        return f"HiveTableMeta(table={self.tableName}, columns={len(self.columns)}, records={self.recordCount})"
    
    def __repr__(self) -> str:
        return self.__str__()