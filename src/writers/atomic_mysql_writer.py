import logging
from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
import json
import threading

from src.config.base import MySQLConfig

logger = logging.getLogger(__name__)


class AtomicMySQLTagWriter:
    """原子化MySQL标签写入器 - 支持并发写入时的标签合并"""
    
    def __init__(self, spark: SparkSession, mysql_config: MySQLConfig):
        self.spark = spark
        self.mysql_config = mysql_config
        self._lock = threading.Lock()
    
    def write_tags_parallel_atomic(self, tag_results: List[DataFrame]) -> bool:
        """
        并行原子写入多个标签结果
        
        每个标签独立写入，通过MySQL原子操作解决并发冲突
        """
        logger.info(f"🚀 开始并行原子写入 {len(tag_results)} 个标签结果")
        
        success_count = 0
        failed_count = 0
        
        def write_single_tag_atomic(tag_df: DataFrame):
            """原子写入单个标签的结果"""
            try:
                # 收集该标签的所有用户结果
                tag_data = tag_df.collect()
                if not tag_data:
                    return True, "空结果"
                
                tag_id = tag_data[0]['tag_id']
                logger.info(f"开始原子写入标签 {tag_id}")
                
                # 为每个用户原子更新标签
                success_users = 0
                for row in tag_data:
                    user_id = row['user_id']
                    if self._atomic_merge_user_tag(user_id, tag_id, row):
                        success_users += 1
                
                logger.info(f"✅ 标签 {tag_id} 写入完成，成功用户数: {success_users}/{len(tag_data)}")
                return True, f"成功用户数: {success_users}/{len(tag_data)}"
                
            except Exception as e:
                logger.error(f"❌ 标签原子写入失败: {str(e)}")
                return False, str(e)
        
        # 使用线程池并行写入不同标签
        max_workers = min(4, len(tag_results))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有写入任务
            future_to_tag = {
                executor.submit(write_single_tag_atomic, tag_df): i 
                for i, tag_df in enumerate(tag_results)
            }
            
            # 收集结果
            for future in as_completed(future_to_tag):
                tag_index = future_to_tag[future]
                try:
                    success, message = future.result(timeout=180)  # 3分钟超时
                    if success:
                        success_count += 1
                        logger.info(f"标签 {tag_index} 写入成功: {message}")
                    else:
                        failed_count += 1
                        logger.error(f"标签 {tag_index} 写入失败: {message}")
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"标签 {tag_index} 写入超时或异常: {str(e)}")
        
        logger.info(f"🎉 并行原子写入完成 - 成功: {success_count}, 失败: {failed_count}")
        return failed_count == 0
    
    def _atomic_merge_user_tag(self, user_id: str, tag_id: int, tag_row) -> bool:
        """
        原子合并单个用户的标签 - 使用MySQL的JSON函数实现原子操作
        """
        connection = None
        try:
            # 创建数据库连接
            connection = pymysql.connect(
                host=self.mysql_config.host,
                port=self.mysql_config.port,
                user=self.mysql_config.username,
                password=self.mysql_config.password,
                database=self.mysql_config.database,
                charset='utf8mb4'
            )
            
            with connection.cursor() as cursor:
                # 构建标签详情
                tag_detail = {
                    str(tag_id): {
                        'tag_name': getattr(tag_row, 'tag_name', ''),
                        'tag_category': getattr(tag_row, 'tag_category', ''),
                        'computed_time': getattr(tag_row, 'computed_date', '').strftime('%Y-%m-%d') if hasattr(tag_row, 'computed_date') else '',
                        'tag_detail': getattr(tag_row, 'tag_detail', '{}')
                    }
                }
                
                # 使用MySQL的原子操作进行标签合并
                merge_sql = """
                INSERT INTO user_tags (user_id, tag_ids, tag_details, computed_date) 
                VALUES (%s, JSON_ARRAY(%s), %s, CURDATE())
                ON DUPLICATE KEY UPDATE 
                    tag_ids = JSON_MERGE_PRESERVE(
                        tag_ids, 
                        JSON_ARRAY(%s)
                    ),
                    tag_details = JSON_MERGE_PATCH(
                        tag_details,
                        %s
                    )
                """
                
                tag_detail_json = json.dumps(tag_detail, ensure_ascii=False)
                
                cursor.execute(merge_sql, (
                    user_id,           # INSERT的tag_ids
                    tag_id,
                    tag_detail_json,   # INSERT的tag_details
                    tag_id,           # UPDATE的tag_ids（要合并的新标签）
                    tag_detail_json    # UPDATE的tag_details（要合并的新详情）
                ))
                
                connection.commit()
                return True
                
        except Exception as e:
            logger.error(f"用户 {user_id} 标签 {tag_id} 原子合并失败: {str(e)}")
            if connection:
                connection.rollback()
            return False
            
        finally:
            if connection:
                connection.close()
    
    def write_tags_batch_atomic(self, tag_results: List[DataFrame]) -> bool:
        """
        批量原子写入 - 先合并所有标签，再批量原子写入
        """
        logger.info(f"开始批量原子写入 {len(tag_results)} 个标签结果")
        
        try:
            # 1. 合并所有标签结果到用户维度
            user_tag_map = self._merge_all_tag_results(tag_results)
            
            # 2. 批量原子写入
            return self._batch_atomic_write(user_tag_map)
            
        except Exception as e:
            logger.error(f"批量原子写入失败: {str(e)}")
            return False
    
    def _merge_all_tag_results(self, tag_results: List[DataFrame]) -> Dict[str, Dict]:
        """合并所有标签结果到用户维度"""
        from functools import reduce
        
        # 合并所有标签结果
        all_tags = reduce(lambda df1, df2: df1.union(df2), tag_results)
        
        # 收集数据并按用户分组
        all_data = all_tags.collect()
        user_tag_map = {}
        
        for row in all_data:
            user_id = row['user_id']
            tag_id = row['tag_id']
            
            if user_id not in user_tag_map:
                user_tag_map[user_id] = {
                    'tag_ids': [],
                    'tag_details': {}
                }
            
            user_tag_map[user_id]['tag_ids'].append(tag_id)
            user_tag_map[user_id]['tag_details'][str(tag_id)] = {
                'tag_name': getattr(row, 'tag_name', ''),
                'tag_category': getattr(row, 'tag_category', ''),
                'tag_detail': getattr(row, 'tag_detail', '{}')
            }
        
        return user_tag_map
    
    def _batch_atomic_write(self, user_tag_map: Dict[str, Dict]) -> bool:
        """批量原子写入用户标签"""
        connection = None
        try:
            connection = pymysql.connect(
                host=self.mysql_config.host,
                port=self.mysql_config.port,
                user=self.mysql_config.username,
                password=self.mysql_config.password,
                database=self.mysql_config.database,
                charset='utf8mb4'
            )
            
            success_count = 0
            
            with connection.cursor() as cursor:
                for user_id, user_data in user_tag_map.items():
                    try:
                        tag_ids_json = json.dumps(user_data['tag_ids'])
                        tag_details_json = json.dumps(user_data['tag_details'], ensure_ascii=False)
                        
                        # 原子合并SQL
                        merge_sql = """
                        INSERT INTO user_tags (user_id, tag_ids, tag_details, computed_date) 
                        VALUES (%s, %s, %s, CURDATE())
                        ON DUPLICATE KEY UPDATE 
                            tag_ids = JSON_MERGE_PRESERVE(tag_ids, VALUES(tag_ids)),
                            tag_details = JSON_MERGE_PATCH(tag_details, VALUES(tag_details))
                        """
                        
                        cursor.execute(merge_sql, (user_id, tag_ids_json, tag_details_json))
                        success_count += 1
                        
                    except Exception as e:
                        logger.error(f"用户 {user_id} 批量写入失败: {str(e)}")
                        continue
                
                connection.commit()
                logger.info(f"✅ 批量原子写入完成，成功用户数: {success_count}/{len(user_tag_map)}")
                return success_count > 0
                
        except Exception as e:
            logger.error(f"批量原子写入失败: {str(e)}")
            if connection:
                connection.rollback()
            return False
            
        finally:
            if connection:
                connection.close()
    
    def get_atomic_write_statistics(self) -> dict:
        """获取原子写入后的统计信息"""
        try:
            stats_df = self.spark.read.jdbc(
                url=self.mysql_config.jdbc_url,
                table="user_tags",
                properties=self.mysql_config.connection_properties
            )
            
            total_users = stats_df.count()
            if total_users == 0:
                return {"total_users": 0, "total_tag_assignments": 0}
            
            # 统计标签分配（需要解析JSON数组）
            from pyspark.sql.functions import from_json, size
            from pyspark.sql.types import ArrayType, IntegerType
            
            parsed_df = stats_df.select(
                "user_id",
                from_json("tag_ids", ArrayType(IntegerType())).alias("tag_ids_array")
            )
            
            tag_stats = parsed_df.select("user_id", size("tag_ids_array").alias("tag_count"))
            total_assignments = tag_stats.agg({"tag_count": "sum"}).collect()[0][0]
            
            return {
                "total_users": total_users,
                "total_tag_assignments": total_assignments,
                "avg_tags_per_user": round(total_assignments / total_users, 2) if total_users > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            return {}