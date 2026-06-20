#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark UDF函数集合 - 模块级函数避免序列化问题

 1. Driver端执行 (DolphinScheduler节点)
# 这些都在Driver端执行，不会有版本冲突
class TagEngine:           # Python类实例化
class TagGroup:           # Python类实例化
class MaxComputeMeta:      # Python类实例化
class MysqlMeta:           # Python类实例化
class TagRuleParser:       # Python类实例化
# 编排逻辑都在Driver端
tagEngine = TagEngine(spark, hiveConfig, mysqlConfig)
tagEngine.computeTags()

2. Executor端执行 (YARN集群各节点)

# ❌ 这些会被序列化到Executor端，触发版本检查
@udf(returnType=ArrayType(IntegerType()))
def mergeUserTags(tagList):     # Python UDF函数
  # 这个函数会在Worker节点执行
  一旦设计成类，就会触发版本冲突

# ✅ 这些是Spark原生表达式，不涉及Python序列化
df.withColumn("tags", array_distinct(array_sort(col("tags"))))


所以：使用模块级函数而非类实例，避免Python对象序列化导致的版本不匹配
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *



def merge_with_existing_tags(new_tags_col, existing_tags_col):
    """新标签与MySQL现有标签合并 - 传统合并模式（只增不减）
    
    使用Spark原生函数：array_union + array_distinct + array_sort
    
    Args:
        new_tags_col: Column - 新标签数组列
        existing_tags_col: Column - 现有标签数组列
        
    Returns:
        Column - 合并去重排序后的标签数组
    """
    # 处理空值情况
    new_tags = coalesce(new_tags_col, array())
    existing_tags = coalesce(existing_tags_col, array())
    
    # 使用Spark原生函数合并数组
    return array_distinct(
        array_sort(
            array_union(new_tags, existing_tags)
        )
    )


def replace_computed_tags(computed_tags_col, existing_tags_col, computed_tag_scope_col):
    """基于本次计算范围智能替换用户标签 - 支持标签移除
    
    核心逻辑：
    1. 保留现有标签中不在本次计算范围内的标签（不受影响的标签）
    2. 用本次计算结果替换范围内的标签（支持新增、移除、保持）
    
    业务场景：
    - 现有标签: [1,2,3,4,5]
    - 本次计算范围: [2,3,6] 
    - 本次计算结果: [3,6] (用户匹配了3,6标签，不匹配2标签)
    - 最终结果: [1,4,5] + [3,6] = [1,3,4,5,6]
    
    Args:
        computed_tags_col: Column - 本次计算得到的标签数组
        existing_tags_col: Column - 用户现有的标签数组
        computed_tag_scope_col: Column - 本次计算涉及的所有标签范围数组
        
    Returns:
        Column - 智能替换后的最终标签数组
    """
    from pyspark.sql.functions import coalesce, array_except, array_union, array_distinct, array_sort, when, size
    from pyspark.sql.types import ArrayType, IntegerType
    
    # 关键修复：使用 when 条件处理空值，避免 array() 的问题
    # 从现有标签中排除本次计算范围内的标签
    unaffected_tags = array_except(
        coalesce(existing_tags_col, array_sort(array())),
        coalesce(computed_tag_scope_col, array_sort(array()))
    )
    
    # 合并不受影响的标签 + 本次计算结果
    return array_distinct(
        array_sort(
            array_union(
                unaffected_tags,
                coalesce(computed_tags_col, array_sort(array()))
            )
        )
    )



def array_to_json(array_col):
    """将数组转换为JSON字符串
    
    Args:
        array_col: Column - 数组列
        
    Returns:
        Column - JSON字符串列
    """
    return to_json(coalesce(array_col, array()))


def json_to_array(json_col):
    """将JSON字符串转换为数组
    
    Args:
        json_col: Column - JSON字符串列
        
    Returns:
        Column - 数组列
    """
    array_schema = ArrayType(IntegerType())
    return coalesce(from_json(json_col, array_schema), array())


@udf(returnType=ArrayType(IntegerType()))
def _apply_layering_to_tag_array_udf(user_id, tag_ids, tag_group_map_str):
    """对整个标签数组应用分层逻辑
    
    Args:
        user_id: str - 用户ID
        tag_ids: List[int] - 用户的标签ID列表
        tag_group_map_str: str - 标签ID到group_attr的映射，字符串格式
        
    Returns:
        List[int] - 分层后的标签列表
    """
    import json
    import hashlib
    
    if not tag_ids:
        return []
    
    try:
        # 解析标签映射
        # 注意：JSON 会将整数 key 转换为字符串，在判断时需要处理
        tag_group_map = json.loads(tag_group_map_str)

        result_tags = []
        
        for tag_id in tag_ids:
            # 关键修复：将 tag_id 转换为字符串来匹配 JSON key
            # 因为 json.loads() 会将所有数字 key 转换为字符串
            tag_id_str = str(tag_id)
            if tag_id_str in tag_group_map:
                group_attr_json = tag_group_map[tag_id_str]
                
                # 解析分层配置
                group_attr = json.loads(group_attr_json)
                layer_type = group_attr.get('type')
                number_str = group_attr.get('number', '')
                child_tag_ids_raw = group_attr.get('tagIds', [])
                
                # 关键修复：确保 child_tag_ids 是整数列表
                if not isinstance(child_tag_ids_raw, list):
                    result_tags.append(tag_id)
                    continue
                
                # 强制转换为整数列表
                try:
                    child_tag_ids = [int(tid) for tid in child_tag_ids_raw if tid is not None]
                except (ValueError, TypeError) as e:
                    # 转换失败，保留原标签
                    print(f"标签 {tag_id} 的 tagIds 转换失败: {e}, 原始值: {child_tag_ids_raw}")
                    result_tags.append(tag_id)
                    continue
                
                if not layer_type or not child_tag_ids:
                    # 配置无效，保留原标签
                    result_tags.append(tag_id)
                    continue
                
                # 使用user_id的hash值确保幂等性
                user_hash = int(hashlib.md5(user_id.encode('utf-8')).hexdigest(), 16)
                
                # 根据分层类型确定子标签索引
                if layer_type == 'layers':
                    num_layers = int(number_str)
                    if num_layers != len(child_tag_ids):
                        result_tags.append(tag_id)
                        continue
                    layer_index = user_hash % num_layers
                    
                elif layer_type == 'ratio':
                    ratios = [int(r.strip()) for r in number_str.split(',')]
                    if len(ratios) != len(child_tag_ids):
                        result_tags.append(tag_id)
                        continue
                    
                    # 关键修复：使用 Python 内置的 sum()，而不是 Spark 的 sum()
                    # 因为模块顶部有 from pyspark.sql.functions import *
                    total_ratio = __builtins__['sum'](ratios) if isinstance(__builtins__, dict) else __builtins__.sum(ratios)
                    
                    # 使用 user_hash 对 total_ratio 取模，得到 0 到 total_ratio-1 的值
                    user_position = user_hash % total_ratio
                    
                    # 调试日志：查看分层结果
                    # print(f"RATIO DEBUG: user_id={user_id}, hash={user_hash}, position={user_position}, total={total_ratio}, ratios={ratios}")
                    
                    # 根据累积区间判断属于哪个分层
                    # 例：ratios=[30,30,40], total=100
                    # 0-29 -> 层级0, 30-59 -> 层级1, 60-99 -> 层级2
                    cumulative = 0
                    layer_index = len(child_tag_ids) - 1  # 默认最后一层
                    for i, ratio in enumerate(ratios):
                        if user_position < cumulative + ratio:
                            layer_index = i
                            break
                        cumulative += ratio
                    
                    # 调试日志
                    # print(f"RATIO RESULT: user_id={user_id}, layer_index={layer_index}, child_tag={child_tag_ids[layer_index]}")
                else:
                    result_tags.append(tag_id)
                    continue
                
                # 添加分配的子标签（确保是整数）
                assigned_child_tag = int(child_tag_ids[layer_index])
                result_tags.append(assigned_child_tag)
            else:
                # 不需要分层，直接保留
                result_tags.append(tag_id)
        
        # 返回去重排序后的结果（确保所有元素都是整数）
        try:
            unique_tags = list(set(result_tags))
            # 再次确保所有元素都是整数
            int_tags = [int(t) for t in unique_tags]
            return sorted(int_tags)
        except (ValueError, TypeError) as e:
            print(f"标签数组排序异常: {e}, result_tags={result_tags}")
            return tag_ids if tag_ids else []

    except json.JSONDecodeError as e:
        print(f"JSON 解析失败: {e}, input={tag_group_map_str}")
        import traceback
        traceback.print_exc()
        return tag_ids if tag_ids else []
    except Exception as e:
        # 异常情况返回原标签
        print(f"标签数组分层异常: {e}")
        print(f"详细错误信息: user_id={user_id}, tag_ids={tag_ids}, tag_group_map_str={tag_group_map_str[:200]}...")
        import traceback
        traceback.print_exc()
        return tag_ids if tag_ids else []


