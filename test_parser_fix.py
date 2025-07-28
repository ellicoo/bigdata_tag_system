#!/usr/bin/env python3
"""
测试TagRuleParser修复效果
"""
import sys
sys.path.append('.')

from src.tag_engine.parser.TagRuleParser import TagRuleParser

def test_parser():
    """测试解析器"""
    print("🧪 测试TagRuleParser修复效果...")
    
    # 测试JSON规则（来自你的示例）
    test_rule = """{
        "logic": "AND", 
        "conditions": [
            {
                "condition": {
                    "logic": "None", 
                    "fields": [
                        {
                            "table": "tag_system.user_asset_summary", 
                            "field": "total_asset_value", 
                            "operator": "=", 
                            "value": "100000", 
                            "type": "number"
                        }
                    ]
                }
            }
        ]
    }"""
    
    parser = TagRuleParser()
    
    # 测试表名提取
    print("\n1. 测试表名提取:")
    tables = parser._extractTablesFromRule(test_rule)
    print(f"   提取到的表: {list(tables) if tables else '[]'}")
    
    # 测试字段提取  
    print("\n2. 测试字段提取:")
    fields = parser._extractFieldsFromRule(test_rule)
    print(f"   提取到的字段依赖: {dict(fields) if fields else '{}'}")
    
    # 测试SQL生成
    print("\n3. 测试SQL生成:")
    sql = parser.parseRuleToSql(test_rule)
    print(f"   生成的SQL: {sql}")
    
    # 判断修复是否成功
    if tables and 'tag_system.user_asset_summary' in tables:
        print("\n✅ 修复成功！能够正确提取表名")
    else:
        print("\n❌ 修复失败！仍然无法提取表名")
        return False
        
    if fields and 'tag_system.user_asset_summary' in fields:
        print("✅ 字段依赖提取成功！")
    else:
        print("❌ 字段依赖提取失败！")
        return False
    
    return True

if __name__ == "__main__":
    success = test_parser()
    if success:
        print("\n🎉 TagRuleParser修复测试通过！")
    else:
        print("\n💥 TagRuleParser修复测试失败！")
        sys.exit(1)