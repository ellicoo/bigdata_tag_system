#!/usr/bin/env python3
"""
测试6个功能场景的脚本
验证新实现的并行计算和标签合并逻辑
"""

import sys
import os
import subprocess
import time
import logging

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def run_command(command: str) -> bool:
    """运行命令并返回是否成功"""
    try:
        logger.info(f"🚀 执行命令: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ 命令执行成功")
            logger.info(f"输出: {result.stdout[-500:]}")  # 显示最后500字符
            return True
        else:
            logger.error("❌ 命令执行失败")
            logger.error(f"错误: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 命令执行异常: {str(e)}")
        return False

def test_scenario_1():
    """测试场景1: 全量用户打全量标签"""
    logger.info("=" * 60)
    logger.info("🎯 测试场景1: 全量用户打全量标签")
    logger.info("- 多标签并行计算")
    logger.info("- 内存合并同用户多标签结果") 
    logger.info("- 不与MySQL现有标签合并")
    logger.info("- INSERT ON DUPLICATE KEY UPDATE写入")
    logger.info("=" * 60)
    
    command = "python main.py --env local --mode full-parallel"
    return run_command(command)

def test_scenario_2():
    """测试场景2: 全量用户打指定标签"""
    logger.info("=" * 60)
    logger.info("🎯 测试场景2: 全量用户打指定标签")
    logger.info("- 多标签并行计算") 
    logger.info("- 内存合并同用户多标签结果")
    logger.info("- 与MySQL现有标签合并")
    logger.info("- INSERT ON DUPLICATE KEY UPDATE写入")
    logger.info("=" * 60)
    
    command = "python main.py --env local --mode tags-parallel --tag-ids 1,2,3"
    return run_command(command)

def test_scenario_3():
    """测试场景3: 增量用户打全量标签"""
    logger.info("=" * 60)
    logger.info("🎯 测试场景3: 增量用户打全量标签")
    logger.info("- 识别新增用户")
    logger.info("- 多标签并行计算")
    logger.info("- 内存合并同用户多标签结果")
    logger.info("- 不与MySQL现有标签合并（新用户）")
    logger.info("- INSERT ON DUPLICATE KEY UPDATE写入")
    logger.info("=" * 60)
    
    command = "python main.py --env local --mode incremental-parallel --days 7"
    return run_command(command)

def test_scenario_4():
    """测试场景4: 增量用户打指定标签"""
    logger.info("=" * 60)
    logger.info("🎯 测试场景4: 增量用户打指定标签")
    logger.info("- 识别新增用户")
    logger.info("- 多标签并行计算")
    logger.info("- 内存合并同用户多标签结果")
    logger.info("- 不与MySQL现有标签合并（新用户）")
    logger.info("- INSERT ON DUPLICATE KEY UPDATE写入")
    logger.info("=" * 60)
    
    command = "python main.py --env local --mode incremental-tags-parallel --days 7 --tag-ids 2,4"
    return run_command(command)

def test_scenario_5():
    """测试场景5: 指定用户打全量标签"""
    logger.info("=" * 60)
    logger.info("🎯 测试场景5: 指定用户打全量标签")
    logger.info("- 过滤指定用户")
    logger.info("- 多标签并行计算")
    logger.info("- 内存合并同用户多标签结果")
    logger.info("- 不与MySQL现有标签合并")
    logger.info("- INSERT ON DUPLICATE KEY UPDATE写入")
    logger.info("=" * 60)
    
    command = "python main.py --env local --mode users-parallel --user-ids user_000001,user_000002,user_000003"
    return run_command(command)

def test_scenario_6():
    """测试场景6: 指定用户打指定标签"""
    logger.info("=" * 60)
    logger.info("🎯 测试场景6: 指定用户打指定标签")
    logger.info("- 过滤指定用户")
    logger.info("- 多标签并行计算")
    logger.info("- 内存合并同用户多标签结果")
    logger.info("- 与MySQL现有标签合并")
    logger.info("- INSERT ON DUPLICATE KEY UPDATE写入")
    logger.info("=" * 60)
    
    command = "python main.py --env local --mode user-tags-parallel --user-ids user_000001,user_000002 --tag-ids 1,3,5"
    return run_command(command)

def test_health_check():
    """测试系统健康检查"""
    logger.info("=" * 60)
    logger.info("🏥 测试系统健康检查")
    logger.info("=" * 60)
    
    command = "python main.py --env local --mode health"
    return run_command(command)

def main():
    """主函数"""
    logger.info("🚀 开始测试6个功能场景")
    
    # 测试计划
    test_cases = [
        ("健康检查", test_health_check),
        ("场景1", test_scenario_1),
        ("场景2", test_scenario_2), 
        ("场景3", test_scenario_3),
        ("场景4", test_scenario_4),
        ("场景5", test_scenario_5),
        ("场景6", test_scenario_6)
    ]
    
    results = {}
    
    for test_name, test_func in test_cases:
        logger.info(f"\n🔄 开始测试: {test_name}")
        start_time = time.time()
        
        try:
            success = test_func()
            results[test_name] = success
            
            if success:
                logger.info(f"✅ {test_name} 测试通过")
            else:
                logger.error(f"❌ {test_name} 测试失败")
                
        except Exception as e:
            logger.error(f"❌ {test_name} 测试异常: {str(e)}")
            results[test_name] = False
        
        end_time = time.time()
        logger.info(f"⏱️ {test_name} 耗时: {end_time - start_time:.2f}秒")
        
        # 每个测试之间暂停一下
        time.sleep(2)
    
    # 汇总结果
    logger.info("\n" + "=" * 60)
    logger.info("📊 测试结果汇总")
    logger.info("=" * 60)
    
    success_count = 0
    total_count = len(results)
    
    for test_name, success in results.items():
        status = "✅ 通过" if success else "❌ 失败"
        logger.info(f"{test_name}: {status}")
        if success:
            success_count += 1
    
    logger.info("=" * 60)
    logger.info(f"📈 总体结果: {success_count}/{total_count} 测试通过")
    
    if success_count == total_count:
        logger.info("🎉 所有测试场景都通过了！")
        return 0
    else:
        logger.error(f"💔 有 {total_count - success_count} 个测试场景失败")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)