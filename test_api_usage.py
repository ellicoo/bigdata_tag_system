#!/usr/bin/env python3
"""
API使用示例和测试 - 展示重构后的API模块
演示驼峰命名风格的API接口使用
"""

import sys
import os
import time
import requests
import json
import logging
from threading import Thread

# 添加项目路径到系统路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.batch.api import TagTriggerAPI, TaskManager, APIServer

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def testTaskManager():
    """测试任务管理器"""
    logger.info("🧪 测试任务管理器功能...")
    
    try:
        with TaskManager(maxWorkers=2) as taskManager:
            # 测试提交任务
            result1 = taskManager.submitTask(
                taskId="test_task_1",
                tagIds=[1, 2],
                userIds=None,
                mode="full",
                priority="normal",
                env="local"
            )
            logger.info(f"提交任务1结果: {result1}")
            
            # 测试提交另一个任务
            result2 = taskManager.submitTask(
                taskId="test_task_2", 
                tagIds=[3],
                userIds=["user_001", "user_002"],
                mode="full",
                priority="high",
                env="local"
            )
            logger.info(f"提交任务2结果: {result2}")
            
            # 等待一段时间让任务开始执行
            time.sleep(2)
            
            # 测试获取任务状态
            status1 = taskManager.getTaskStatus("test_task_1")
            logger.info(f"任务1状态: {status1}")
            
            # 测试列出所有任务
            allTasks = taskManager.listTasks()
            logger.info(f"所有任务: {len(allTasks)} 个任务")
            
            # 测试系统统计
            stats = taskManager.getSystemStats()
            logger.info(f"系统统计: {stats}")
            
            # 测试取消任务
            cancelResult = taskManager.cancelTask("test_task_2")
            logger.info(f"取消任务结果: {cancelResult}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试任务管理器失败: {str(e)}")
        return False


def testAPIServer():
    """测试API服务器（不实际启动，只测试初始化）"""
    logger.info("🧪 测试API服务器功能...")
    
    try:
        # 测试API服务器初始化
        server = APIServer(
            environment='local',
            host='localhost',
            port=5001,  # 使用不同端口避免冲突
            logLevel='INFO'
        )
        
        # 测试健康状态
        healthStatus = server.getHealthStatus()
        logger.info(f"服务器健康状态: {healthStatus}")
        
        # 测试关闭
        server.shutdown()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试API服务器失败: {str(e)}")
        return False


def testTagTriggerAPI():
    """测试标签触发API（Flask应用测试）"""
    logger.info("🧪 测试标签触发API功能...")
    
    try:
        # 创建API实例
        api = TagTriggerAPI(env='local')
        app = api.getApp()
        
        # 使用Flask测试客户端
        with app.test_client() as client:
            # 测试健康检查
            response = client.get('/health')
            logger.info(f"健康检查响应: {response.status_code}, {response.get_json()}")
            
            # 测试获取可用标签
            response = client.get('/api/v1/tags/available')
            logger.info(f"可用标签响应: {response.status_code}, 标签数: {len(response.get_json().get('available_tags', []))}")
            
            # 测试触发标签任务
            triggerData = {
                'tag_ids': [1, 2],
                'mode': 'full',
                'priority': 'normal'
            }
            response = client.post('/api/v1/tags/trigger', 
                                 data=json.dumps(triggerData),
                                 content_type='application/json')
            result = response.get_json()
            logger.info(f"触发任务响应: {response.status_code}, 任务ID: {result.get('task_id')}")
            
            # 测试获取任务状态
            if result.get('task_id'):
                taskId = result['task_id']
                time.sleep(1)  # 等待任务开始
                
                response = client.get(f'/api/v1/tasks/{taskId}/status')
                logger.info(f"任务状态响应: {response.status_code}, {response.get_json()}")
            
            # 测试列出任务
            response = client.get('/api/v1/tasks?limit=10')
            result = response.get_json()
            logger.info(f"任务列表响应: {response.status_code}, 任务数: {result.get('total', 0)}")
            
            # 测试系统统计
            response = client.get('/api/v1/system/stats')
            logger.info(f"系统统计响应: {response.status_code}")
        
        # 关闭API
        api.shutdown()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试标签触发API失败: {str(e)}")
        return False


def startTestServer():
    """启动测试服务器（在后台线程中）"""
    def runServer():
        try:
            server = APIServer(
                environment='local',
                host='localhost',
                port=5002,
                logLevel='WARNING'  # 减少日志输出
            )
            server.start(debug=False)
        except Exception as e:
            logger.warning(f"测试服务器异常: {str(e)}")
    
    thread = Thread(target=runServer, daemon=True)
    thread.start()
    time.sleep(3)  # 等待服务器启动
    return thread


def testAPIRequests():
    """测试真实的HTTP请求"""
    logger.info("🧪 测试真实HTTP请求...")
    
    try:
        # 启动测试服务器
        serverThread = startTestServer()
        baseUrl = "http://localhost:5002"
        
        # 测试健康检查
        response = requests.get(f"{baseUrl}/health", timeout=5)
        logger.info(f"HTTP健康检查: {response.status_code}")
        
        # 测试获取可用标签
        response = requests.get(f"{baseUrl}/api/v1/tags/available", timeout=5)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"HTTP可用标签: {len(data.get('available_tags', []))} 个标签")
        
        # 测试触发任务
        triggerData = {
            'tag_ids': [1],
            'mode': 'full'
        }
        response = requests.post(f"{baseUrl}/api/v1/tags/trigger", 
                               json=triggerData, timeout=10)
        if response.status_code == 202:
            result = response.json()
            taskId = result.get('task_id')
            logger.info(f"HTTP触发任务成功: {taskId}")
            
            # 查询任务状态
            time.sleep(2)
            response = requests.get(f"{baseUrl}/api/v1/tasks/{taskId}/status", timeout=5)
            if response.status_code == 200:
                status = response.json()
                logger.info(f"HTTP任务状态: {status['status']['status']}")
        
        return True
        
    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠️ HTTP请求测试失败（可能是服务器未启动）: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"❌ 测试HTTP请求失败: {str(e)}")
        return False


def testAPIUsage():
    """测试API模块的完整用法"""
    logger.info("🚀 开始测试API模块...")
    
    allTestsPassed = True
    
    # 测试1: 任务管理器
    logger.info("\n" + "="*50)
    logger.info("测试1: 任务管理器")
    logger.info("="*50)
    test1Result = testTaskManager()
    allTestsPassed = allTestsPassed and test1Result
    logger.info(f"测试1结果: {'✅ 通过' if test1Result else '❌ 失败'}")
    
    # 测试2: API服务器
    logger.info("\n" + "="*50)
    logger.info("测试2: API服务器")
    logger.info("="*50)
    test2Result = testAPIServer()
    allTestsPassed = allTestsPassed and test2Result
    logger.info(f"测试2结果: {'✅ 通过' if test2Result else '❌ 失败'}")
    
    # 测试3: 标签触发API
    logger.info("\n" + "="*50)
    logger.info("测试3: 标签触发API")
    logger.info("="*50)
    test3Result = testTagTriggerAPI()
    allTestsPassed = allTestsPassed and test3Result
    logger.info(f"测试3结果: {'✅ 通过' if test3Result else '❌ 失败'}")
    
    # 测试4: HTTP请求
    logger.info("\n" + "="*50)
    logger.info("测试4: HTTP请求")
    logger.info("="*50)
    test4Result = testAPIRequests()
    allTestsPassed = allTestsPassed and test4Result
    logger.info(f"测试4结果: {'✅ 通过' if test4Result else '⚠️ 跳过'}")
    
    # 总结
    logger.info("\n" + "="*60)
    logger.info("🎯 API模块测试总结")
    logger.info("="*60)
    logger.info(f"总体结果: {'✅ 所有测试通过' if allTestsPassed else '❌ 部分测试失败'}")
    logger.info("🔧 重构后的API模块特性:")
    logger.info("   📁 重构位置: src/batch/api/")
    logger.info("   🐪 驼峰命名: TagTriggerAPI, TaskManager, APIServer")
    logger.info("   📦 Bean集成: 使用TaskExecutionContext管理任务状态")
    logger.info("   🔄 异步支持: 完整的异步任务管理和状态跟踪")
    logger.info("   🚀 RESTful API: 标准的REST接口设计")
    logger.info("   🎯 任务优先级: 支持任务优先级和取消机制")
    logger.info("   📊 系统监控: 完整的统计和监控功能")
    
    return allTestsPassed


if __name__ == "__main__":
    try:
        success = testAPIUsage()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 测试被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 测试执行异常: {str(e)}")
        sys.exit(1)