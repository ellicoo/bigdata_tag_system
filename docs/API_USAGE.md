# 标签任务触发API使用文档

## 概述

标签任务触发API提供了一个RESTful接口，允许后端系统通过HTTP请求触发标签计算任务。API支持异步任务处理，确保不会阻塞调用方。

## 启动API服务器

### 基本启动
```bash
# 启动本地环境API服务器
python api_server.py --env local

# 启动开发环境API服务器
python api_server.py --env glue-dev --host 0.0.0.0 --port 5000

# 启动生产环境API服务器
python api_server.py --env glue-prod --host 0.0.0.0 --port 8080
```

### 参数说明
- `--env`: 运行环境 (local/glue-dev/glue-prod)
- `--host`: 监听地址 (默认: 0.0.0.0)
- `--port`: 监听端口 (默认: 5000)
- `--debug`: 开启调试模式
- `--log-level`: 日志级别 (DEBUG/INFO/WARNING/ERROR)

## API接口

### 1. 健康检查

**GET** `/health`

检查API服务器是否正常运行。

**响应示例:**
```json
{
    "status": "healthy",
    "timestamp": "2024-01-20T10:30:00",
    "environment": "local"
}
```

### 2. 触发标签任务

**POST** `/api/v1/tags/trigger`

触发指定标签的计算任务。

**请求体:**
```json
{
    "tag_ids": [1, 2, 3],                    // 必需: 标签ID列表
    "user_ids": ["user_000001", "user_000002"], // 可选: 指定用户列表
    "mode": "full"                           // 可选: 执行模式 (full/incremental)
}
```

**响应示例 (成功):**
```json
{
    "success": true,
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "message": "标签任务已成功提交",
    "data": {
        "tag_ids": [1, 2, 3],
        "user_ids": ["user_000001", "user_000002"],
        "mode": "full",
        "environment": "local",
        "submitted_at": "2024-01-20T10:30:00"
    }
}
```

**响应示例 (失败):**
```json
{
    "success": false,
    "error": "Invalid tag_ids: [999]",
    "message": "无效的标签ID: [999]",
    "available_tag_ids": [1, 2, 3, 4, 5, 6, 7, 8]
}
```

### 3. 查询任务状态

**GET** `/api/v1/tasks/{task_id}/status`

查询指定任务的执行状态。

**响应示例:**
```json
{
    "success": true,
    "task_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": {
        "task_id": "550e8400-e29b-41d4-a716-446655440000",
        "tag_ids": [1, 2, 3],
        "user_ids": ["user_000001", "user_000002"],
        "mode": "full",
        "environment": "local",
        "status": "completed",
        "submitted_at": "2024-01-20T10:30:00",
        "started_at": "2024-01-20T10:30:05",
        "completed_at": "2024-01-20T10:32:15",
        "result": {
            "total_users": 2,
            "message": "Successfully processed 2 users"
        }
    }
}
```

**任务状态说明:**
- `submitted`: 任务已提交，等待执行
- `running`: 任务正在执行
- `completed`: 任务执行成功
- `failed`: 任务执行失败

### 4. 列出所有任务

**GET** `/api/v1/tasks`

获取所有任务的列表（按提交时间降序）。

**响应示例:**
```json
{
    "success": true,
    "tasks": [
        {
            "task_id": "550e8400-e29b-41d4-a716-446655440000",
            "tag_ids": [1, 2, 3],
            "status": "completed",
            "submitted_at": "2024-01-20T10:30:00",
            "completed_at": "2024-01-20T10:32:15"
        }
    ]
}
```

### 5. 获取可用标签

**GET** `/api/v1/tags/available`

获取所有可用的标签任务。

**响应示例:**
```json
{
    "success": true,
    "available_tags": [
        {
            "tag_id": 1,
            "task_class": "HighNetWorthUserTask",
            "description": "高净值用户"
        },
        {
            "tag_id": 2,
            "task_class": "ActiveTraderTask",
            "description": "活跃交易者"
        },
        {
            "tag_id": 3,
            "task_class": "LowRiskUserTask",
            "description": "低风险用户"
        }
    ]
}
```

## 使用示例

### Python客户端示例

```python
import requests
import json
import time

# API服务器地址
API_BASE_URL = "http://localhost:5000"

def trigger_tag_task(tag_ids, user_ids=None):
    """触发标签任务"""
    url = f"{API_BASE_URL}/api/v1/tags/trigger"
    
    payload = {
        "tag_ids": tag_ids,
        "mode": "full"
    }
    
    if user_ids:
        payload["user_ids"] = user_ids
    
    response = requests.post(url, json=payload)
    
    if response.status_code == 202:
        result = response.json()
        print(f"任务提交成功: {result['task_id']}")
        return result['task_id']
    else:
        print(f"任务提交失败: {response.json()}")
        return None

def check_task_status(task_id):
    """检查任务状态"""
    url = f"{API_BASE_URL}/api/v1/tasks/{task_id}/status"
    response = requests.get(url)
    
    if response.status_code == 200:
        result = response.json()
        return result['status']
    else:
        print(f"查询任务状态失败: {response.json()}")
        return None

def wait_for_task_completion(task_id, timeout=300):
    """等待任务完成"""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        status = check_task_status(task_id)
        if status:
            if status['status'] == 'completed':
                print(f"任务完成: {status['result']}")
                return True
            elif status['status'] == 'failed':
                print(f"任务失败: {status['error']}")
                return False
            else:
                print(f"任务状态: {status['status']}")
                time.sleep(5)
        else:
            time.sleep(5)
    
    print(f"任务执行超时")
    return False

# 使用示例
if __name__ == "__main__":
    # 1. 触发高净值用户和活跃交易者标签任务
    task_id = trigger_tag_task([1, 2])
    
    if task_id:
        # 2. 等待任务完成
        wait_for_task_completion(task_id)
    
    # 3. 指定用户指定标签
    task_id = trigger_tag_task([1, 3], ["user_000001", "user_000002"])
    
    if task_id:
        wait_for_task_completion(task_id)
```

### cURL示例

```bash
# 1. 健康检查
curl -X GET http://localhost:5000/health

# 2. 触发标签任务
curl -X POST http://localhost:5000/api/v1/tags/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "tag_ids": [1, 2, 3],
    "mode": "full"
  }'

# 3. 查询任务状态
curl -X GET http://localhost:5000/api/v1/tasks/550e8400-e29b-41d4-a716-446655440000/status

# 4. 列出所有任务
curl -X GET http://localhost:5000/api/v1/tasks

# 5. 获取可用标签
curl -X GET http://localhost:5000/api/v1/tags/available
```

## 错误处理

API使用标准的HTTP状态码和JSON错误响应：

- `200 OK`: 请求成功
- `202 Accepted`: 任务已提交
- `400 Bad Request`: 请求参数错误
- `404 Not Found`: 资源不存在
- `500 Internal Server Error`: 服务器内部错误

错误响应格式：
```json
{
    "success": false,
    "error": "错误代码或描述",
    "message": "中文错误信息"
}
```

## 并发和性能

- API服务器使用线程池处理并发任务（默认最大3个并发任务）
- 每个任务内部使用PySpark的分布式计算，支持大数据处理
- 任务状态存储在内存中，服务器重启后会丢失历史任务信息
- 建议在生产环境中使用外部任务队列（如Redis/RabbitMQ）存储任务状态

## 监控和日志

- API服务器日志输出到控制台和 `api_server.log` 文件
- 每个任务的执行日志包含详细的执行统计信息
- 可以通过任务状态接口监控任务执行进度
- 建议集成外部监控系统（如Prometheus）进行生产环境监控