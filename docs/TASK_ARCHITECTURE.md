# 任务化架构设计文档

## 概述

本文档详细介绍了大数据标签系统的任务化架构设计，包括任务抽象化、工厂模式、并行执行引擎以及RESTful API接口的技术实现。

## 架构设计理念

### 分布式开发支持

任务化架构的核心目标是支持分布式开发，解决以下问题：

1. **任务边界不清**：开发者难以区分不同标签任务
2. **代码耦合**：标签逻辑和系统架构紧密耦合
3. **并行开发困难**：多个团队难以同时开发不同标签
4. **维护成本高**：添加新标签需要修改核心代码

### 解决方案

通过任务化架构，我们实现了：

- **任务抽象化**：每个标签都是独立的任务类
- **工厂模式**：自动注册和管理任务类
- **并行执行**：支持多任务并行计算
- **规则驱动**：任务类从MySQL读取规则，无需硬编码

## 核心组件

### 1. 任务抽象基类 (BaseTagTask)

```python
# src/tasks/base_tag_task.py
class BaseTagTask:
    """标签任务抽象基类"""
    
    @property
    def tag_id(self) -> int:
        """获取标签ID（从类名或配置中推断）"""
        pass
    
    def get_required_fields(self) -> List[str]:
        """获取任务所需的数据字段"""
        pass
    
    def get_data_sources(self) -> Dict[str, str]:
        """获取数据源配置"""
        pass
    
    def validate_data(self, data: DataFrame) -> bool:
        """验证数据是否符合任务要求"""
        pass
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """数据预处理"""
        pass
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """结果后处理"""
        pass
```

### 2. 任务工厂 (TaskFactory)

```python
# src/tasks/task_factory.py
class TaskFactory:
    """任务工厂 - 负责任务类的注册和管理"""
    
    _registered_tasks: Dict[str, Type[BaseTagTask]] = {}
    
    @classmethod
    def register_task(cls, task_class: Type[BaseTagTask]) -> None:
        """注册任务类"""
        cls._registered_tasks[task_class.__name__] = task_class
    
    @classmethod
    def get_all_tasks(cls) -> List[Type[BaseTagTask]]:
        """获取所有注册的任务类"""
        return list(cls._registered_tasks.values())
    
    @classmethod
    def get_task_by_name(cls, name: str) -> Optional[Type[BaseTagTask]]:
        """根据名称获取任务类"""
        return cls._registered_tasks.get(name)
```

### 3. 任务并行引擎 (TaskParallelEngine)

```python
# src/engine/task_parallel_engine.py
class TaskParallelEngine:
    """任务并行执行引擎"""
    
    def __init__(self, spark: SparkSession, config: BaseConfig, max_workers: int = 4):
        self.spark = spark
        self.config = config
        self.max_workers = max_workers
        self.cached_mysql_rules = None
        self.data_cache = {}
    
    def execute_all_tasks(self, user_filter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """执行所有注册的任务类"""
        # 1. 预缓存MySQL规则数据
        self._preload_all_rules()
        
        # 2. 获取所有任务类
        all_tasks = [task_class() for task_class in TaskFactory.get_all_tasks()]
        
        # 3. 并行执行任务
        results = self._execute_tasks_parallel(all_tasks, user_filter)
        
        # 4. 合并结果
        return self._merge_task_results(results)
    
    def execute_specific_tag_tasks(self, tag_ids: List[int], user_filter: Optional[List[str]] = None) -> Optional[DataFrame]:
        """执行指定标签ID对应的任务类"""
        # 1. 获取标签ID到任务类的映射
        tag_to_task_mapping = self._get_tag_to_task_mapping()
        
        # 2. 找到对应的任务类
        target_tasks = []
        for tag_id in tag_ids:
            if tag_id in tag_to_task_mapping:
                task_class = tag_to_task_mapping[tag_id]
                target_tasks.append(task_class())
        
        # 3. 执行指定任务
        results = self._execute_tasks_parallel(target_tasks, user_filter)
        
        # 4. 合并结果
        return self._merge_task_results(results)
```

## 任务类组织结构

### 业务域划分

任务类按业务域进行组织：

```
src/tasks/
├── base_tag_task.py        # 抽象基类
├── task_factory.py         # 任务工厂
├── wealth/                 # 财富管理领域
│   ├── __init__.py
│   ├── high_net_worth_task.py      # 高净值用户任务
│   └── cash_rich_task.py           # 现金充足用户任务
├── behavior/               # 行为分析领域
│   ├── __init__.py
│   ├── active_trader_task.py       # 活跃交易者任务
│   └── recent_active_task.py       # 最近活跃用户任务
├── risk/                   # 风险管理领域
│   ├── __init__.py
│   └── low_risk_task.py            # 低风险用户任务
├── demographic/            # 人口特征领域
│   ├── __init__.py
│   └── young_user_task.py          # 年轻用户任务
├── lifecycle/              # 生命周期领域
│   ├── __init__.py
│   └── new_user_task.py            # 新用户任务
└── value/                  # 价值管理领域
    ├── __init__.py
    └── vip_user_task.py            # VIP用户任务
```

### 任务类实现示例

```python
# src/tasks/wealth/high_net_worth_task.py
from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from ..base_tag_task import BaseTagTask

class HighNetWorthUserTask(BaseTagTask):
    """高净值用户标签任务 - 标签ID: 1"""
    
    @property
    def tag_id(self) -> int:
        return 1
    
    def get_required_fields(self) -> List[str]:
        """高净值用户需要的数据字段"""
        return ['user_id', 'total_asset_value', 'cash_balance']
    
    def get_data_sources(self) -> Dict[str, str]:
        """数据源配置"""
        return {
            'primary': 'user_asset_summary',
            'secondary': None
        }
    
    def validate_data(self, data: DataFrame) -> bool:
        """验证数据完整性"""
        required_fields = self.get_required_fields()
        data_fields = data.columns
        
        missing_fields = set(required_fields) - set(data_fields)
        if missing_fields:
            return False
        
        return data.count() > 0
    
    def preprocess_data(self, raw_data: DataFrame) -> DataFrame:
        """数据预处理 - 过滤掉无效数据"""
        return raw_data.filter(
            col('total_asset_value').isNotNull() & 
            (col('total_asset_value') >= 0) &
            col('cash_balance').isNotNull() &
            (col('cash_balance') >= 0)
        )
    
    def post_process_result(self, tagged_users: DataFrame) -> DataFrame:
        """结果后处理 - 可添加额外的业务逻辑"""
        return tagged_users
```

## 标签ID映射系统

### 映射配置

```python
# src/engine/task_parallel_engine.py
def _get_tag_to_task_mapping(self) -> Dict[int, type]:
    """获取标签ID到任务类的映射"""
    return {
        1: HighNetWorthUserTask,       # 高净值用户
        2: ActiveTraderTask,           # 活跃交易者
        3: LowRiskUserTask,            # 低风险用户
        4: NewUserTask,                # 新注册用户
        5: VIPUserTask,                # VIP客户
        6: CashRichUserTask,           # 现金充足用户
        7: YoungUserTask,              # 年轻用户
        8: RecentActiveUserTask        # 最近活跃用户
    }
```

### 动态映射

未来可以实现动态映射：

```python
# 从数据库或配置文件读取映射关系
def _load_tag_mapping_from_database(self) -> Dict[int, str]:
    """从数据库加载标签ID到任务类名的映射"""
    query = """
    SELECT tag_id, task_class_name 
    FROM tag_task_mapping 
    WHERE is_active = 1
    """
    # 执行查询并返回映射字典
    pass
```

## 并行执行机制

### 执行流程

1. **任务发现**：通过TaskFactory获取所有注册的任务类
2. **数据预加载**：预缓存MySQL规则数据和用户数据
3. **任务并行**：使用PySpark的分布式计算并行执行任务
4. **结果合并**：将多个任务的结果合并为统一格式
5. **标签合并**：与MySQL现有标签进行合并

### 性能优化

```python
def _execute_tasks_parallel(self, tasks: List[BaseTagTask], user_filter: Optional[List[str]] = None) -> List[DataFrame]:
    """并行执行任务列表"""
    
    # 1. 预加载所有任务所需的数据源
    loaded_data = self._preload_data_sources(tasks)
    
    # 2. 并行执行所有任务
    results = []
    failed_tasks = []
    
    for task in tasks:
        try:
            # 验证MySQL规则存在
            mysql_rule = self._find_mysql_rule_for_task(task)
            if not mysql_rule:
                continue
            
            # 获取任务所需数据
            task_data = self._prepare_task_data(task, loaded_data)
            if not task_data or not task.validate_data(task_data):
                continue
            
            # 预处理数据
            processed_data = task.preprocess_data(task_data)
            
            # 执行标签计算
            tagged_users = self._compute_single_task(processed_data, task, mysql_rule['rule_conditions'])
            
            # 后处理结果
            if tagged_users and tagged_users.count() > 0:
                final_result = task.post_process_result(tagged_users)
                results.append(final_result)
                
        except Exception as e:
            failed_tasks.append(task.__class__.__name__)
            logger.error(f"任务执行失败: {task.__class__.__name__}: {str(e)}")
    
    return results
```

## RESTful API集成

### API架构

```python
# src/api/tag_trigger_api.py
class TagTriggerAPI:
    """标签任务触发API"""
    
    def __init__(self, env: str = 'local'):
        self.env = env
        self.app = Flask(__name__)
        self.task_manager = TaskManager()
        self._setup_routes()
    
    def _setup_routes(self):
        @self.app.route('/api/v1/tags/trigger', methods=['POST'])
        def trigger_tag_tasks():
            """触发标签任务接口"""
            data = request.get_json()
            tag_ids = data['tag_ids']
            user_ids = data.get('user_ids', None)
            mode = data.get('mode', 'full')
            
            # 生成任务ID
            task_id = str(uuid.uuid4())
            
            # 异步提交任务
            self.task_manager.submit_task(
                task_id=task_id,
                tag_ids=tag_ids,
                user_ids=user_ids,
                mode=mode,
                env=self.env
            )
            
            return jsonify({
                'success': True,
                'task_id': task_id,
                'message': '标签任务已成功提交'
            }), 202
```

### 异步任务管理

```python
# src/api/task_manager.py
class TaskManager:
    """异步任务管理器"""
    
    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.executor = ThreadPoolExecutor(max_workers=3)
    
    def submit_task(self, task_id: str, tag_ids: List[int], user_ids: Optional[List[str]] = None, 
                   mode: str = 'full', env: str = 'local') -> None:
        """提交异步任务"""
        # 记录任务状态
        self.tasks[task_id] = {
            'task_id': task_id,
            'tag_ids': tag_ids,
            'user_ids': user_ids,
            'mode': mode,
            'environment': env,
            'status': 'submitted',
            'submitted_at': datetime.now().isoformat()
        }
        
        # 异步执行任务
        self.executor.submit(self._execute_task, task_id, tag_ids, user_ids, mode, env)
    
    def _execute_task(self, task_id: str, tag_ids: List[int], user_ids: Optional[List[str]], 
                     mode: str, env: str) -> None:
        """执行标签任务"""
        try:
            # 更新任务状态
            self._update_task_status(task_id, 'running', started_at=datetime.now().isoformat())
            
            # 创建调度器并执行任务
            scheduler = TagScheduler(env=env)
            
            if user_ids:
                result = scheduler.scenario_task_specific_users_specific_tags(user_ids, tag_ids)
            else:
                result = scheduler.scenario_task_all_users_specific_tags(tag_ids)
            
            # 更新任务状态为完成
            self._update_task_status(task_id, 'completed', completed_at=datetime.now().isoformat())
            
        except Exception as e:
            # 更新任务状态为失败
            self._update_task_status(task_id, 'failed', error=str(e), completed_at=datetime.now().isoformat())
```

## 规则驱动系统

### MySQL规则集成

任务类不再包含硬编码的规则定义，而是从MySQL读取规则：

```python
def _find_mysql_rule_for_task(self, task: BaseTagTask) -> Optional[Dict[str, Any]]:
    """为任务找到对应的MySQL规则"""
    if not self.cached_mysql_rules:
        return None
    
    # 根据任务的tag_id查找规则
    for rule in self.cached_mysql_rules.collect():
        if rule['tag_id'] == task.tag_id:
            return rule.asDict()
    
    return None
```

### 规则缓存机制

```python
def _preload_all_rules(self) -> None:
    """预加载所有MySQL规则到内存"""
    if self.cached_mysql_rules is None:
        from src.readers.rule_reader import RuleReader
        
        rule_reader = RuleReader(self.spark, self.config.mysql)
        all_rules = rule_reader.read_active_rules()
        
        # 缓存规则数据
        self.cached_mysql_rules = all_rules.persist(StorageLevel.MEMORY_AND_DISK)
        
        logger.info(f"✅ 预加载MySQL规则完成，共 {self.cached_mysql_rules.count()} 条规则")
```

## 错误处理和监控

### 任务级错误处理

```python
def _execute_tasks_parallel(self, tasks: List[BaseTagTask], user_filter: Optional[List[str]] = None) -> List[DataFrame]:
    """并行执行任务 - 包含完善的错误处理"""
    results = []
    failed_tasks = []
    
    for task in tasks:
        try:
            # 执行任务的各个阶段
            result = self._execute_single_task(task, user_filter)
            if result:
                results.append(result)
            else:
                logger.warning(f"任务 {task.__class__.__name__} 没有产生结果")
                
        except Exception as e:
            logger.error(f"任务 {task.__class__.__name__} 执行失败: {str(e)}")
            failed_tasks.append(task.__class__.__name__)
    
    # 统计执行结果
    logger.info(f"任务执行完成 - 成功: {len(results)}, 失败: {len(failed_tasks)}")
    if failed_tasks:
        logger.warning(f"失败的任务: {failed_tasks}")
    
    return results
```

### 监控指标

```python
# 关键监控指标
class TaskMetrics:
    def __init__(self):
        self.task_execution_time = {}
        self.task_success_rate = {}
        self.task_data_volume = {}
    
    def record_task_execution(self, task_name: str, start_time: float, end_time: float, success: bool, data_count: int):
        """记录任务执行指标"""
        execution_time = end_time - start_time
        
        if task_name not in self.task_execution_time:
            self.task_execution_time[task_name] = []
        
        self.task_execution_time[task_name].append(execution_time)
        self.task_data_volume[task_name] = data_count
        
        # 计算成功率
        if task_name not in self.task_success_rate:
            self.task_success_rate[task_name] = {'success': 0, 'total': 0}
        
        self.task_success_rate[task_name]['total'] += 1
        if success:
            self.task_success_rate[task_name]['success'] += 1
```

## 扩展性设计

### 添加新任务类

1. **创建任务类**：继承BaseTagTask，实现所需方法
2. **注册任务**：通过TaskFactory自动注册
3. **配置映射**：在标签ID映射中添加配置
4. **添加规则**：在MySQL中添加对应的标签规则

```python
# 新任务类示例
class HighFrequencyTraderTask(BaseTagTask):
    """高频交易者任务 - 标签ID: 9"""
    
    @property
    def tag_id(self) -> int:
        return 9
    
    def get_required_fields(self) -> List[str]:
        return ['user_id', 'trade_count_30d', 'trade_frequency_score']
    
    def get_data_sources(self) -> Dict[str, str]:
        return {
            'primary': 'user_trading_summary',
            'secondary': None
        }
    
    # ... 其他方法实现
```

### 自动注册机制

```python
# src/tasks/__init__.py
"""任务模块初始化 - 自动注册所有任务类"""

import importlib
import pkgutil
from .task_factory import TaskFactory
from .base_tag_task import BaseTagTask

def auto_register_tasks():
    """自动注册所有任务类"""
    # 遍历所有子模块
    for importer, modname, ispkg in pkgutil.walk_packages(__path__, __name__ + "."):
        if not ispkg:
            try:
                module = importlib.import_module(modname)
                # 查找BaseTagTask的子类
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (isinstance(attr, type) and 
                        issubclass(attr, BaseTagTask) and 
                        attr != BaseTagTask):
                        TaskFactory.register_task(attr)
            except ImportError:
                pass

# 模块加载时自动注册
auto_register_tasks()
```

## 总结

任务化架构通过以下设计实现了分布式开发的目标：

1. **任务抽象化**：统一的任务接口，清晰的职责分离
2. **工厂模式**：自动化的任务管理和注册
3. **并行执行**：高效的多任务并行计算
4. **规则驱动**：灵活的MySQL规则配置
5. **API集成**：完整的RESTful API支持
6. **错误处理**：健壮的错误处理和监控
7. **扩展性**：易于添加新任务类和功能

这种架构使得不同团队可以独立开发各自的标签任务，同时保持系统的统一性和高性能。