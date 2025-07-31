# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an enterprise-level big data tag computing system designed for DolphinScheduler deployment. The system uses PySpark DSL and custom UDFs to read user data from S3 Hive tables, combines with JSON format rules in MySQL for distributed tag computation, and writes results back to MySQL.

## Core Architecture

### Tech Stack
- **Computing Engine**: PySpark 3.5+ (Spark SQL + DataFrame API)
- **Data Source**: S3 Hive Tables (Parquet format)
- **Rule Storage**: MySQL (JSON format rules)
- **Scheduler**: DolphinScheduler
- **Deployment**: YARN Cluster mode

### Project Structure
```
src/tag_engine/                    # Core tag engine
├── main.py                       # Unified command line entry
├── engine/                       # Tag computing engine
│   ├── TagEngine.py             # Main orchestration engine
│   └── TagGroup.py              # Smart grouping based on table dependencies
├── meta/                        # Data source management
│   ├── HiveMeta.py             # Hive table operations with smart caching
│   └── MysqlMeta.py            # MySQL rule and result management
├── parser/                      # Rule parsing and SQL generation
│   └── TagRuleParser.py        # JSON rule to SQL condition converter
└── utils/                       # User defined functions
    └── SparkUdfs.py            # PySpark UDF function collection (module-level functions)

dolphin_gui_deploy/               # DolphinScheduler deployment package
├── main.py                      # Deployment entry (wrapper)
├── generate_test_data.py        # Test data generator
├── create_test_tables.sql       # Hive table creation SQL
└── deployment_guide.md          # Detailed deployment guide
```

## Core Components

### 1. TagEngine.py - Main Orchestration Engine
**Responsibilities**: Overall orchestration and execution of tag computation workflow
- Manage Hive and MySQL data source connections
- Coordinate tag grouping and parallel computation
- Execute tag merging and result writing
- Provide health checks and system monitoring

**Key Methods**:
- `computeTags()`: Main method for tag computation
- `healthCheck()`: System health check
- `_mergeAllTagResults()`: Tag merging using Spark native functions

### 2. TagGroup.py - Smart Parallel Processing
**Responsibilities**: Smart grouping and concurrent computation based on table dependencies
- Analyze table dependencies in tag rules
- Group tags by dependent tables intelligently
- Execute one JOIN operation per group, compute all tags in group in parallel
- Maximize resource utilization, reduce duplicate data reading

### 3. SparkUdfs.py - Type-Safe Module Functions
**Responsibilities**: Provide type-safe tag processing functions using module-level approach
- `merge_user_tags()`: Merge tags using Spark native functions (array_distinct + array_sort)
- `merge_with_existing_tags()`: Smart merging with array_union for performance
- `json_to_array()` / `array_to_json()`: Type-safe JSON conversions

**Design Philosophy**: Module-level functions avoid Python object serialization issues in YARN cluster environments.

**Core Functions**:
```python
def merge_user_tags(tag_column):
    """Merge single user's multiple tags using Spark native functions
    
    Uses Spark native functions: array_distinct + array_sort
    Avoids UDF serialization overhead
    """
    return array_distinct(array_sort(tag_column))

def merge_with_existing_tags(new_tags_col, existing_tags_col):
    """Smart merging of new and existing tags
    
    Uses Spark native functions: array_union + array_distinct + array_sort
    Handles null values automatically with coalesce()
    """
    new_tags = coalesce(new_tags_col, array())
    existing_tags = coalesce(existing_tags_col, array())
    
    return array_distinct(
        array_sort(
            array_union(new_tags, existing_tags)
        )
    )
```

### 4. MysqlMeta.py - MySQL Data Management
**Responsibilities**: Unified MySQL data access layer
- Load tag rules and existing user tags
- Batch UPSERT tag results to MySQL
- Support timestamp management and idempotent operations

**Configuration Usage**:
- JDBC URL construction: `host:port/database`
- PySpark reading: Pass username/password via `.option()`
- PyMySQL writing: Use complete config dict for direct connection

### 5. Main Entry main.py
**Responsibilities**: Unified command line entry supporting multiple execution modes
- Environment variable configuration loading
- MySQL configuration management
- Test data generator integration

**Supported Execution Modes**:
```bash
# Health check
python src/tag_engine/main.py --mode health

# Full tag computation
python src/tag_engine/main.py --mode task-all

# Specific tag computation
python src/tag_engine/main.py --mode task-tags --tag-ids 1,2,3

# Generate test data
python src/tag_engine/main.py --mode generate-test-data

# List available tasks
python src/tag_engine/main.py --mode list-tasks
```

## Smart Tag Merging Mechanism

The system uses a **Spark native functions + UDF** hybrid strategy to ensure type safety and high performance:

### In-Memory Tag Merging (Performance Optimization)
```python
# Use Spark native functions for tag merging, avoid UDF serialization overhead
finalDF = mergedDF.groupBy("user_id").agg(
    array_distinct(
        array_sort(
            flatten(collect_list("tag_ids_array"))
        )
    ).alias("merged_tag_ids")
)
```

### MySQL Tag Merging (Business Logic)
```python
# Merge with existing MySQL tags, support historical tag preservation
finalDF = joinedDF.withColumn(
    "final_tag_ids",
    tagUdfs.mergeWithExistingTags(
        col("new.merged_tag_ids"),
        col("existing.existing_tag_ids")
    )
)
```

## JSON Rule System

Supports complex business rule definitions, rules stored in JSON format in MySQL:

```json
{
  "logic": "AND",
  "conditions": [
    {
      "fields": [
        {
          "table": "user_basic_info",
          "field": "age",
          "operator": ">=", 
          "value": 30,
          "type": "number"
        },
        {
          "table": "user_asset_summary", 
          "field": "total_assets",
          "operator": ">=",
          "value": 100000,
          "type": "number"
        }
      ]
    }
  ]
}
```

## Data Model

### Input Data Sources
- `user_basic_info`: User basic information (age, gender, registration time, etc.)
- `user_asset_summary`: User asset summary (total assets, cash balance, etc.)  
- `user_activity_summary`: User activity summary (transaction count, login time, etc.)

### Output Results
- `user_tags` table: Uses **one record per user** design
  ```sql
  CREATE TABLE user_tags (
      id BIGINT PRIMARY KEY AUTO_INCREMENT,
      user_id VARCHAR(100) NOT NULL,
      tag_ids JSON NOT NULL COMMENT 'All tag IDs for user as array [1,2,3,5]',
      tag_details JSON COMMENT 'Tag detailed information',
      created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_user_id (user_id)
  );
  ```

## DolphinScheduler Deployment

### Deployment Package Structure
```
bigdata_tag_system/              # Deployment directory
├── main.py                     # Deployment entry (wrapper)
├── src/                        # Core source code
├── generate_test_data.py       # Test data generator
├── create_test_tables.sql      # Table creation SQL
└── requirements.txt            # Python dependencies
```

### Standard Deployment Process
1. **Extract Package** → **Fix File Permissions** → **Install Dependencies** → **Health Check** → **Full Tag Computation** → **Specific Tag Computation**

### Spark Task Configuration
```
Main Program: /dolphinscheduler/default/resources/bigdata_tag_system/main.py
Program Arguments: --mode task-all
Driver Memory: 4g
Executor Count: 5
Executor Memory: 8g
```

## Configuration Management

### MySQL Configuration (Environment Variables)
```bash
export MYSQL_HOST="cex-mysql-test.c5mgk4qm8m2z.ap-southeast-1.rds.amazonaws.com"
export MYSQL_PORT="3358"
export MYSQL_DATABASE="biz_statistics"
export MYSQL_USER="root"
export MYSQL_PASSWORD="ayjUzzH8b7gcQYRh"
```

### Spark Optimization Configuration
```python
spark = SparkSession.builder \
    .appName(app_name) \
    .enableHiveSupport() \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

## Performance Optimization Strategies

### 1. Smart Caching Strategy
- **Table-level caching**: Use `persist(StorageLevel.MEMORY_AND_DISK)`
- **Partition optimization**: Dynamically adjust partition count for better parallelism
- **Column projection**: Load only necessary columns to reduce I/O

### 2. Type Safety and Performance Balance
- **Spark native functions first**: Use `array_distinct`, `array_sort`, `flatten`
- **UDF backup strategy**: Use type-safe UDFs for complex logic
- **Serialization optimization**: Reduce UDF calls, avoid Python-JVM serialization overhead

### 3. Parallel Processing Optimization  
- **Dependency analysis**: Smart grouping based on table dependencies
- **Batch computation**: Parallel computation within groups, share table read results
- **Resource management**: Proper Spark resource configuration and memory management

## Development Guide

### Adding New Tags
1. **MySQL rule configuration**: Insert JSON format rules in `tag_rules` table
2. **Test validation**: Use `--mode task-tags --tag-ids NEW_TAG_ID` for testing
3. **Deployment update**: Use `dolphin_deploy_package.py` to generate new deployment package

### Custom UDF Development
Add new functions in `SparkUdfs.py`, prioritize module-level functions:
- **Spark native functions first**: Use Column expressions when possible
- **Type safety**: Explicit type definitions with comprehensive null handling
- **Module-level approach**: Avoid UDF classes to prevent serialization issues

### Test Data Generation
- DolphinScheduler environment: Use `dolphin_gui_deploy/generate_test_data.py`
- Local development environment: Use `environments/local/test_data_generator.py`

## Testing Framework

### Test Architecture Overview  
The project employs a comprehensive **pytest + PySpark** testing framework providing complete unit and integration test coverage.

**Test Structure**:
```
tests/
├── conftest.py              # Pytest fixtures and test configuration
├── test_rule_parser.py      # JSON rule parsing and SQL generation tests (14 tests)
└── test_tag_grouping.py     # Tag grouping algorithm validation tests (8 tests)
```

### Running Tests

#### **Complete Test Suite**
```bash
# Run all tests (recommended)
python -m pytest tests/ -v

# Run with coverage report
python -m pytest tests/ -v --cov=src/tag_engine --cov-report=html

# Test results overview
======================== test session starts ========================
tests/test_rule_parser.py::TestTagRuleParser::test_init PASSED [  4%]
tests/test_rule_parser.py::TestTagRuleParser::test_not_logic PASSED [ 45%]  
tests/test_tag_grouping.py::TestTagGrouping::test_group_tags_complex_scenario PASSED [ 90%]
======================== 22 passed, 1 warning in 13.07s ========================
```

#### **Module-Specific Testing**
```bash
# Rule parser tests only
python -m pytest tests/test_rule_parser.py -v

# Tag grouping tests only  
python -m pytest tests/test_tag_grouping.py -v

# Single test case
python -m pytest tests/test_rule_parser.py::TestTagRuleParser::test_not_logic -v
```

### Test Coverage Analysis

#### **TagRuleParser Tests (14 test cases)**
- ✅ **Initialization**: Basic component setup and configuration
- ✅ **SQL Generation**: Number, string, enum, date, boolean condition parsing
- ✅ **Operator Support**: `=`, `!=`, `>=`, `<=`, `LIKE`, `IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`
- ✅ **Complex Logic**: Nested AND/OR/NOT conditions with unlimited depth
- ✅ **String Patterns**: `contains`, `starts_with`, `ends_with`, `not_contains`
- ✅ **Array Operations**: `contains_any`, `contains_all`, `array_contains` for list fields
- ✅ **Edge Cases**: Invalid JSON, empty rules, malformed conditions
- ✅ **Logic Precedence**: Field-level vs condition-level logic handling

#### **TagGrouping Tests (8 test cases)**
- ✅ **Dependency Analysis**: Single-table and multi-table dependency extraction
- ✅ **Field Dependency**: Required field identification with user_id auto-addition
- ✅ **Smart Grouping**: Same-table grouping, different-table grouping strategies
- ✅ **Complex Scenarios**: Multi-level grouping with overlapping dependencies
- ✅ **Edge Handling**: Empty rules, invalid JSON rule processing
- ✅ **Optimization Validation**: Group efficiency and resource sharing verification

### Test Data Model

#### **Test Environment Configuration**
```python
# tests/conftest.py
@pytest.fixture(scope="session")  
def spark():
    """Local Spark session optimized for testing"""
    return SparkSession.builder \
        .appName("TagSystem_Test") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

@pytest.fixture
def sample_rules_data():
    """Comprehensive business rule test cases"""
    return [
        {
            "tag_id": 1,
            "tag_name": "高净值用户", 
            "rule_conditions": json.dumps({...})  # Complex multi-condition rules
        },
        # ... more test scenarios
    ]
```

#### **Realistic Test Data**
```python
@pytest.fixture
def sample_user_data():
    """Production-like user data for testing"""
    return {
        "user_basic_info": [
            ("user001", 30, "VIP2", "verified", True),
            ("user002", 25, "VIP1", "verified", False),
            ("user003", 35, "VIP3", "pending", True)
        ],
        "user_asset_summary": [
            ("user001", 150000.00, 50000.00),
            ("user002", 80000.00, 20000.00)  
        ],
        "user_activity_summary": [
            ("user001", 15, "2025-07-30"),
            ("user002", 8, "2025-07-29")
        ]
    }
```

### Test Quality Assurance

#### **Comprehensive Rule Testing**
```python
def test_complex_multi_condition_and_logic(self):
    """Test complex nested AND/OR logic with multiple tables"""
    rule_json = json.dumps({
        "logic": "AND",
        "conditions": [
            {
                "condition": {
                    "logic": "OR",  # Inner OR logic
                    "fields": [
                        {"table": "user_basic_info", "field": "user_level", "operator": "belongs_to", "value": ["VIP2", "VIP3"]},
                        {"table": "user_asset_summary", "field": "total_asset_value", "operator": ">=", "value": "100000"}
                    ]
                }
            },
            {
                "condition": {
                    "logic": "AND",  # Inner AND logic
                    "fields": [
                        {"table": "user_basic_info", "field": "kyc_status", "operator": "=", "value": "verified"},
                        {"table": "user_activity_summary", "field": "trade_count_30d", "operator": ">", "value": "5"}
                    ]
                }
            }
        ]
    })
    
    sql = parser.parseRuleToSql(rule_json, ["user_basic_info", "user_asset_summary", "user_activity_summary"])
    
    # Comprehensive SQL validation
    assert "`tag_system.user_basic_info`.`user_level` IN ('VIP2','VIP3')" in sql
    assert "`tag_system.user_asset_summary`.`total_asset_value` >= 100000" in sql
    assert " AND " in sql and " OR " in sql
```

#### **Edge Case Validation**
```python
def test_invalid_rule_handling(self):
    """Test system robustness with invalid inputs"""
    parser = TagRuleParser()
    
    # Empty rule handling
    assert parser.parseRuleToSql("", ["user_basic_info"]) == "1=0"
    
    # Invalid JSON handling
    assert parser.parseRuleToSql("invalid json", ["user_basic_info"]) == "1=0"
    
    # None rule handling
    assert parser.parseRuleToSql(None, ["user_basic_info"]) == "1=0"
```

### Testing Best Practices

#### **Test Design Principles**
- **Isolation**: Each test is independent with no shared state
- **Realistic**: Test data mirrors production scenarios  
- **Comprehensive**: Cover normal flows, edge cases, and error conditions
- **Performance**: Tests complete in ~13 seconds for 22 test cases
- **Maintainable**: Clear test names and structured assertions

#### **Continuous Integration**
```yaml
# GitHub Actions example
name: Test Suite
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.12
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: python -m pytest tests/ -v --cov=src/tag_engine
```

### Test-Driven Development Workflow

#### **Development Process**
1. **Write Test First**: Define expected behavior in test cases
2. **Run Test (Should Fail)**: Verify test correctly identifies missing functionality  
3. **Implement Feature**: Write minimal code to make test pass
4. **Refactor**: Improve code quality while maintaining test coverage
5. **Integration Test**: Run full test suite to ensure no regressions

#### **Example TDD Cycle**
```python
# 1. Write failing test
def test_new_operator_support(self):
    """Test new 'regex_match' operator"""
    rule_json = json.dumps({
        "logic": "AND",
        "conditions": [{
            "fields": [{
                "table": "user_basic_info",
                "field": "email", 
                "operator": "regex_match",
                "value": ".*@company\\.com$",
                "type": "string"
            }]
        }]
    })
    
    sql = parser.parseRuleToSql(rule_json, ["user_basic_info"])
    expected = "`user_basic_info`.`email` RLIKE '.*@company\\.com$'"
    assert expected in sql

# 2. Implement feature in TagRuleParser.py
def _parseFieldToSql(self, field, isSingleTable=False):
    # ... existing operators ...
    elif operator == "regex_match":
        return f"{fullField} RLIKE '{value}'"

# 3. Run test to verify implementation
python -m pytest tests/test_rule_parser.py::TestTagRuleParser::test_new_operator_support -v
```

## Troubleshooting

### Common Issues
1. **Test data generator not found**: Ensure `generate_test_data.py` is in working directory
2. **MySQL connection failure**: Check environment variable configuration and network connectivity
3. **Type conversion errors**: Check UDF function input type handling logic
4. **Tag merging exceptions**: Verify coordination between Spark native functions and UDFs

### Performance Tuning
- Adjust Executor configuration based on data volume
- Optimize tag grouping strategy to reduce JOIN operations
- Use Spark UI to monitor resource usage

## Important Notes

### Code Standards
- All business logic concentrated in `src/tag_engine/` directory
- Use standard Python package imports: `from src.tag_engine.engine.TagEngine import TagEngine`
- Follow modular design with clear component responsibilities

### Deployment Considerations
- `src/tag_engine/main.py` is the single source of truth, other main.py files are wrappers
- DolphinScheduler deployment depends on `generate_test_data.py` in deployment package
- MySQL configuration loaded via environment variables, supports multi-environment deployment

### Architecture Design Principles
- Type safety first, all UDFs have explicit type definitions
- Balance performance optimization with functionality completeness
- Support large-scale data processing and enterprise-level deployment requirements

---

**Let data drive business, let tags create value!**

Enterprise-level tag computing system based on PySpark DSL + UDF, empowering precision marketing and user insights.