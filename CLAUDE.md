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
    └── TagUdfs.py              # PySpark UDF function collection

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

### 3. TagUdfs.py - Type-Safe UDF Functions
**Responsibilities**: Provide type-safe tag processing UDFs
- `mergeUserTags()`: Merge tags for single user, supports multiple input types
- `mergeWithExistingTags()`: Smart merging of new and existing tags
- `arrayToJson()` / `jsonToArray()`: Array and JSON mutual conversion

**Type Safety Features**:
```python
@udf(returnType=ArrayType(IntegerType()))
def mergeUserTags(tagList):
    """Supports List[int], Array[int], nested arrays and other input types"""
    if not tagList:
        return []
    
    # Handle different input types and nested arrays
    if isinstance(tagList, list):
        flatTags = tagList
    else:
        flatTags = []
        for item in tagList:
            if isinstance(item, (list, tuple)):
                flatTags.extend(item)
            else:
                flatTags.append(item)
    
    # Filter None values, deduplicate and sort
    validTags = [tag for tag in flatTags if tag is not None]
    uniqueTags = list(set(validTags))
    uniqueTags.sort()
    return uniqueTags
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
Add new UDF functions in `TagUdfs.py`, ensure:
- Type safety and None value handling
- Support for multiple input types
- Clear return type definition

### Test Data Generation
- DolphinScheduler environment: Use `dolphin_gui_deploy/generate_test_data.py`
- Local development environment: Use `environments/local/test_data_generator.py`

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