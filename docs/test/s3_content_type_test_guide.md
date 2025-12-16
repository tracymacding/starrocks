# S3 Export ContentType 功能测试指南

## 概述

本文档描述了 StarRocks 导出文件到 S3 时设置正确 ContentType 元数据功能的测试方法。

### 背景

在修复前，导出到 S3 的文件（CSV、Parquet、ORC 等）的 ContentType 元数据默认被设置为 `application/xml`，这会导致一些工具无法正确识别文件类型。

### 修复后的 ContentType 映射

| 文件格式 | ContentType |
|---------|-------------|
| CSV | `text/csv` |
| Parquet | `application/parquet` |
| ORC | `application/x-orc` |
| 其他/默认 | `application/octet-stream` |

---

## 测试环境准备

### 1. S3 配置信息

请根据实际环境修改以下配置：

```bash
# S3 配置
export S3_BUCKET="your-bucket"
export S3_ENDPOINT="your-endpoint"
export S3_ACCESS_KEY="your-access-key"
export S3_SECRET_KEY="your-secret-key"
export S3_REGION="your-region"
```

### 2. 创建测试表

```sql
-- 创建测试数据库
CREATE DATABASE IF NOT EXISTS test_s3_content_type;
USE test_s3_content_type;

-- 创建测试表
CREATE TABLE test_export (
    id INT,
    name VARCHAR(100),
    amount DECIMAL(10, 2),
    created_at DATETIME
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 插入测试数据
INSERT INTO test_export VALUES
(1, 'Alice', 100.50, '2024-01-01 10:00:00'),
(2, 'Bob', 200.75, '2024-01-02 11:00:00'),
(3, 'Charlie', 300.25, '2024-01-03 12:00:00'),
(4, 'David', 400.00, '2024-01-04 13:00:00'),
(5, 'Eve', 500.50, '2024-01-05 14:00:00');
```

---

## 测试用例

### 测试用例 1: CSV 格式导出

**目的**: 验证 CSV 导出文件的 ContentType 为 `text/csv`

**执行 SQL**:

```sql
INSERT INTO FILES (
    "path" = "s3://${S3_BUCKET}/test_content_type/csv/",
    "format" = "csv",
    "aws.s3.access_key" = "${S3_ACCESS_KEY}",
    "aws.s3.secret_key" = "${S3_SECRET_KEY}",
    "aws.s3.endpoint" = "${S3_ENDPOINT}",
    "aws.s3.region" = "${S3_REGION}"
)
SELECT * FROM test_export;
```

**验证命令**:

```bash
# 列出导出的文件
aws s3 ls s3://${S3_BUCKET}/test_content_type/csv/ --endpoint-url ${S3_ENDPOINT}

# 检查文件 ContentType (替换 xxx.csv 为实际文件名)
aws s3api head-object \
    --bucket ${S3_BUCKET} \
    --key test_content_type/csv/xxx.csv \
    --endpoint-url ${S3_ENDPOINT} \
    --query 'ContentType' \
    --output text
```

**预期结果**: `text/csv`

---

### 测试用例 2: Parquet 格式导出

**目的**: 验证 Parquet 导出文件的 ContentType 为 `application/parquet`

**执行 SQL**:

```sql
INSERT INTO FILES (
    "path" = "s3://${S3_BUCKET}/test_content_type/parquet/",
    "format" = "parquet",
    "aws.s3.access_key" = "${S3_ACCESS_KEY}",
    "aws.s3.secret_key" = "${S3_SECRET_KEY}",
    "aws.s3.endpoint" = "${S3_ENDPOINT}",
    "aws.s3.region" = "${S3_REGION}"
)
SELECT * FROM test_export;
```

**验证命令**:

```bash
# 列出导出的文件
aws s3 ls s3://${S3_BUCKET}/test_content_type/parquet/ --endpoint-url ${S3_ENDPOINT}

# 检查文件 ContentType
aws s3api head-object \
    --bucket ${S3_BUCKET} \
    --key test_content_type/parquet/xxx.parquet \
    --endpoint-url ${S3_ENDPOINT} \
    --query 'ContentType' \
    --output text
```

**预期结果**: `application/parquet`

---

### 测试用例 3: ORC 格式导出

**目的**: 验证 ORC 导出文件的 ContentType 为 `application/x-orc`

**执行 SQL**:

```sql
INSERT INTO FILES (
    "path" = "s3://${S3_BUCKET}/test_content_type/orc/",
    "format" = "orc",
    "aws.s3.access_key" = "${S3_ACCESS_KEY}",
    "aws.s3.secret_key" = "${S3_SECRET_KEY}",
    "aws.s3.endpoint" = "${S3_ENDPOINT}",
    "aws.s3.region" = "${S3_REGION}"
)
SELECT * FROM test_export;
```

**验证命令**:

```bash
# 列出导出的文件
aws s3 ls s3://${S3_BUCKET}/test_content_type/orc/ --endpoint-url ${S3_ENDPOINT}

# 检查文件 ContentType
aws s3api head-object \
    --bucket ${S3_BUCKET} \
    --key test_content_type/orc/xxx.orc \
    --endpoint-url ${S3_ENDPOINT} \
    --query 'ContentType' \
    --output text
```

**预期结果**: `application/x-orc`

---

### 测试用例 4: 大文件多部分上传

**目的**: 验证大文件触发 multipart upload 时 ContentType 仍然正确设置

**创建大数据量表**:

```sql
-- 方法 1: 使用 generate_series (如果支持)
CREATE TABLE test_large_export AS
SELECT
    number as id,
    concat('name_', cast(number as string)) as name,
    number * 1.5 as amount,
    now() as created_at
FROM TABLE(generate_series(1, 1000000));

-- 方法 2: 多次插入扩展数据
INSERT INTO test_export SELECT * FROM test_export;
-- 重复执行直到数据量足够大
```

**执行 SQL**:

```sql
INSERT INTO FILES (
    "path" = "s3://${S3_BUCKET}/test_content_type/large_csv/",
    "format" = "csv",
    "aws.s3.access_key" = "${S3_ACCESS_KEY}",
    "aws.s3.secret_key" = "${S3_SECRET_KEY}",
    "aws.s3.endpoint" = "${S3_ENDPOINT}",
    "aws.s3.region" = "${S3_REGION}"
)
SELECT * FROM test_large_export;
```

**验证命令**:

```bash
aws s3api head-object \
    --bucket ${S3_BUCKET} \
    --key test_content_type/large_csv/xxx.csv \
    --endpoint-url ${S3_ENDPOINT} \
    --query 'ContentType' \
    --output text
```

**预期结果**: `text/csv`

---

### 测试用例 5: 压缩 CSV 导出

**目的**: 验证压缩 CSV 文件的 ContentType 是否正确

**执行 SQL**:

```sql
INSERT INTO FILES (
    "path" = "s3://${S3_BUCKET}/test_content_type/csv_gzip/",
    "format" = "csv",
    "compression" = "gzip",
    "aws.s3.access_key" = "${S3_ACCESS_KEY}",
    "aws.s3.secret_key" = "${S3_SECRET_KEY}",
    "aws.s3.endpoint" = "${S3_ENDPOINT}",
    "aws.s3.region" = "${S3_REGION}"
)
SELECT * FROM test_export;
```

**验证命令**:

```bash
aws s3api head-object \
    --bucket ${S3_BUCKET} \
    --key test_content_type/csv_gzip/xxx.csv.gz \
    --endpoint-url ${S3_ENDPOINT} \
    --query 'ContentType' \
    --output text
```

**预期结果**: `text/csv`

---

### 测试用例 6: 带表头的 CSV 导出

**目的**: 验证带表头的 CSV 导出 ContentType 是否正确

**执行 SQL**:

```sql
INSERT INTO FILES (
    "path" = "s3://${S3_BUCKET}/test_content_type/csv_header/",
    "format" = "csv",
    "csv.column_separator" = ",",
    "csv.line_delimiter" = "\n",
    "csv.with_header" = "true",
    "aws.s3.access_key" = "${S3_ACCESS_KEY}",
    "aws.s3.secret_key" = "${S3_SECRET_KEY}",
    "aws.s3.endpoint" = "${S3_ENDPOINT}",
    "aws.s3.region" = "${S3_REGION}"
)
SELECT * FROM test_export;
```

**验证命令**:

```bash
aws s3api head-object \
    --bucket ${S3_BUCKET} \
    --key test_content_type/csv_header/xxx.csv \
    --endpoint-url ${S3_ENDPOINT} \
    --query 'ContentType' \
    --output text
```

**预期结果**: `text/csv`

---

## 批量验证脚本

将以下脚本保存为 `verify_content_type.sh`：

```bash
#!/bin/bash

# 配置
BUCKET="${S3_BUCKET:-your-bucket}"
ENDPOINT="${S3_ENDPOINT:-your-endpoint}"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# 定义预期的 ContentType
declare -A expected_types
expected_types["csv"]="text/csv"
expected_types["parquet"]="application/parquet"
expected_types["orc"]="application/x-orc"

# 统计
total=0
passed=0
failed=0

# 检查函数
check_content_type() {
    local prefix=$1
    local expected=$2
    local format=$3

    echo ""
    echo "=========================================="
    echo "Checking $format files in s3://$BUCKET/$prefix"
    echo "Expected ContentType: $expected"
    echo "=========================================="

    # 列出目录下的文件
    files=$(aws s3 ls "s3://$BUCKET/$prefix" --endpoint-url "$ENDPOINT" 2>/dev/null | awk '{print $4}')

    if [ -z "$files" ]; then
        echo "  No files found in this directory"
        return
    fi

    for file in $files; do
        if [ -n "$file" ]; then
            ((total++))
            actual=$(aws s3api head-object \
                --bucket "$BUCKET" \
                --key "${prefix}${file}" \
                --endpoint-url "$ENDPOINT" \
                --query 'ContentType' \
                --output text 2>/dev/null)

            if [ "$actual" == "$expected" ]; then
                echo -e "  ${GREEN}✓${NC} $file: ContentType=$actual"
                ((passed++))
            else
                echo -e "  ${RED}✗${NC} $file: ContentType=$actual (expected: $expected)"
                ((failed++))
            fi
        fi
    done
}

echo "S3 Export ContentType Verification"
echo "==================================="
echo "Bucket: $BUCKET"
echo "Endpoint: $ENDPOINT"

# 执行检查
check_content_type "test_content_type/csv/" "${expected_types[csv]}" "CSV"
check_content_type "test_content_type/parquet/" "${expected_types[parquet]}" "Parquet"
check_content_type "test_content_type/orc/" "${expected_types[orc]}" "ORC"
check_content_type "test_content_type/large_csv/" "${expected_types[csv]}" "Large CSV"
check_content_type "test_content_type/csv_gzip/" "${expected_types[csv]}" "Compressed CSV"
check_content_type "test_content_type/csv_header/" "${expected_types[csv]}" "CSV with Header"

# 输出统计
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "Total files checked: $total"
echo -e "Passed: ${GREEN}$passed${NC}"
echo -e "Failed: ${RED}$failed${NC}"

if [ $failed -eq 0 ] && [ $total -gt 0 ]; then
    echo -e "\n${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "\n${RED}Some tests failed!${NC}"
    exit 1
fi
```

**使用方法**:

```bash
chmod +x verify_content_type.sh
./verify_content_type.sh
```

---

## 测试结果汇总表

| 测试用例 | 文件格式 | 上传方式 | 预期 ContentType | 实际结果 |
|---------|---------|---------|-----------------|---------|
| 用例 1 | CSV | Singlepart | `text/csv` | |
| 用例 2 | Parquet | Singlepart | `application/parquet` | |
| 用例 3 | ORC | Singlepart | `application/x-orc` | |
| 用例 4 | CSV (大文件) | Multipart | `text/csv` | |
| 用例 5 | CSV + GZIP | Singlepart | `text/csv` | |
| 用例 6 | CSV + Header | Singlepart | `text/csv` | |

---

## 清理测试数据

### 清理 StarRocks 数据

```sql
DROP DATABASE IF EXISTS test_s3_content_type;
```

### 清理 S3 数据

```bash
aws s3 rm s3://${S3_BUCKET}/test_content_type/ --recursive --endpoint-url ${S3_ENDPOINT}
```

---

## 问题排查

### 常见问题

1. **ContentType 仍然是 `application/xml`**
   - 确认使用的是修复后的 StarRocks 版本
   - 检查是否使用了正确的导出方式 (INSERT INTO FILES)

2. **无法连接 S3**
   - 检查 S3 配置参数是否正确
   - 确认网络连接正常
   - 验证 Access Key 和 Secret Key 是否有效

3. **文件未生成**
   - 检查 SQL 执行是否成功
   - 查看 StarRocks BE 日志是否有错误信息

### 日志位置

- FE 日志: `fe/log/fe.log`
- BE 日志: `be/log/be.INFO`

---

## 相关代码修改

本功能涉及以下代码文件的修改：

| 文件 | 修改内容 |
|------|----------|
| `be/src/fs/fs.h` | 添加 `content_type` 字段到 `WritableFileOptions` |
| `be/src/io/s3_output_stream.cpp` | 在 S3 上传请求中设置 ContentType |
| `be/src/io/direct_s3_output_stream.cpp` | 在 S3 上传请求中设置 ContentType |
| `be/src/fs/fs_s3.cpp` | 传递 content_type 到输出流 |
| `be/src/formats/csv/csv_file_writer.cpp` | 设置 CSV 的 ContentType |
| `be/src/formats/parquet/parquet_file_writer.cpp` | 设置 Parquet 的 ContentType |
| `be/src/formats/orc/orc_file_writer.cpp` | 设置 ORC 的 ContentType |
