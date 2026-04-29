#!/bin/bash
# Test script: Validate Databricks Asset Bundle

set -e

echo "=========================================="
echo "Testing: Bundle Validation"
echo "=========================================="

cd "$(dirname "$0")/.."

echo ""
echo "1. Checking bundle structure..."
if [ ! -f "databricks.yml" ]; then
    echo "FAIL: databricks.yml not found"
    exit 1
fi
echo "   OK: databricks.yml exists"

if [ ! -d "resources" ]; then
    echo "FAIL: resources/ directory not found"
    exit 1
fi
echo "   OK: resources/ directory exists"

if [ ! -d "src" ]; then
    echo "FAIL: src/ directory not found"
    exit 1
fi
echo "   OK: src/ directory exists"

echo ""
echo "2. Checking resource files..."
for resource in pipeline.yml app.yml jobs.yml; do
    if [ -f "resources/$resource" ]; then
        echo "   OK: resources/$resource exists"
    else
        echo "   WARN: resources/$resource not found"
    fi
done

echo ""
echo "3. Checking pipeline transformations..."
sql_count=$(find src/pipelines -name "*.sql" 2>/dev/null | wc -l)
echo "   Found $sql_count SQL transformation files"

if [ "$sql_count" -lt 10 ]; then
    echo "   WARN: Expected at least 10 SQL files"
fi

echo ""
echo "4. Checking app files..."
for appfile in app.py app.yaml requirements.txt; do
    if [ -f "src/app/$appfile" ]; then
        echo "   OK: src/app/$appfile exists"
    else
        echo "   FAIL: src/app/$appfile not found"
        exit 1
    fi
done

echo ""
echo "5. Running databricks bundle validate..."
if command -v databricks &> /dev/null; then
    databricks bundle validate
    echo "   OK: Bundle validation passed"
else
    echo "   SKIP: Databricks CLI not installed"
    echo "   Install with: pip install databricks-cli"
fi

echo ""
echo "=========================================="
echo "Bundle validation complete!"
echo "=========================================="
