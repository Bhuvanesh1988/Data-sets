# Simplified Table Rename Strategy - 3 Steps Only

## Overview
Streamlined version for specific known tables with minimal monitoring overhead.
**Reduces complexity from 12+ steps to just 3 simple steps.**

## Prerequisites
- ✅ You know the exact table names to migrate
- ✅ No additional monitoring infrastructure needed
- ✅ Basic backup in place

---

## 🚀 STEP 1: One-Time Setup (5 minutes)
```sql
-- Run this once on your database
\i simple_table_rename.sql
```
This creates:
- Simple logging table (optional)
- Utility function to determine data cutoff

---

## 🚀 STEP 2: Execute Migration Per Table (10-30 minutes each)

### Before running:
1. **Update these variables** in the script:
   ```sql
   table_name TEXT := 'your_actual_table_name';
   new_table TEXT := 'your_new_table_name';    
   archive_table TEXT := 'your_archive_table_name';
   ```

2. **Update column mappings** to match your table structure:
   ```sql
   -- In the CREATE TABLE section, replace with your actual columns:
   data_column1 VARCHAR(255),  -- Your actual column name/type
   data_column2 INTEGER,       -- Your actual column name/type
   business_date DATE,         -- Your actual column name/type
   ```

3. **Update date column** if different from 'created_date':
   ```sql
   cutoff_date := get_cutoff_date(table_name, 'your_date_column');
   ```

### Run the migration:
```sql
-- Execute STEP 2 section from simple_table_rename.sql
-- This will:
-- ✅ Archive 90% of old data
-- ✅ Create new optimized table
-- ✅ Migrate 10% recent data
```

---

## 🚀 STEP 3: Setup Live Sync (5 minutes)

### Update trigger function with your table/column names:
```sql
-- In sync_to_new_table() function, update:
INSERT INTO your_actual_new_table (original_id, actual_col1, actual_col2, ...)
VALUES (NEW.id, NEW.actual_col1, NEW.actual_col2, ...);

-- And update trigger creation:
CREATE TRIGGER sync_original_to_new_trigger
    AFTER INSERT OR UPDATE OR DELETE ON your_actual_table_name
    FOR EACH ROW EXECUTE FUNCTION sync_to_new_table();
```

---

## ✅ Validation (2 minutes)
```sql
-- Quick check - row counts should add up
SELECT 
    'Original' as table_type, COUNT(*) FROM your_table_name
UNION ALL
SELECT 
    'New' as table_type, COUNT(*) FROM your_new_table  
UNION ALL
SELECT 
    'Archive' as table_type, COUNT(*) FROM your_archive_table;

-- Test the trigger
INSERT INTO your_table_name (col1, col2) VALUES ('test', 123);
-- Should appear in your_new_table automatically
```

---

## 🎯 What This Eliminates From Original Strategy

| **Removed Complexity** | **Simplified To** |
|------------------------|-------------------|
| 12+ execution steps | 3 simple steps |
| Complex schema setup | 1 basic log table |
| Multiple monitoring tables | Optional basic logging |
| Batch processing logic | Direct migration |
| Replication control framework | Simple trigger sync |
| Health check infrastructure | Basic validation queries |
| Complex rollback procedures | Standard backup restore |
| Multi-phase execution plan | Single execution per table |

## 📋 Summary Checklist

- [ ] **Step 1**: Run setup script once
- [ ] **Step 2**: Update variables & execute migration per table  
- [ ] **Step 3**: Setup sync trigger per table
- [ ] **Validate**: Check row counts and test trigger
- [ ] **Cleanup**: Drop or rename original tables (optional)

## ⚡ Time Savings
- **Original**: 2-3 days planning + 4-8 hours execution per table
- **Simplified**: 30 minutes setup + 15-45 minutes per table

## 🛡️ Safety Notes
- Always backup before migration
- Test on non-production first
- Keep original tables until validation complete
- Monitor for 24-48 hours after migration