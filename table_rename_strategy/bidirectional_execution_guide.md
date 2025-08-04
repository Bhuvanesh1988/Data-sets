# Bi-Directional Logical Replication Table Rename Guide

## Overview
This guide provides step-by-step instructions for safely renaming tables in a PostgreSQL bi-directional logical replication environment. The strategy includes batch processing, coordination between sites, and replication-aware triggers.

## 🔥 CRITICAL CONSIDERATIONS

⚠️ **REPLICATION CONFLICTS**: Bi-directional replication requires careful coordination to prevent data conflicts and replication loops.

⚠️ **BOTH SITES ACTIVE**: Unlike the simple strategy, both sites may have active writes during the process.

⚠️ **COORDINATION REQUIRED**: The process must be executed in a specific sequence between coordinator and partner sites.

---

## 📋 Prerequisites

### Infrastructure Requirements
- ✅ PostgreSQL with logical replication enabled
- ✅ Both sites have identical table structures
- ✅ Replication slots and subscriptions configured
- ✅ Sufficient storage (3x largest table size on each site)
- ✅ Network connectivity between sites
- ✅ Maintenance window (4-8 hours recommended)

### Access Requirements
- ✅ SUPERUSER privileges on both sites
- ✅ Ability to modify replication subscriptions
- ✅ Access to monitor replication lag
- ✅ Coordination mechanism between DBAs on both sites

---

## 🚀 EXECUTION WORKFLOW

### Phase 0: Setup (One-time per environment)

#### Install Batch Processing Framework
```sql
-- Run on BOTH sites
\i batch_execution_logic.sql
```

#### Install Bi-directional Replication Strategy
```sql
-- Run on BOTH sites  
\i bidirectional_replication_rename.sql
```

---

### Phase 1: Pre-Migration Coordination

#### Step 1.1: Coordinate Maintenance Window
```bash
# Coordinate between DBA teams
# - Schedule identical maintenance window on both sites
# - Ensure communication channels are open
# - Have rollback procedures ready
```

#### Step 1.2: Backup Both Sites
```sql
-- On Primary Site
pg_dump -h primary_host -U postgres -d database_name > primary_pre_migration_backup.sql

-- On Secondary Site  
pg_dump -h secondary_host -U postgres -d database_name > secondary_pre_migration_backup.sql
```

#### Step 1.3: Check Replication Health
```sql
-- On Primary Site - Check replication slots
SELECT slot_name, active, confirmed_flush_lsn, wal_status 
FROM pg_replication_slots;

-- On Secondary Site - Check subscriptions
SELECT subname, subenabled, subconninfo, subslotname 
FROM pg_subscription;

-- Check replication lag on both sites
SELECT 
    client_addr,
    state,
    pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) AS flush_lag_bytes
FROM pg_stat_replication;
```

---

### Phase 2: Coordinated Execution

#### Step 2.1: Initialize on Primary Site (Coordinator)

**Customize these parameters first:**
```sql
-- Update table and column names for YOUR specific table
-- In the script, replace:
-- 'data_column1', 'data_column2', 'business_date' 
-- with your actual column names
```

**Execute on Primary Site:**
```sql
-- Start coordinated rename operation
SELECT execute_coordinated_table_rename(
    'primary',                    -- site_name
    'your_table_name',           -- original_table (CHANGE THIS)
    'your_new_table_name',       -- new_table (CHANGE THIS)  
    'your_archive_table_primary', -- archive_table (CHANGE THIS)
    TRUE,                        -- is_coordinator
    FALSE                        -- wait_for_partner (set to FALSE for immediate execution)
);

-- Note the returned operation_id UUID for monitoring
```

#### Step 2.2: Execute on Secondary Site (Partner)

**Wait for Primary to complete preparation, then execute on Secondary:**
```sql
-- Execute on secondary with same operation_id if needed for coordination
SELECT execute_coordinated_table_rename(
    'secondary',                   -- site_name
    'your_table_name',            -- original_table (SAME as primary)
    'your_new_table_name',        -- new_table (SAME as primary)
    'your_archive_table_secondary', -- archive_table (DIFFERENT suffix)
    FALSE,                        -- is_coordinator
    FALSE                         -- wait_for_partner
);
```

#### Step 2.3: Monitor Progress on Both Sites

```sql
-- Check coordination status
SELECT * FROM check_coordination_status('your-operation-uuid');

-- Monitor batch job progress
SELECT * FROM batch_jobs_status WHERE operation LIKE '%COORDINATED%';

-- Check individual batch progress
SELECT * FROM get_batch_progress(job_id);

-- Monitor replication lag during process
SELECT 
    application_name,
    state,
    pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) AS lag_bytes,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_lag_bytes
FROM pg_stat_replication;
```

---

### Phase 3: Validation and Testing

#### Step 3.1: Data Integrity Validation

**Run on BOTH sites:**
```sql
-- Validate row counts
SELECT 
    'Original Table' as table_type, COUNT(*) as row_count 
FROM your_table_name
UNION ALL
SELECT 
    'New Table' as table_type, COUNT(*) as row_count 
FROM your_new_table_name
UNION ALL
SELECT 
    'Archive Table' as table_type, COUNT(*) as row_count 
FROM your_archive_table_[primary|secondary];

-- Check that total rows are preserved
SELECT 
    (SELECT COUNT(*) FROM your_new_table_name) + 
    (SELECT COUNT(*) FROM your_archive_table_[primary|secondary]) as total_after_migration,
    (SELECT COUNT(*) FROM your_table_name) as original_count;
```

#### Step 3.2: Test Replication Sync

**Test on Primary Site:**
```sql
-- Insert test record
INSERT INTO your_table_name (column1, column2, created_date) 
VALUES ('test_primary', 123, NOW());

-- Check it appears in new table locally
SELECT COUNT(*) FROM your_new_table_name 
WHERE replication_source = 'primary' AND data_column1 = 'test_primary';
```

**Verify on Secondary Site:**
```sql
-- Should see the test record replicated
SELECT COUNT(*) FROM your_new_table_name 
WHERE replication_source = 'primary' AND data_column1 = 'test_primary';
```

**Test reverse direction - on Secondary Site:**
```sql
-- Insert test record
INSERT INTO your_table_name (column1, column2, created_date) 
VALUES ('test_secondary', 456, NOW());

-- Check it appears in new table locally
SELECT COUNT(*) FROM your_new_table_name 
WHERE replication_source = 'secondary' AND data_column1 = 'test_secondary';
```

**Verify on Primary Site:**
```sql
-- Should see the test record replicated
SELECT COUNT(*) FROM your_new_table_name 
WHERE replication_source = 'secondary' AND data_column1 = 'test_secondary';
```

#### Step 3.3: Performance Testing

```sql
-- Test query performance on new tables
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM your_new_table_name 
WHERE created_at > NOW() - INTERVAL '1 day';

-- Check trigger overhead
SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del 
FROM pg_stat_user_tables 
WHERE tablename IN ('your_table_name', 'your_new_table_name');
```

---

### Phase 4: Application Cutover (Optional)

#### Step 4.1: Create Unified View (if needed)
```sql
-- Create view that combines both tables for gradual migration
CREATE OR REPLACE VIEW your_table_unified_view AS
SELECT 
    COALESCE(n.id, o.id) as id,
    COALESCE(n.data_column1, o.data_column1) as data_column1,
    COALESCE(n.data_column2, o.data_column2) as data_column2,
    COALESCE(n.business_date, o.business_date) as business_date,
    COALESCE(n.created_at, o.created_date) as created_date,
    CASE 
        WHEN n.id IS NOT NULL THEN 'NEW_TABLE'
        ELSE 'ORIGINAL_TABLE'
    END as source_table
FROM your_new_table_name n
FULL OUTER JOIN your_table_name o ON n.original_id = o.id;
```

---

## 🔍 MONITORING AND TROUBLESHOOTING

### Continuous Monitoring Queries

```sql
-- Replication lag monitoring (run every 5 minutes)
SELECT 
    NOW() as check_time,
    application_name,
    client_addr,
    state,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)) AS flush_lag,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)) AS replay_lag
FROM pg_stat_replication;

-- Batch job monitoring
SELECT 
    job_name,
    table_name,
    status,
    progress_percentage,
    current_batch,
    processed_rows,
    total_rows
FROM batch_jobs_status 
WHERE status IN ('RUNNING', 'FAILED')
ORDER BY job_id DESC;

-- Coordination status across operations
SELECT 
    operation_id,
    site_name,
    table_name,
    operation_status,
    phase,
    notes
FROM replication_rename_coordination 
WHERE operation_status NOT IN ('COMPLETED')
ORDER BY initiated_at DESC;
```

### Common Issues and Solutions

#### Issue: Replication Lag Too High
```sql
-- Check and fix replication lag
SELECT slot_name, confirmed_flush_lsn, wal_status FROM pg_replication_slots;

-- If lag is excessive, consider:
-- 1. Pausing batch jobs temporarily
SELECT pause_batch_job(job_id) FROM batch_job_control WHERE status = 'RUNNING';

-- 2. Adjusting batch sizes
UPDATE batch_job_control SET batch_size = 1000 WHERE job_id = your_job_id;

-- 3. Resume when lag reduces
SELECT resume_batch_job(job_id) FROM batch_job_control WHERE status = 'PAUSED';
```

#### Issue: Coordination Stuck
```sql
-- Check stuck operations
SELECT * FROM replication_rename_coordination 
WHERE operation_status IN ('PREPARING', 'EXECUTING') 
AND initiated_at < NOW() - INTERVAL '1 hour';

-- Manual intervention may be required
-- Contact the other site's DBA team
```

#### Issue: Batch Job Failed
```sql
-- Check failed batches
SELECT * FROM batch_execution_log 
WHERE status = 'FAILED' 
ORDER BY timestamp DESC;

-- Retry failed job
UPDATE batch_job_control SET status = 'PENDING', error_message = NULL 
WHERE job_id = failed_job_id;
```

---

## 🛡️ ROLLBACK PROCEDURES

### Immediate Rollback (if issues detected within first hour)

#### Step 1: Stop All Operations
```sql
-- On both sites: Pause running batch jobs
SELECT pause_batch_job(job_id) FROM batch_job_control WHERE status = 'RUNNING';

-- Disable triggers
DROP TRIGGER IF EXISTS trig_your_table_name_sync_primary ON your_table_name;
DROP TRIGGER IF EXISTS trig_your_table_name_sync_secondary ON your_table_name;
```

#### Step 2: Restore Replication State
```sql
-- Re-enable subscriptions if they were disabled
ALTER SUBSCRIPTION your_subscription_name ENABLE;

-- Check replication is working
SELECT * FROM pg_stat_subscription;
```

#### Step 3: Validate and Clean Up
```sql
-- Drop new tables if safe to do so
DROP TABLE IF EXISTS your_new_table_name;
-- Keep archive tables for investigation

-- Validate original table is intact
SELECT COUNT(*) FROM your_table_name;
```

### Full Rollback (complete restoration)

#### Option 1: Point-in-time recovery
```bash
# Restore from backup taken before migration
pg_restore -h host -U postgres -d database pre_migration_backup.sql
```

#### Option 2: Data restoration from archive
```sql
-- Restore archived data back to original table
INSERT INTO your_table_name 
SELECT * FROM your_archive_table_[site] 
WHERE archived_at > 'migration_start_time';
```

---

## ✅ SUCCESS CRITERIA CHECKLIST

- [ ] **Data Integrity**: All row counts match (original = new + archive)
- [ ] **Bi-directional Sync**: Test records sync both ways between sites
- [ ] **Performance**: Query performance maintained or improved
- [ ] **Replication Health**: Lag within acceptable limits (<1MB)
- [ ] **Coordination**: Both sites show 'COMPLETED' status
- [ ] **Monitoring**: All batch jobs completed successfully
- [ ] **Application**: Services working normally with new structure

---

## 📞 Emergency Contacts Template

| Role | Primary Site | Secondary Site |
|------|-------------|----------------|
| Lead DBA | [Name] [Phone] | [Name] [Phone] |
| Replication Specialist | [Name] [Phone] | [Name] [Phone] |
| Application Team | [Name] [Phone] | [Name] [Phone] |
| Operations Center | [24/7 Number] | [24/7 Number] |

---

## 📚 Post-Migration Tasks

### First 24 Hours
- [ ] Monitor replication lag continuously
- [ ] Verify trigger performance impact
- [ ] Check application error rates
- [ ] Validate data consistency between sites

### First Week  
- [ ] Daily data integrity checks
- [ ] Performance tuning if needed
- [ ] Application team feedback
- [ ] Documentation updates

### First Month
- [ ] Plan original table cleanup (after business approval)
- [ ] Archive old coordination logs
- [ ] Update monitoring dashboards
- [ ] Conduct post-mortem and lessons learned