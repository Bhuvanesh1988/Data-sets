# Table Rename Strategy for Bi-Directional Replication

## Overview
This strategy outlines a comprehensive approach to recreate existing tables to fetch only new data through triggers while maintaining historical data integrity in a bi-directional replication environment where only one site is active.

## Prerequisites
- Bi-directional replication setup with one active site
- Database supports DDL replication (or manual DDL sync capability)
- Sufficient storage for temporary tables during transition
- Maintenance window availability
- Backup and recovery procedures in place

## Strategy Components

### 1. Pre-Migration Assessment

#### 1.1 Identify Tables for Rename
- Tables with >90% historical data that won't be needed
- Tables with predictable new data patterns
- Tables that can benefit from trigger-based incremental loading

#### 1.2 Dependency Analysis
```sql
-- Query to find table dependencies
SELECT 
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
LEFT JOIN information_schema.constraint_column_usage ccu 
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.table_name = 'your_table_name'
ORDER BY tc.table_name, tc.constraint_type;
```

#### 1.3 Data Volume Analysis
```sql
-- Analyze data distribution by time periods
SELECT 
    DATE_TRUNC('month', created_date) as month,
    COUNT(*) as record_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) as percentage
FROM your_table_name
GROUP BY DATE_TRUNC('month', created_date)
ORDER BY month DESC;
```

### 2. Design Phase

#### 2.1 New Table Structure
```sql
-- Example new table with optimized structure
CREATE TABLE new_table_name (
    id BIGSERIAL PRIMARY KEY,
    original_id BIGINT, -- Reference to original data if needed
    data_column1 VARCHAR(255),
    data_column2 INTEGER,
    business_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_migrated BOOLEAN DEFAULT FALSE,
    source_system VARCHAR(50) DEFAULT 'NEW_SYSTEM'
);

-- Add indexes for performance
CREATE INDEX idx_new_table_business_date ON new_table_name(business_date);
CREATE INDEX idx_new_table_created_at ON new_table_name(created_at);
CREATE INDEX idx_new_table_original_id ON new_table_name(original_id);
```

#### 2.2 Archive Table for Historical Data
```sql
-- Archive table for historical data
CREATE TABLE archive_table_name (
    LIKE original_table_name INCLUDING ALL
);

-- Add archive metadata
ALTER TABLE archive_table_name 
ADD COLUMN archived_at TIMESTAMP DEFAULT NOW(),
ADD COLUMN archive_reason VARCHAR(100) DEFAULT 'TABLE_RENAME_STRATEGY';
```

### 3. Implementation Steps

#### Phase 1: Preparation (Active Site)

##### Step 1: Create Logging Infrastructure
```sql
-- Create audit/log table for tracking the migration
CREATE TABLE table_rename_audit (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    operation VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    rows_affected BIGINT,
    error_message TEXT,
    notes TEXT
);
```

##### Step 2: Create Replication Control
```sql
-- Table to control replication behavior during migration
CREATE TABLE replication_control (
    table_name VARCHAR(255) PRIMARY KEY,
    replication_enabled BOOLEAN DEFAULT TRUE,
    last_sync_time TIMESTAMP,
    notes TEXT
);

-- Disable replication for the target table during migration
INSERT INTO replication_control (table_name, replication_enabled, notes)
VALUES ('original_table_name', FALSE, 'Disabled for table rename strategy');
```

##### Step 3: Create Data Cutoff Determination
```sql
-- Function to determine data cutoff point (e.g., last 10% of data)
CREATE OR REPLACE FUNCTION get_data_cutoff_date(table_name TEXT, percentage NUMERIC DEFAULT 0.1)
RETURNS TIMESTAMP AS $$
DECLARE
    cutoff_date TIMESTAMP;
    total_rows BIGINT;
    target_rows BIGINT;
BEGIN
    -- Get total row count
    EXECUTE format('SELECT COUNT(*) FROM %I', table_name) INTO total_rows;
    
    -- Calculate target rows (last X%)
    target_rows := FLOOR(total_rows * percentage);
    
    -- Get cutoff date
    EXECUTE format(
        'SELECT created_date FROM %I ORDER BY created_date DESC LIMIT 1 OFFSET %s',
        table_name, target_rows
    ) INTO cutoff_date;
    
    RETURN cutoff_date;
END;
$$ LANGUAGE plpgsql;
```

#### Phase 2: Data Migration Execution

##### Step 4: Archive Historical Data
```sql
-- Log the start of archival process
INSERT INTO table_rename_audit (table_name, operation, start_time, status)
VALUES ('original_table_name', 'ARCHIVE_HISTORICAL', NOW(), 'STARTED');

-- Archive historical data (>90% old data)
DO $$
DECLARE
    cutoff_date TIMESTAMP;
    rows_archived BIGINT;
BEGIN
    -- Get cutoff date for recent data (last 10%)
    SELECT get_data_cutoff_date('original_table_name', 0.1) INTO cutoff_date;
    
    -- Move historical data to archive
    INSERT INTO archive_table_name
    SELECT *, NOW(), 'PRE_RENAME_ARCHIVE'
    FROM original_table_name
    WHERE created_date < cutoff_date;
    
    GET DIAGNOSTICS rows_archived = ROW_COUNT;
    
    -- Update audit log
    UPDATE table_rename_audit 
    SET end_time = NOW(), 
        status = 'COMPLETED', 
        rows_affected = rows_archived,
        notes = format('Archived data before %s', cutoff_date)
    WHERE table_name = 'original_table_name' 
    AND operation = 'ARCHIVE_HISTORICAL' 
    AND status = 'STARTED';
    
    RAISE NOTICE 'Archived % rows with cutoff date %', rows_archived, cutoff_date;
END $$;
```

##### Step 5: Create New Table with Recent Data
```sql
-- Log the start of new table creation
INSERT INTO table_rename_audit (table_name, operation, start_time, status)
VALUES ('new_table_name', 'CREATE_WITH_RECENT_DATA', NOW(), 'STARTED');

DO $$
DECLARE
    cutoff_date TIMESTAMP;
    rows_migrated BIGINT;
BEGIN
    -- Get the same cutoff date
    SELECT get_data_cutoff_date('original_table_name', 0.1) INTO cutoff_date;
    
    -- Insert recent data into new table
    INSERT INTO new_table_name (original_id, data_column1, data_column2, business_date, created_at, is_migrated)
    SELECT 
        id,
        data_column1,
        data_column2,
        business_date,
        created_date,
        TRUE
    FROM original_table_name
    WHERE created_date >= cutoff_date;
    
    GET DIAGNOSTICS rows_migrated = ROW_COUNT;
    
    -- Update audit log
    UPDATE table_rename_audit 
    SET end_time = NOW(), 
        status = 'COMPLETED', 
        rows_affected = rows_migrated,
        notes = format('Migrated recent data from %s', cutoff_date)
    WHERE table_name = 'new_table_name' 
    AND operation = 'CREATE_WITH_RECENT_DATA' 
    AND status = 'STARTED';
    
    RAISE NOTICE 'Migrated % rows to new table', rows_migrated;
END $$;
```

#### Phase 3: Trigger Setup for New Data Capture

##### Step 6: Create Change Capture Triggers
```sql
-- Function to capture changes from original table to new table
CREATE OR REPLACE FUNCTION sync_to_new_table()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Insert new records into the new table
        INSERT INTO new_table_name (original_id, data_column1, data_column2, business_date, created_at, source_system)
        VALUES (NEW.id, NEW.data_column1, NEW.data_column2, NEW.business_date, NEW.created_date, 'TRIGGER_SYNC');
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        -- Update corresponding record in new table
        UPDATE new_table_name 
        SET data_column1 = NEW.data_column1,
            data_column2 = NEW.data_column2,
            business_date = NEW.business_date,
            updated_at = NOW()
        WHERE original_id = NEW.id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        -- Mark as deleted or actually delete based on business rules
        UPDATE new_table_name 
        SET updated_at = NOW(),
            source_system = 'DELETED_FROM_ORIGINAL'
        WHERE original_id = OLD.id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER sync_original_to_new_trigger
    AFTER INSERT OR UPDATE OR DELETE ON original_table_name
    FOR EACH ROW EXECUTE FUNCTION sync_to_new_table();
```

##### Step 7: Create Monitoring and Health Check Functions
```sql
-- Function to monitor sync health
CREATE OR REPLACE FUNCTION check_table_sync_health()
RETURNS TABLE (
    metric_name TEXT,
    metric_value BIGINT,
    last_checked TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'original_table_count'::TEXT,
        (SELECT COUNT(*) FROM original_table_name)::BIGINT,
        NOW()
    UNION ALL
    SELECT 
        'new_table_count'::TEXT,
        (SELECT COUNT(*) FROM new_table_name)::BIGINT,
        NOW()
    UNION ALL
    SELECT 
        'archive_table_count'::TEXT,
        (SELECT COUNT(*) FROM archive_table_name)::BIGINT,
        NOW()
    UNION ALL
    SELECT 
        'records_added_last_hour'::TEXT,
        (SELECT COUNT(*) FROM new_table_name WHERE created_at > NOW() - INTERVAL '1 hour')::BIGINT,
        NOW();
END;
$$ LANGUAGE plpgsql;
```

### 4. Replication Synchronization

#### Step 8: Sync to Passive Site
```sql
-- Script to prepare passive site for the changes
-- (Run on passive site after DDL replication or manual execution)

-- Verify table structures match
DO $$
DECLARE
    active_table_count BIGINT;
    passive_table_count BIGINT;
BEGIN
    -- This would be customized based on your replication setup
    -- Example verification queries
    
    SELECT COUNT(*) INTO passive_table_count 
    FROM information_schema.tables 
    WHERE table_name IN ('new_table_name', 'archive_table_name', 'table_rename_audit');
    
    IF passive_table_count < 3 THEN
        RAISE EXCEPTION 'Missing tables on passive site. Expected 3, found %', passive_table_count;
    END IF;
    
    RAISE NOTICE 'Passive site verification completed successfully';
END $$;
```

#### Step 9: Re-enable Replication
```sql
-- Re-enable replication after successful sync
UPDATE replication_control 
SET replication_enabled = TRUE,
    last_sync_time = NOW(),
    notes = 'Re-enabled after table rename strategy completion'
WHERE table_name = 'original_table_name';

-- Log the completion
INSERT INTO table_rename_audit (table_name, operation, start_time, end_time, status, notes)
VALUES ('ALL_TABLES', 'REPLICATION_RE_ENABLED', NOW(), NOW(), 'COMPLETED', 'Table rename strategy completed successfully');
```

### 5. Validation and Testing

#### Step 10: Data Validation Queries
```sql
-- Validation query to ensure data integrity
WITH validation_summary AS (
    SELECT 
        'Original Table' as table_type,
        COUNT(*) as record_count,
        MIN(created_date) as earliest_date,
        MAX(created_date) as latest_date
    FROM original_table_name
    
    UNION ALL
    
    SELECT 
        'New Table' as table_type,
        COUNT(*) as record_count,
        MIN(created_at) as earliest_date,
        MAX(created_at) as latest_date
    FROM new_table_name
    
    UNION ALL
    
    SELECT 
        'Archive Table' as table_type,
        COUNT(*) as record_count,
        MIN(created_date) as earliest_date,
        MAX(created_date) as latest_date
    FROM archive_table_name
)
SELECT * FROM validation_summary;

-- Check for data consistency
SELECT 
    COUNT(*) as overlapping_records
FROM new_table_name n
JOIN original_table_name o ON n.original_id = o.id
WHERE n.is_migrated = TRUE;
```

### 6. Cutover Strategy

#### Step 11: Application Cutover Plan
```sql
-- Create view for seamless application transition
CREATE OR REPLACE VIEW application_table_view AS
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
FROM new_table_name n
FULL OUTER JOIN original_table_name o ON n.original_id = o.id;
```

### 7. Monitoring and Maintenance

#### Step 12: Ongoing Monitoring Setup
```sql
-- Create monitoring view for operational visibility
CREATE OR REPLACE VIEW table_rename_monitoring AS
SELECT 
    'Data Distribution' as metric_category,
    metric_name,
    metric_value,
    last_checked
FROM check_table_sync_health()
UNION ALL
SELECT 
    'Migration Status' as metric_category,
    operation as metric_name,
    rows_affected as metric_value,
    end_time as last_checked
FROM table_rename_audit
WHERE status = 'COMPLETED'
ORDER BY last_checked DESC;
```

## Risk Mitigation

### Rollback Plan
1. **Immediate Rollback**: Drop triggers and revert to original table
2. **Data Recovery**: Restore from archive table if needed
3. **Replication Recovery**: Reset replication lag and sync

### Performance Considerations
- Monitor trigger overhead on the original table
- Consider batching for large data migrations
- Plan for increased storage during transition period
- Schedule maintenance windows for minimal business impact

### Security and Compliance
- Ensure audit trail is maintained throughout the process
- Verify data retention policies are met
- Document all changes for compliance requirements

## Success Criteria
- [ ] New table receives all new data via triggers
- [ ] Historical data preserved in archive table
- [ ] Replication lag within acceptable limits
- [ ] Application performance maintained or improved
- [ ] Data integrity validated
- [ ] Rollback procedures tested and documented

## Post-Implementation Tasks
1. Monitor system performance for 1 week
2. Validate data consistency daily for 1 week
3. Plan for original table cleanup after business approval
4. Update documentation and procedures
5. Train team on new monitoring and maintenance procedures