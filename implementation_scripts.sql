-- =====================================================
-- Table Rename Strategy Implementation Scripts
-- =====================================================
-- Execute these scripts in sequence on the ACTIVE site
-- Then sync to passive site as per your replication setup

-- =====================================================
-- PART 1: INFRASTRUCTURE SETUP
-- =====================================================

-- Create audit/logging infrastructure
CREATE SCHEMA IF NOT EXISTS table_migration;

CREATE TABLE IF NOT EXISTS table_migration.rename_audit (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    operation VARCHAR(50),
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP,
    status VARCHAR(20) DEFAULT 'STARTED',
    rows_affected BIGINT DEFAULT 0,
    error_message TEXT,
    notes TEXT,
    executed_by VARCHAR(100) DEFAULT CURRENT_USER
);

-- Create replication control table
CREATE TABLE IF NOT EXISTS table_migration.replication_control (
    table_name VARCHAR(255) PRIMARY KEY,
    replication_enabled BOOLEAN DEFAULT TRUE,
    last_sync_time TIMESTAMP,
    maintenance_window_start TIMESTAMP,
    maintenance_window_end TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create configuration table for the migration
CREATE TABLE IF NOT EXISTS table_migration.migration_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert default configuration
INSERT INTO table_migration.migration_config (config_key, config_value, description) VALUES
('data_retention_percentage', '0.1', 'Percentage of recent data to retain in new table (0.1 = 10%)'),
('batch_size', '10000', 'Batch size for data migration operations'),
('enable_parallel_processing', 'true', 'Enable parallel processing for large tables'),
('max_archive_age_days', '365', 'Maximum age in days for data to be archived')
ON CONFLICT (config_key) DO NOTHING;

-- =====================================================
-- PART 2: UTILITY FUNCTIONS
-- =====================================================

-- Function to get configuration values
CREATE OR REPLACE FUNCTION table_migration.get_config(p_key VARCHAR(100))
RETURNS TEXT AS $$
BEGIN
    RETURN (SELECT config_value FROM table_migration.migration_config WHERE config_key = p_key);
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to log migration steps
CREATE OR REPLACE FUNCTION table_migration.log_operation(
    p_table_name VARCHAR(255),
    p_operation VARCHAR(50),
    p_status VARCHAR(20) DEFAULT 'STARTED',
    p_notes TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    audit_id INTEGER;
BEGIN
    INSERT INTO table_migration.rename_audit (table_name, operation, status, notes)
    VALUES (p_table_name, p_operation, p_status, p_notes)
    RETURNING id INTO audit_id;
    
    RETURN audit_id;
END;
$$ LANGUAGE plpgsql;

-- Function to update operation status
CREATE OR REPLACE FUNCTION table_migration.update_operation(
    p_audit_id INTEGER,
    p_status VARCHAR(20),
    p_rows_affected BIGINT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE table_migration.rename_audit 
    SET end_time = NOW(),
        status = p_status,
        rows_affected = COALESCE(p_rows_affected, rows_affected),
        error_message = COALESCE(p_error_message, error_message),
        notes = COALESCE(p_notes, notes)
    WHERE id = p_audit_id;
END;
$$ LANGUAGE plpgsql;

-- Enhanced function to determine data cutoff point
CREATE OR REPLACE FUNCTION table_migration.get_data_cutoff_date(
    p_table_name TEXT, 
    p_date_column TEXT DEFAULT 'created_date',
    p_percentage NUMERIC DEFAULT NULL
)
RETURNS TIMESTAMP AS $$
DECLARE
    cutoff_date TIMESTAMP;
    total_rows BIGINT;
    target_rows BIGINT;
    retention_pct NUMERIC;
BEGIN
    -- Get retention percentage from config if not provided
    retention_pct := COALESCE(p_percentage, table_migration.get_config('data_retention_percentage')::NUMERIC);
    
    -- Get total row count
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO total_rows;
    
    -- Calculate target rows (last X%)
    target_rows := FLOOR(total_rows * retention_pct);
    
    -- Get cutoff date
    EXECUTE format(
        'SELECT %I FROM %I ORDER BY %I DESC LIMIT 1 OFFSET %s',
        p_date_column, p_table_name, p_date_column, target_rows
    ) INTO cutoff_date;
    
    -- Log the cutoff calculation
    INSERT INTO table_migration.rename_audit (table_name, operation, status, notes)
    VALUES (p_table_name, 'CUTOFF_CALCULATION', 'COMPLETED', 
            format('Total rows: %s, Retention: %s%%, Target rows: %s, Cutoff date: %s', 
                   total_rows, retention_pct * 100, target_rows, cutoff_date));
    
    RETURN cutoff_date;
END;
$$ LANGUAGE plpgsql;

-- Function to validate table structure compatibility
CREATE OR REPLACE FUNCTION table_migration.validate_table_structure(
    p_source_table TEXT,
    p_target_table TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    source_columns INTEGER;
    target_columns INTEGER;
    compatible_columns INTEGER;
BEGIN
    -- Count source table columns
    SELECT COUNT(*) INTO source_columns
    FROM information_schema.columns
    WHERE table_name = p_source_table AND table_schema = 'public';
    
    -- Count target table columns  
    SELECT COUNT(*) INTO target_columns
    FROM information_schema.columns
    WHERE table_name = p_target_table AND table_schema = 'public';
    
    -- Count compatible columns (same name and compatible types)
    SELECT COUNT(*) INTO compatible_columns
    FROM information_schema.columns s
    JOIN information_schema.columns t ON s.column_name = t.column_name
    WHERE s.table_name = p_source_table 
    AND t.table_name = p_target_table
    AND s.table_schema = 'public' 
    AND t.table_schema = 'public';
    
    -- Log validation results
    INSERT INTO table_migration.rename_audit (table_name, operation, status, notes)
    VALUES (format('%s->%s', p_source_table, p_target_table), 'STRUCTURE_VALIDATION', 'COMPLETED',
            format('Source: %s cols, Target: %s cols, Compatible: %s cols', 
                   source_columns, target_columns, compatible_columns));
    
    -- Return true if at least 80% of source columns are compatible
    RETURN (compatible_columns::NUMERIC / source_columns::NUMERIC) >= 0.8;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 3: DATA MIGRATION FUNCTIONS
-- =====================================================

-- Function to create archive table
CREATE OR REPLACE FUNCTION table_migration.create_archive_table(
    p_source_table TEXT,
    p_archive_table TEXT DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    archive_table_name TEXT;
    audit_id INTEGER;
BEGIN
    archive_table_name := COALESCE(p_archive_table, p_source_table || '_archive');
    audit_id := table_migration.log_operation(archive_table_name, 'CREATE_ARCHIVE_TABLE');
    
    BEGIN
        -- Create archive table with same structure as source
        EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING ALL)', archive_table_name, p_source_table);
        
        -- Add archive metadata columns
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP DEFAULT NOW()', archive_table_name);
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS archive_reason VARCHAR(100) DEFAULT ''TABLE_RENAME_STRATEGY''', archive_table_name);
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS archive_batch_id INTEGER', archive_table_name);
        
        -- Create index on archived_at for performance
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_archived_at ON %I(archived_at)', archive_table_name, archive_table_name);
        
        PERFORM table_migration.update_operation(audit_id, 'COMPLETED', 0, NULL, 'Archive table created successfully');
        
    EXCEPTION WHEN OTHERS THEN
        PERFORM table_migration.update_operation(audit_id, 'FAILED', 0, SQLERRM, 'Failed to create archive table');
        RAISE;
    END;
    
    RETURN archive_table_name;
END;
$$ LANGUAGE plpgsql;

-- Function to archive historical data in batches
CREATE OR REPLACE FUNCTION table_migration.archive_historical_data(
    p_source_table TEXT,
    p_archive_table TEXT,
    p_cutoff_date TIMESTAMP,
    p_date_column TEXT DEFAULT 'created_date',
    p_batch_size INTEGER DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    batch_size INTEGER;
    total_archived BIGINT := 0;
    batch_archived BIGINT;
    batch_id INTEGER := 1;
    audit_id INTEGER;
BEGIN
    batch_size := COALESCE(p_batch_size, table_migration.get_config('batch_size')::INTEGER);
    audit_id := table_migration.log_operation(p_source_table, 'ARCHIVE_HISTORICAL_DATA');
    
    BEGIN
        -- Archive data in batches
        LOOP
            EXECUTE format(
                'WITH batch_data AS (
                    SELECT * FROM %I 
                    WHERE %I < $1 
                    LIMIT $2
                )
                INSERT INTO %I 
                SELECT *, NOW(), ''HISTORICAL_ARCHIVE'', $3
                FROM batch_data',
                p_source_table, p_date_column, p_archive_table
            ) USING p_cutoff_date, batch_size, batch_id;
            
            GET DIAGNOSTICS batch_archived = ROW_COUNT;
            total_archived := total_archived + batch_archived;
            
            -- Log batch progress
            RAISE NOTICE 'Archived batch % with % rows (Total: %)', batch_id, batch_archived, total_archived;
            
            batch_id := batch_id + 1;
            
            -- Exit if no more rows to archive
            EXIT WHEN batch_archived = 0;
            
            -- Small delay to avoid overwhelming the system
            PERFORM pg_sleep(0.1);
        END LOOP;
        
        PERFORM table_migration.update_operation(audit_id, 'COMPLETED', total_archived, NULL, 
                format('Archived %s rows in %s batches', total_archived, batch_id - 1));
        
    EXCEPTION WHEN OTHERS THEN
        PERFORM table_migration.update_operation(audit_id, 'FAILED', total_archived, SQLERRM, 
                format('Failed after archiving %s rows', total_archived));
        RAISE;
    END;
    
    RETURN total_archived;
END;
$$ LANGUAGE plpgsql;

-- Function to migrate recent data to new table
CREATE OR REPLACE FUNCTION table_migration.migrate_recent_data(
    p_source_table TEXT,
    p_target_table TEXT,
    p_cutoff_date TIMESTAMP,
    p_date_column TEXT DEFAULT 'created_date'
)
RETURNS BIGINT AS $$
DECLARE
    rows_migrated BIGINT;
    audit_id INTEGER;
    column_mapping TEXT;
BEGIN
    audit_id := table_migration.log_operation(p_target_table, 'MIGRATE_RECENT_DATA');
    
    BEGIN
        -- Build column mapping (this is a simplified version - adjust based on your table structures)
        column_mapping := format(
            'INSERT INTO %I (original_id, data_column1, data_column2, business_date, created_at, is_migrated, source_system)
             SELECT id, data_column1, data_column2, business_date, %I, TRUE, ''MIGRATION''
             FROM %I WHERE %I >= $1',
            p_target_table, p_date_column, p_source_table, p_date_column
        );
        
        EXECUTE column_mapping USING p_cutoff_date;
        GET DIAGNOSTICS rows_migrated = ROW_COUNT;
        
        PERFORM table_migration.update_operation(audit_id, 'COMPLETED', rows_migrated, NULL, 
                format('Migrated %s recent rows from %s', rows_migrated, p_cutoff_date));
        
    EXCEPTION WHEN OTHERS THEN
        PERFORM table_migration.update_operation(audit_id, 'FAILED', 0, SQLERRM, 'Failed to migrate recent data');
        RAISE;
    END;
    
    RETURN rows_migrated;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 4: TRIGGER FUNCTIONS FOR ONGOING SYNC
-- =====================================================

-- Generic trigger function for syncing changes
CREATE OR REPLACE FUNCTION table_migration.sync_changes_trigger()
RETURNS TRIGGER AS $$
DECLARE
    target_table TEXT;
    sync_enabled BOOLEAN;
BEGIN
    -- Get target table name from trigger name convention: sync_[source]_to_[target]_trigger
    target_table := split_part(TG_NAME, '_to_', 2);
    target_table := split_part(target_table, '_trigger', 1);
    
    -- Check if sync is enabled for this table
    SELECT replication_enabled INTO sync_enabled
    FROM table_migration.replication_control
    WHERE table_name = TG_TABLE_NAME;
    
    IF NOT COALESCE(sync_enabled, TRUE) THEN
        RETURN COALESCE(NEW, OLD);
    END IF;
    
    IF TG_OP = 'INSERT' THEN
        -- Insert new records into the target table
        EXECUTE format(
            'INSERT INTO %I (original_id, data_column1, data_column2, business_date, created_at, source_system)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (original_id) DO UPDATE SET
                data_column1 = EXCLUDED.data_column1,
                data_column2 = EXCLUDED.data_column2,
                business_date = EXCLUDED.business_date,
                updated_at = NOW()',
            target_table
        ) USING NEW.id, NEW.data_column1, NEW.data_column2, NEW.business_date, NEW.created_date, 'TRIGGER_SYNC';
        
        RETURN NEW;
        
    ELSIF TG_OP = 'UPDATE' THEN
        -- Update corresponding record in target table
        EXECUTE format(
            'UPDATE %I SET 
                data_column1 = $1,
                data_column2 = $2,
                business_date = $3,
                updated_at = NOW()
             WHERE original_id = $4',
            target_table
        ) USING NEW.data_column1, NEW.data_column2, NEW.business_date, NEW.id;
        
        RETURN NEW;
        
    ELSIF TG_OP = 'DELETE' THEN
        -- Handle deletes based on business rules (soft delete vs hard delete)
        EXECUTE format(
            'UPDATE %I SET 
                updated_at = NOW(),
                source_system = ''DELETED_FROM_ORIGINAL''
             WHERE original_id = $1',
            target_table
        ) USING OLD.id;
        
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 5: MONITORING AND HEALTH CHECK FUNCTIONS
-- =====================================================

-- Comprehensive health check function
CREATE OR REPLACE FUNCTION table_migration.health_check()
RETURNS TABLE (
    check_category TEXT,
    check_name TEXT,
    check_value TEXT,
    status TEXT,
    last_checked TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    -- Table row counts
    SELECT 
        'Row Counts'::TEXT,
        'Original Table'::TEXT,
        (SELECT COUNT(*)::TEXT FROM original_table_name),
        'INFO'::TEXT,
        NOW()
    UNION ALL
    SELECT 
        'Row Counts'::TEXT,
        'New Table'::TEXT,
        (SELECT COUNT(*)::TEXT FROM new_table_name),
        'INFO'::TEXT,
        NOW()
    UNION ALL
    SELECT 
        'Row Counts'::TEXT,
        'Archive Table'::TEXT,
        (SELECT COUNT(*)::TEXT FROM archive_table_name),
        'INFO'::TEXT,
        NOW()
    UNION ALL
    -- Recent activity
    SELECT 
        'Activity'::TEXT,
        'Records Added Last Hour'::TEXT,
        (SELECT COUNT(*)::TEXT FROM new_table_name WHERE created_at > NOW() - INTERVAL '1 hour'),
        CASE WHEN (SELECT COUNT(*) FROM new_table_name WHERE created_at > NOW() - INTERVAL '1 hour') > 0 
             THEN 'ACTIVE'::TEXT ELSE 'IDLE'::TEXT END,
        NOW()
    UNION ALL
    -- Migration status
    SELECT 
        'Migration'::TEXT,
        'Last Operation'::TEXT,
        COALESCE((SELECT operation FROM table_migration.rename_audit ORDER BY start_time DESC LIMIT 1), 'NONE'),
        COALESCE((SELECT status FROM table_migration.rename_audit ORDER BY start_time DESC LIMIT 1), 'UNKNOWN'),
        COALESCE((SELECT end_time FROM table_migration.rename_audit ORDER BY start_time DESC LIMIT 1), NOW())
    UNION ALL
    -- Replication status
    SELECT 
        'Replication'::TEXT,
        'Status'::TEXT,
        CASE WHEN (SELECT replication_enabled FROM table_migration.replication_control WHERE table_name = 'original_table_name')
             THEN 'ENABLED' ELSE 'DISABLED' END,
        CASE WHEN (SELECT replication_enabled FROM table_migration.replication_control WHERE table_name = 'original_table_name')
             THEN 'OK' ELSE 'WARNING' END,
        NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to generate migration report
CREATE OR REPLACE FUNCTION table_migration.generate_migration_report()
RETURNS TABLE (
    operation_time TIMESTAMP,
    table_name TEXT,
    operation TEXT,
    status TEXT,
    rows_affected BIGINT,
    duration INTERVAL,
    notes TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ra.start_time,
        ra.table_name,
        ra.operation,
        ra.status,
        ra.rows_affected,
        COALESCE(ra.end_time - ra.start_time, NOW() - ra.start_time) as duration,
        ra.notes
    FROM table_migration.rename_audit ra
    ORDER BY ra.start_time DESC;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 6: MAIN ORCHESTRATION FUNCTION
-- =====================================================

-- Master function to orchestrate the entire migration
CREATE OR REPLACE FUNCTION table_migration.execute_table_rename_strategy(
    p_source_table TEXT,
    p_new_table TEXT,
    p_archive_table TEXT DEFAULT NULL,
    p_date_column TEXT DEFAULT 'created_date',
    p_dry_run BOOLEAN DEFAULT FALSE
)
RETURNS TEXT AS $$
DECLARE
    archive_table_name TEXT;
    cutoff_date TIMESTAMP;
    archived_rows BIGINT;
    migrated_rows BIGINT;
    validation_result BOOLEAN;
    result_summary TEXT;
    master_audit_id INTEGER;
BEGIN
    master_audit_id := table_migration.log_operation('MASTER_MIGRATION', 'EXECUTE_STRATEGY');
    
    BEGIN
        -- Step 1: Validate input parameters
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_source_table) THEN
            RAISE EXCEPTION 'Source table % does not exist', p_source_table;
        END IF;
        
        -- Step 2: Create archive table
        archive_table_name := table_migration.create_archive_table(p_source_table, p_archive_table);
        
        -- Step 3: Determine cutoff date
        cutoff_date := table_migration.get_data_cutoff_date(p_source_table, p_date_column);
        
        IF p_dry_run THEN
            result_summary := format(
                'DRY RUN COMPLETED - Would archive data before %s to table %s and migrate recent data to %s',
                cutoff_date, archive_table_name, p_new_table
            );
            PERFORM table_migration.update_operation(master_audit_id, 'DRY_RUN_COMPLETED', 0, NULL, result_summary);
            RETURN result_summary;
        END IF;
        
        -- Step 4: Disable replication during migration
        INSERT INTO table_migration.replication_control (table_name, replication_enabled, notes)
        VALUES (p_source_table, FALSE, 'Disabled for table rename strategy')
        ON CONFLICT (table_name) DO UPDATE SET 
            replication_enabled = FALSE,
            notes = 'Disabled for table rename strategy',
            updated_at = NOW();
        
        -- Step 5: Archive historical data
        archived_rows := table_migration.archive_historical_data(p_source_table, archive_table_name, cutoff_date, p_date_column);
        
        -- Step 6: Migrate recent data (assuming new table exists)
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_new_table) THEN
            migrated_rows := table_migration.migrate_recent_data(p_source_table, p_new_table, cutoff_date, p_date_column);
        ELSE
            RAISE NOTICE 'New table % does not exist. Skipping data migration step.', p_new_table;
            migrated_rows := 0;
        END IF;
        
        -- Step 7: Re-enable replication
        UPDATE table_migration.replication_control 
        SET replication_enabled = TRUE,
            last_sync_time = NOW(),
            notes = 'Re-enabled after table rename strategy completion',
            updated_at = NOW()
        WHERE table_name = p_source_table;
        
        result_summary := format(
            'MIGRATION COMPLETED - Archived %s rows to %s, Migrated %s rows to %s, Cutoff date: %s',
            archived_rows, archive_table_name, migrated_rows, p_new_table, cutoff_date
        );
        
        PERFORM table_migration.update_operation(master_audit_id, 'COMPLETED', archived_rows + migrated_rows, NULL, result_summary);
        
    EXCEPTION WHEN OTHERS THEN
        -- Re-enable replication on error
        UPDATE table_migration.replication_control 
        SET replication_enabled = TRUE,
            notes = 'Re-enabled after migration failure: ' || SQLERRM,
            updated_at = NOW()
        WHERE table_name = p_source_table;
        
        PERFORM table_migration.update_operation(master_audit_id, 'FAILED', 0, SQLERRM, 'Migration failed and replication re-enabled');
        RAISE;
    END;
    
    RETURN result_summary;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- USAGE EXAMPLES AND TESTING
-- =====================================================

/*
-- Example usage:

-- 1. Dry run first
SELECT table_migration.execute_table_rename_strategy(
    'original_table_name', 
    'new_table_name', 
    'archive_table_name',
    'created_date',
    TRUE  -- dry run
);

-- 2. Execute the actual migration
SELECT table_migration.execute_table_rename_strategy(
    'original_table_name', 
    'new_table_name', 
    'archive_table_name',
    'created_date',
    FALSE  -- actual run
);

-- 3. Check health status
SELECT * FROM table_migration.health_check();

-- 4. Generate migration report
SELECT * FROM table_migration.generate_migration_report();

-- 5. Set up ongoing sync trigger (after creating new table)
CREATE TRIGGER sync_original_to_new_trigger
    AFTER INSERT OR UPDATE OR DELETE ON original_table_name
    FOR EACH ROW EXECUTE FUNCTION table_migration.sync_changes_trigger();
*/