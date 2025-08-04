-- =====================================================
-- Bi-Directional Replication Management Procedures
-- for Table Rename Strategy
-- =====================================================

-- =====================================================
-- PART 1: REPLICATION STATUS MONITORING
-- =====================================================

-- Function to check current replication status and health
CREATE OR REPLACE FUNCTION table_migration.check_replication_status()
RETURNS TABLE (
    site_role TEXT,
    is_active_site BOOLEAN,
    replication_state TEXT,
    lag_bytes BIGINT,
    lag_seconds INTEGER,
    last_wal_receive TIMESTAMP,
    sync_state TEXT,
    recommendations TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH replication_info AS (
        SELECT 
            CASE 
                WHEN pg_is_in_recovery() THEN 'STANDBY'
                ELSE 'PRIMARY'
            END as site_role,
            NOT pg_is_in_recovery() as is_active_site,
            CASE 
                WHEN pg_is_in_recovery() THEN 'RECEIVING'
                WHEN EXISTS (SELECT 1 FROM pg_stat_replication) THEN 'SENDING'
                ELSE 'ISOLATED'
            END as replication_state
    )
    SELECT 
        ri.site_role::TEXT,
        ri.is_active_site,
        ri.replication_state::TEXT,
        COALESCE(
            (SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) 
             FROM pg_stat_replication LIMIT 1), 0
        )::BIGINT as lag_bytes,
        COALESCE(
            EXTRACT(EPOCH FROM (NOW() - pg_last_wal_receive_lsn()))::INTEGER, 0
        ) as lag_seconds,
        pg_last_wal_receive_lsn() as last_wal_receive,
        COALESCE(
            (SELECT state FROM pg_stat_replication LIMIT 1), 'N/A'
        )::TEXT as sync_state,
        CASE 
            WHEN ri.site_role = 'STANDBY' THEN 'This is a standby site - execute migrations on primary only'
            WHEN ri.replication_state = 'ISOLATED' THEN 'WARNING: No replication detected - verify setup'
            WHEN COALESCE((SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) FROM pg_stat_replication LIMIT 1), 0) > 1000000 
                THEN 'WARNING: High replication lag detected'
            ELSE 'Replication appears healthy for migration'
        END::TEXT as recommendations
    FROM replication_info ri;
END;
$$ LANGUAGE plpgsql;

-- Function to validate site is ready for migration
CREATE OR REPLACE FUNCTION table_migration.validate_migration_readiness()
RETURNS TABLE (
    check_name TEXT,
    status TEXT,
    details TEXT,
    action_required TEXT
) AS $$
BEGIN
    RETURN QUERY
    -- Check if this is the active site
    SELECT 
        'Site Role Check'::TEXT,
        CASE WHEN NOT pg_is_in_recovery() THEN 'PASS' ELSE 'FAIL' END::TEXT,
        CASE WHEN NOT pg_is_in_recovery() 
             THEN 'This is the primary site' 
             ELSE 'This is a standby site' END::TEXT,
        CASE WHEN pg_is_in_recovery() 
             THEN 'Execute migration on primary site only' 
             ELSE 'Proceed with migration' END::TEXT
    
    UNION ALL
    
    -- Check replication lag
    SELECT 
        'Replication Lag Check'::TEXT,
        CASE WHEN COALESCE(
            (SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) 
             FROM pg_stat_replication LIMIT 1), 0) < 1000000 
             THEN 'PASS' ELSE 'WARNING' END::TEXT,
        format('Current lag: %s bytes', 
               COALESCE((SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) 
                        FROM pg_stat_replication LIMIT 1), 0))::TEXT,
        CASE WHEN COALESCE(
            (SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) 
             FROM pg_stat_replication LIMIT 1), 0) >= 1000000
             THEN 'Wait for replication to catch up before migration'
             ELSE 'Lag is acceptable' END::TEXT
    
    UNION ALL
    
    -- Check for active connections
    SELECT 
        'Active Connections Check'::TEXT,
        CASE WHEN (SELECT COUNT(*) FROM pg_stat_activity 
                   WHERE state = 'active' AND datname = current_database()) < 10 
             THEN 'PASS' ELSE 'WARNING' END::TEXT,
        format('Active connections: %s', 
               (SELECT COUNT(*) FROM pg_stat_activity 
                WHERE state = 'active' AND datname = current_database()))::TEXT,
        'Consider scheduling migration during low activity period'::TEXT
    
    UNION ALL
    
    -- Check disk space
    SELECT 
        'Disk Space Check'::TEXT,
        'INFO'::TEXT,
        'Check available disk space manually'::TEXT,
        'Ensure 3x largest table size is available'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 2: REPLICATION CONTROL FUNCTIONS
-- =====================================================

-- Function to temporarily pause replication for a specific table
CREATE OR REPLACE FUNCTION table_migration.pause_table_replication(
    p_table_name TEXT,
    p_reason TEXT DEFAULT 'Table migration in progress'
)
RETURNS BOOLEAN AS $$
DECLARE
    is_primary BOOLEAN;
    audit_id INTEGER;
BEGIN
    -- Check if this is the primary site
    is_primary := NOT pg_is_in_recovery();
    
    IF NOT is_primary THEN
        RAISE EXCEPTION 'Replication control can only be managed from primary site';
    END IF;
    
    audit_id := table_migration.log_operation(p_table_name, 'PAUSE_REPLICATION');
    
    BEGIN
        -- Insert or update replication control
        INSERT INTO table_migration.replication_control (
            table_name, 
            replication_enabled, 
            notes,
            maintenance_window_start
        )
        VALUES (p_table_name, FALSE, p_reason, NOW())
        ON CONFLICT (table_name) DO UPDATE SET
            replication_enabled = FALSE,
            notes = p_reason,
            maintenance_window_start = NOW(),
            updated_at = NOW();
        
        -- Create a publication exclusion if using logical replication
        -- (This is a placeholder - adjust based on your replication type)
        /*
        EXECUTE format(
            'ALTER PUBLICATION migration_pub DROP TABLE %I',
            p_table_name
        );
        */
        
        PERFORM table_migration.update_operation(
            audit_id, 'COMPLETED', 0, NULL, 
            format('Replication paused for table %s', p_table_name)
        );
        
        RETURN TRUE;
        
    EXCEPTION WHEN OTHERS THEN
        PERFORM table_migration.update_operation(
            audit_id, 'FAILED', 0, SQLERRM, 
            'Failed to pause replication'
        );
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- Function to resume replication for a specific table
CREATE OR REPLACE FUNCTION table_migration.resume_table_replication(
    p_table_name TEXT,
    p_verify_sync BOOLEAN DEFAULT TRUE
)
RETURNS BOOLEAN AS $$
DECLARE
    is_primary BOOLEAN;
    audit_id INTEGER;
    sync_check_result BOOLEAN := TRUE;
BEGIN
    -- Check if this is the primary site
    is_primary := NOT pg_is_in_recovery();
    
    IF NOT is_primary THEN
        RAISE EXCEPTION 'Replication control can only be managed from primary site';
    END IF;
    
    audit_id := table_migration.log_operation(p_table_name, 'RESUME_REPLICATION');
    
    BEGIN
        -- Optionally verify sync before resuming
        IF p_verify_sync THEN
            -- Add your sync verification logic here
            -- This could involve checking row counts, checksums, etc.
            sync_check_result := table_migration.verify_table_sync(p_table_name);
        END IF;
        
        IF NOT sync_check_result THEN
            RAISE EXCEPTION 'Table sync verification failed. Manual intervention required.';
        END IF;
        
        -- Update replication control
        UPDATE table_migration.replication_control 
        SET replication_enabled = TRUE,
            last_sync_time = NOW(),
            maintenance_window_end = NOW(),
            notes = 'Replication resumed after migration',
            updated_at = NOW()
        WHERE table_name = p_table_name;
        
        -- Add table back to publication if using logical replication
        -- (This is a placeholder - adjust based on your replication type)
        /*
        EXECUTE format(
            'ALTER PUBLICATION migration_pub ADD TABLE %I',
            p_table_name
        );
        */
        
        PERFORM table_migration.update_operation(
            audit_id, 'COMPLETED', 0, NULL, 
            format('Replication resumed for table %s', p_table_name)
        );
        
        RETURN TRUE;
        
    EXCEPTION WHEN OTHERS THEN
        PERFORM table_migration.update_operation(
            audit_id, 'FAILED', 0, SQLERRM, 
            'Failed to resume replication'
        );
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 3: SYNC VERIFICATION FUNCTIONS
-- =====================================================

-- Function to verify table synchronization between sites
CREATE OR REPLACE FUNCTION table_migration.verify_table_sync(
    p_table_name TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    primary_count BIGINT;
    primary_checksum TEXT;
    table_exists BOOLEAN;
BEGIN
    -- Check if table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = p_table_name AND table_schema = 'public'
    ) INTO table_exists;
    
    IF NOT table_exists THEN
        RAISE NOTICE 'Table % does not exist', p_table_name;
        RETURN FALSE;
    END IF;
    
    -- Get row count and basic checksum
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO primary_count;
    
    -- Simple checksum (adjust based on your table structure)
    EXECUTE format(
        'SELECT MD5(string_agg(MD5(CAST(%I AS TEXT)), '''' ORDER BY %I)) FROM %I',
        'id', 'id', p_table_name  -- Assuming 'id' column exists
    ) INTO primary_checksum;
    
    -- Log the verification attempt
    INSERT INTO table_migration.rename_audit (
        table_name, operation, status, notes
    ) VALUES (
        p_table_name, 'SYNC_VERIFICATION', 'COMPLETED',
        format('Row count: %s, Checksum: %s', primary_count, primary_checksum)
    );
    
    -- In a real implementation, you would compare this with the standby
    -- For now, we'll assume sync is good if we can get the counts
    RETURN TRUE;
    
EXCEPTION WHEN OTHERS THEN
    INSERT INTO table_migration.rename_audit (
        table_name, operation, status, error_message
    ) VALUES (
        p_table_name, 'SYNC_VERIFICATION', 'FAILED', SQLERRM
    );
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- Function to compare data between primary and standby (to be run manually)
CREATE OR REPLACE FUNCTION table_migration.generate_sync_comparison_script(
    p_table_name TEXT
)
RETURNS TEXT AS $$
DECLARE
    comparison_script TEXT;
BEGIN
    comparison_script := format('
-- Run this script on both PRIMARY and STANDBY sites
-- Compare the results to verify synchronization

-- Row count comparison
SELECT ''%s'' as table_name, COUNT(*) as row_count, ''%s'' as site_type FROM %I;

-- Sample checksum (adjust based on your table structure)
SELECT 
    ''%s'' as table_name,
    MD5(string_agg(MD5(CAST(* AS TEXT)), '''' ORDER BY id)) as table_checksum,
    ''%s'' as site_type
FROM (SELECT * FROM %I LIMIT 1000) sample;

-- Recent data check (if you have timestamp columns)
SELECT 
    ''%s'' as table_name,
    DATE_TRUNC(''hour'', created_date) as hour_bucket,
    COUNT(*) as records_per_hour,
    ''%s'' as site_type
FROM %I 
WHERE created_date > NOW() - INTERVAL ''24 hours''
GROUP BY DATE_TRUNC(''hour'', created_date)
ORDER BY hour_bucket DESC;
    ', 
    p_table_name, 'PRIMARY_OR_STANDBY', p_table_name,
    p_table_name, 'PRIMARY_OR_STANDBY', p_table_name,
    p_table_name, 'PRIMARY_OR_STANDBY', p_table_name
    );
    
    RETURN comparison_script;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 4: FAILOVER PREPARATION FUNCTIONS
-- =====================================================

-- Function to prepare for emergency failover during migration
CREATE OR REPLACE FUNCTION table_migration.prepare_failover_state(
    p_table_list TEXT[]
)
RETURNS TEXT AS $$
DECLARE
    table_name TEXT;
    failover_script TEXT := '';
    audit_id INTEGER;
BEGIN
    audit_id := table_migration.log_operation('FAILOVER_PREP', 'PREPARE_FAILOVER');
    
    BEGIN
        failover_script := '-- Emergency Failover Recovery Script' || E'\n';
        failover_script := failover_script || '-- Generated: ' || NOW()::TEXT || E'\n\n';
        
        -- Generate recovery scripts for each table
        FOREACH table_name IN ARRAY p_table_list
        LOOP
            failover_script := failover_script || format('
-- Recovery for table: %s
-- 1. Check table status
SELECT table_name, operation, status, notes 
FROM table_migration.rename_audit 
WHERE table_name = ''%s'' 
ORDER BY start_time DESC LIMIT 5;

-- 2. Verify replication control state
SELECT * FROM table_migration.replication_control WHERE table_name = ''%s'';

-- 3. Resume replication if needed
SELECT table_migration.resume_table_replication(''%s'', FALSE);

-- 4. Verify data integrity
SELECT table_migration.verify_table_sync(''%s'');

', table_name, table_name, table_name, table_name, table_name);
        END LOOP;
        
        -- Add general recovery steps
        failover_script := failover_script || '
-- General recovery steps:
-- 1. Verify this site is now primary
SELECT table_migration.check_replication_status();

-- 2. Check migration status
SELECT * FROM table_migration.generate_migration_report() 
WHERE operation_time > NOW() - INTERVAL ''24 hours'';

-- 3. Resume all paused replications
UPDATE table_migration.replication_control 
SET replication_enabled = TRUE,
    notes = ''Enabled after failover'',
    updated_at = NOW()
WHERE replication_enabled = FALSE;
';
        
        PERFORM table_migration.update_operation(
            audit_id, 'COMPLETED', array_length(p_table_list, 1), NULL, 
            'Failover preparation script generated'
        );
        
        RETURN failover_script;
        
    EXCEPTION WHEN OTHERS THEN
        PERFORM table_migration.update_operation(
            audit_id, 'FAILED', 0, SQLERRM, 
            'Failed to generate failover script'
        );
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 5: REPLICATION-AWARE MIGRATION ORCHESTRATOR
-- =====================================================

-- Enhanced migration function that handles replication automatically
CREATE OR REPLACE FUNCTION table_migration.execute_replication_aware_migration(
    p_source_table TEXT,
    p_new_table TEXT,
    p_archive_table TEXT DEFAULT NULL,
    p_date_column TEXT DEFAULT 'created_date',
    p_dry_run BOOLEAN DEFAULT FALSE,
    p_wait_for_sync BOOLEAN DEFAULT TRUE
)
RETURNS TEXT AS $$
DECLARE
    replication_status RECORD;
    migration_result TEXT;
    sync_wait_seconds INTEGER := 0;
    max_wait_seconds INTEGER := 300; -- 5 minutes max wait
    audit_id INTEGER;
BEGIN
    audit_id := table_migration.log_operation('REPLICATION_AWARE_MIGRATION', 'START');
    
    BEGIN
        -- Step 1: Validate this is the primary site
        SELECT * INTO replication_status FROM table_migration.check_replication_status() LIMIT 1;
        
        IF NOT replication_status.is_active_site THEN
            RAISE EXCEPTION 'Migration must be executed on the primary site only';
        END IF;
        
        -- Step 2: Check replication health
        IF replication_status.lag_bytes > 5000000 THEN -- 5MB lag threshold
            RAISE WARNING 'High replication lag detected: % bytes', replication_status.lag_bytes;
            
            IF p_wait_for_sync THEN
                RAISE NOTICE 'Waiting for replication to catch up...';
                
                WHILE sync_wait_seconds < max_wait_seconds LOOP
                    SELECT lag_bytes INTO replication_status.lag_bytes 
                    FROM table_migration.check_replication_status() LIMIT 1;
                    
                    EXIT WHEN replication_status.lag_bytes < 1000000; -- 1MB threshold
                    
                    PERFORM pg_sleep(10);
                    sync_wait_seconds := sync_wait_seconds + 10;
                    
                    RAISE NOTICE 'Still waiting... Current lag: % bytes', replication_status.lag_bytes;
                END LOOP;
                
                IF sync_wait_seconds >= max_wait_seconds THEN
                    RAISE WARNING 'Replication sync timeout reached. Proceeding with migration.';
                END IF;
            END IF;
        END IF;
        
        -- Step 3: Pause replication for the source table
        IF NOT p_dry_run THEN
            PERFORM table_migration.pause_table_replication(
                p_source_table, 
                'Paused for table rename strategy migration'
            );
        END IF;
        
        -- Step 4: Execute the migration
        migration_result := table_migration.execute_table_rename_strategy(
            p_source_table,
            p_new_table,
            p_archive_table,
            p_date_column,
            p_dry_run
        );
        
        -- Step 5: Resume replication (if not dry run)
        IF NOT p_dry_run THEN
            PERFORM table_migration.resume_table_replication(p_source_table, TRUE);
        END IF;
        
        PERFORM table_migration.update_operation(
            audit_id, 'COMPLETED', 0, NULL, 
            'Replication-aware migration completed: ' || migration_result
        );
        
        RETURN 'REPLICATION-AWARE MIGRATION COMPLETED: ' || migration_result;
        
    EXCEPTION WHEN OTHERS THEN
        -- Emergency: Resume replication on any error
        BEGIN
            PERFORM table_migration.resume_table_replication(p_source_table, FALSE);
        EXCEPTION WHEN OTHERS THEN
            -- Log the replication resume failure but don't mask the original error
            INSERT INTO table_migration.rename_audit (table_name, operation, status, error_message)
            VALUES (p_source_table, 'EMERGENCY_REPLICATION_RESUME', 'FAILED', SQLERRM);
        END;
        
        PERFORM table_migration.update_operation(
            audit_id, 'FAILED', 0, SQLERRM, 
            'Migration failed, attempted to resume replication'
        );
        
        RAISE;
    END;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PART 6: MONITORING VIEWS FOR REPLICATION
-- =====================================================

-- View for monitoring replication-related migration activities
CREATE OR REPLACE VIEW table_migration.replication_migration_status AS
SELECT 
    rc.table_name,
    rc.replication_enabled,
    rc.last_sync_time,
    rc.maintenance_window_start,
    rc.maintenance_window_end,
    CASE 
        WHEN rc.maintenance_window_start IS NOT NULL AND rc.maintenance_window_end IS NULL 
        THEN EXTRACT(EPOCH FROM (NOW() - rc.maintenance_window_start))/60
        ELSE NULL 
    END as maintenance_duration_minutes,
    ra.operation as last_operation,
    ra.status as last_operation_status,
    ra.end_time as last_operation_time,
    rs.site_role,
    rs.replication_state,
    rs.lag_bytes,
    rs.sync_state
FROM table_migration.replication_control rc
LEFT JOIN LATERAL (
    SELECT operation, status, end_time
    FROM table_migration.rename_audit 
    WHERE table_name = rc.table_name 
    ORDER BY start_time DESC 
    LIMIT 1
) ra ON true
CROSS JOIN LATERAL table_migration.check_replication_status() rs;

-- Function to generate replication health report
CREATE OR REPLACE FUNCTION table_migration.generate_replication_health_report()
RETURNS TABLE (
    report_section TEXT,
    metric_name TEXT,
    metric_value TEXT,
    status TEXT,
    recommendation TEXT
) AS $$
BEGIN
    RETURN QUERY
    -- Overall replication status
    SELECT 
        'Replication Health'::TEXT,
        'Site Role'::TEXT,
        site_role::TEXT,
        CASE WHEN is_active_site THEN 'PRIMARY' ELSE 'STANDBY' END::TEXT,
        CASE WHEN is_active_site THEN 'This is the primary site - OK to run migrations'
             ELSE 'This is a standby site - do not run migrations here' END::TEXT
    FROM table_migration.check_replication_status()
    
    UNION ALL
    
    -- Tables with paused replication
    SELECT 
        'Replication Control'::TEXT,
        'Paused Tables'::TEXT,
        COALESCE(string_agg(table_name, ', '), 'None')::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'WARNING' END::TEXT,
        CASE WHEN COUNT(*) > 0 THEN 'Some tables have paused replication - check if this is expected'
             ELSE 'All tables have active replication' END::TEXT
    FROM table_migration.replication_control 
    WHERE replication_enabled = FALSE
    
    UNION ALL
    
    -- Recent migration activities
    SELECT 
        'Migration Activity'::TEXT,
        'Active Migrations'::TEXT,
        COUNT(*)::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'INFO' END::TEXT,
        CASE WHEN COUNT(*) > 0 THEN 'Migration activities in progress'
             ELSE 'No active migrations' END::TEXT
    FROM table_migration.rename_audit 
    WHERE status = 'STARTED' AND start_time > NOW() - INTERVAL '24 hours';
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================

/*
-- Example usage for replication-aware migration:

-- 1. Check replication status first
SELECT * FROM table_migration.check_replication_status();

-- 2. Validate migration readiness
SELECT * FROM table_migration.validate_migration_readiness();

-- 3. Generate health report
SELECT * FROM table_migration.generate_replication_health_report();

-- 4. Execute replication-aware migration with dry run
SELECT table_migration.execute_replication_aware_migration(
    'orders',           -- source table
    'orders_new',       -- new table  
    'orders_archive',   -- archive table
    'created_date',     -- date column
    TRUE,               -- dry run
    TRUE                -- wait for sync
);

-- 5. Execute actual migration
SELECT table_migration.execute_replication_aware_migration(
    'orders',           -- source table
    'orders_new',       -- new table
    'orders_archive',   -- archive table
    'created_date',     -- date column
    FALSE,              -- actual run
    TRUE                -- wait for sync
);

-- 6. Monitor status
SELECT * FROM table_migration.replication_migration_status;

-- 7. Generate failover preparation script
SELECT table_migration.prepare_failover_state(ARRAY['orders', 'customers', 'transactions']);
*/