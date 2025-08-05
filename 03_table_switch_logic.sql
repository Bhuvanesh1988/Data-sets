-- =====================================================
-- TABLE SWITCH LOGIC
-- For both active and inactive sites in bi-directional replication
-- Atomic table switching with rollback capability
-- =====================================================

-- =====================================================
-- CORE SWITCH FUNCTIONS
-- =====================================================

-- Function to check if this is the primary (active) site
CREATE OR REPLACE FUNCTION is_primary_site()
RETURNS BOOLEAN AS $$
BEGIN
    -- Check if this database is in recovery mode (standby)
    RETURN NOT pg_is_in_recovery();
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to validate tables before switch
CREATE OR REPLACE FUNCTION validate_switch_readiness(
    p_old_table TEXT,
    p_new_table TEXT,
    p_check_data BOOLEAN DEFAULT TRUE
)
RETURNS JSON AS $$
DECLARE
    v_old_exists BOOLEAN;
    v_new_exists BOOLEAN;
    v_old_count BIGINT;
    v_new_count BIGINT;
    v_dependencies INTEGER;
    v_result JSON;
BEGIN
    -- Check table existence
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = p_old_table AND table_schema = 'public'
    ) INTO v_old_exists;
    
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = p_new_table AND table_schema = 'public'
    ) INTO v_new_exists;
    
    -- Get row counts if requested
    IF p_check_data THEN
        IF v_old_exists THEN
            EXECUTE format('SELECT COUNT(*) FROM %I', p_old_table) INTO v_old_count;
        END IF;
        
        IF v_new_exists THEN
            EXECUTE format('SELECT COUNT(*) FROM %I', p_new_table) INTO v_new_count;
        END IF;
    END IF;
    
    -- Check dependencies (foreign keys, views, etc.)
    SELECT COUNT(*) INTO v_dependencies
    FROM information_schema.table_constraints tc
    WHERE tc.table_name = p_old_table
    AND tc.constraint_type = 'FOREIGN KEY';
    
    -- Build result
    v_result := json_build_object(
        'old_table_exists', v_old_exists,
        'new_table_exists', v_new_exists,
        'old_table_count', COALESCE(v_old_count, 0),
        'new_table_count', COALESCE(v_new_count, 0),
        'dependencies_count', v_dependencies,
        'is_primary_site', is_primary_site(),
        'ready_for_switch', (v_old_exists AND v_new_exists)
    );
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- ATOMIC TABLE SWITCH PROCEDURE
-- =====================================================

CREATE OR REPLACE FUNCTION atomic_table_switch(
    p_old_table TEXT,
    p_new_table TEXT,
    p_backup_suffix TEXT DEFAULT '_backup_' || to_char(NOW(), 'YYYYMMDD_HH24MISS'),
    p_force_on_standby BOOLEAN DEFAULT FALSE
)
RETURNS JSON AS $$
DECLARE
    v_backup_table TEXT;
    v_temp_table TEXT;
    v_start_time TIMESTAMP := NOW();
    v_validation JSON;
    v_dependencies RECORD;
    v_constraint_sql TEXT;
    v_index_sql TEXT;
    v_trigger_sql TEXT;
    v_result JSON;
    v_step INTEGER := 0;
BEGIN
    v_step := 1;
    RAISE NOTICE 'Step %: Starting atomic table switch: % -> %', v_step, p_old_table, p_new_table;
    
    -- Validate environment
    IF NOT is_primary_site() AND NOT p_force_on_standby THEN
        RAISE EXCEPTION 'Table switch should be executed on primary site only. Use p_force_on_standby=TRUE to override.';
    END IF;
    
    v_step := 2;
    -- Validate switch readiness
    v_validation := validate_switch_readiness(p_old_table, p_new_table, TRUE);
    
    IF NOT (v_validation->>'ready_for_switch')::BOOLEAN THEN
        RAISE EXCEPTION 'Switch validation failed: %', v_validation;
    END IF;
    
    RAISE NOTICE 'Step %: Validation passed: %', v_step, v_validation;
    
    -- Generate names
    v_backup_table := p_old_table || p_backup_suffix;
    v_temp_table := p_old_table || '_temp_' || extract(epoch from now())::bigint;
    
    v_step := 3;
    RAISE NOTICE 'Step %: Generated names - backup: %, temp: %', v_step, v_backup_table, v_temp_table;
    
    -- Begin atomic switch transaction
    BEGIN
        v_step := 4;
        -- Step 1: Rename old table to backup
        EXECUTE format('ALTER TABLE %I RENAME TO %I', p_old_table, v_backup_table);
        RAISE NOTICE 'Step %: Renamed % to %', v_step, p_old_table, v_backup_table;
        
        v_step := 5;
        -- Step 2: Rename new table to original name
        EXECUTE format('ALTER TABLE %I RENAME TO %I', p_new_table, p_old_table);
        RAISE NOTICE 'Step %: Renamed % to %', v_step, p_new_table, p_old_table;
        
        v_step := 6;
        -- Step 3: Handle dependencies (foreign keys pointing TO this table)
        FOR v_dependencies IN
            SELECT tc.table_name as referencing_table,
                   tc.constraint_name,
                   kcu.column_name,
                   ccu.column_name as referenced_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu 
                ON tc.constraint_name = ccu.constraint_name
            WHERE ccu.table_name = v_backup_table
            AND tc.constraint_type = 'FOREIGN KEY'
        LOOP
            -- Drop and recreate foreign key constraints pointing to the switched table
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I',
                          v_dependencies.referencing_table, v_dependencies.constraint_name);
            
            EXECUTE format('ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I (%I)',
                          v_dependencies.referencing_table, v_dependencies.constraint_name,
                          v_dependencies.column_name, p_old_table, v_dependencies.referenced_column);
                          
            RAISE NOTICE 'Step %: Updated foreign key constraint % on %', 
                        v_step, v_dependencies.constraint_name, v_dependencies.referencing_table;
        END LOOP;
        
        v_step := 7;
        RAISE NOTICE 'Step %: Successfully completed atomic switch', v_step;
        
        -- Build success result
        v_result := json_build_object(
            'status', 'success',
            'old_table', p_old_table,
            'new_table', p_new_table,
            'backup_table', v_backup_table,
            'start_time', v_start_time,
            'end_time', NOW(),
            'duration_seconds', EXTRACT(EPOCH FROM (NOW() - v_start_time)),
            'is_primary_site', is_primary_site(),
            'final_step', v_step,
            'validation', v_validation
        );
        
        RETURN v_result;
        
    EXCEPTION WHEN OTHERS THEN
        -- Rollback: Try to restore original state
        RAISE NOTICE 'ERROR at step %: %, attempting rollback...', v_step, SQLERRM;
        
        BEGIN
            -- Try to restore original table names
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = v_backup_table) THEN
                EXECUTE format('ALTER TABLE %I RENAME TO %I', v_backup_table, v_temp_table);
            END IF;
            
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_old_table) THEN
                EXECUTE format('ALTER TABLE %I RENAME TO %I', p_old_table, p_new_table);
            END IF;
            
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = v_temp_table) THEN
                EXECUTE format('ALTER TABLE %I RENAME TO %I', v_temp_table, p_old_table);
            END IF;
            
            RAISE NOTICE 'Rollback completed successfully';
            
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'CRITICAL: Rollback also failed: %', SQLERRM;
        END;
        
        -- Return error result
        v_result := json_build_object(
            'status', 'failed',
            'error', SQLERRM,
            'failed_at_step', v_step,
            'start_time', v_start_time,
            'end_time', NOW(),
            'rollback_attempted', TRUE
        );
        
        RETURN v_result;
    END;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- COORDINATED SWITCH FOR BOTH SITES
-- =====================================================

-- Function to prepare switch script for standby site
CREATE OR REPLACE FUNCTION generate_standby_switch_script(
    p_old_table TEXT,
    p_new_table TEXT,
    p_backup_suffix TEXT DEFAULT '_backup_' || to_char(NOW(), 'YYYYMMDD_HH24MISS')
)
RETURNS TEXT AS $$
DECLARE
    v_script TEXT;
BEGIN
    v_script := format('
-- =====================================================
-- STANDBY SITE TABLE SWITCH SCRIPT
-- Generated: %s
-- Execute this script on the STANDBY site AFTER primary switch
-- =====================================================

-- 1. Verify this is standby site
DO $$
BEGIN
    IF NOT pg_is_in_recovery() THEN
        RAISE EXCEPTION ''This script should only be run on STANDBY site'';
    END IF;
    RAISE NOTICE ''Confirmed: This is a standby site'';
END $$;

-- 2. Wait for replication to catch up (adjust timeout as needed)
DO $$
DECLARE
    v_wait_seconds INTEGER := 0;
    v_max_wait INTEGER := 300; -- 5 minutes
BEGIN
    WHILE v_wait_seconds < v_max_wait LOOP
        -- Check if tables exist (indicating replication caught up)
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ''%s'') AND
           EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ''%s'') THEN
            RAISE NOTICE ''Replication caught up after % seconds'', v_wait_seconds;
            EXIT;
        END IF;
        
        PERFORM pg_sleep(5);
        v_wait_seconds := v_wait_seconds + 5;
        
        IF v_wait_seconds %% 30 = 0 THEN
            RAISE NOTICE ''Still waiting for replication... (% seconds)'', v_wait_seconds;
        END IF;
    END LOOP;
    
    IF v_wait_seconds >= v_max_wait THEN
        RAISE WARNING ''Replication sync timeout reached'';
    END IF;
END $$;

-- 3. Validate table switch readiness on standby
SELECT validate_switch_readiness(''%s'', ''%s'', TRUE);

-- 4. Execute coordinated switch on standby (forced)
SELECT atomic_table_switch(''%s'', ''%s'', ''%s'', TRUE);

-- 5. Verify switch completed successfully
SELECT 
    table_name,
    schemaname,
    tablespace
FROM pg_tables 
WHERE table_name IN (''%s'', ''%s'', ''%s'')
ORDER BY table_name;

RAISE NOTICE ''Standby site switch completed'';
',
    NOW(),
    p_old_table || p_backup_suffix,  -- backup table should exist after primary switch
    p_old_table,                     -- old table should be the new table now
    p_old_table,                     -- validation parameters
    p_new_table,
    p_old_table,                     -- switch parameters
    p_new_table,
    p_backup_suffix,
    p_old_table,                     -- verification tables
    p_new_table,
    p_old_table || p_backup_suffix
    );
    
    RETURN v_script;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- COORDINATED BI-DIRECTIONAL SWITCH
-- =====================================================

CREATE OR REPLACE FUNCTION coordinated_table_switch(
    p_old_table TEXT,
    p_new_table TEXT,
    p_generate_standby_script BOOLEAN DEFAULT TRUE,
    p_backup_suffix TEXT DEFAULT '_backup_' || to_char(NOW(), 'YYYYMMDD_HH24MISS')
)
RETURNS JSON AS $$
DECLARE
    v_primary_result JSON;
    v_standby_script TEXT;
    v_final_result JSON;
BEGIN
    -- Check if this is primary site
    IF NOT is_primary_site() THEN
        RAISE EXCEPTION 'Coordinated switch must be initiated from PRIMARY site only';
    END IF;
    
    RAISE NOTICE 'Starting coordinated bi-directional table switch...';
    
    -- Execute switch on primary site
    v_primary_result := atomic_table_switch(p_old_table, p_new_table, p_backup_suffix, FALSE);
    
    IF (v_primary_result->>'status')::TEXT != 'success' THEN
        RAISE EXCEPTION 'Primary site switch failed: %', v_primary_result;
    END IF;
    
    RAISE NOTICE 'Primary site switch completed successfully';
    
    -- Generate standby script if requested
    IF p_generate_standby_script THEN
        v_standby_script := generate_standby_switch_script(p_old_table, p_new_table, p_backup_suffix);
        RAISE NOTICE 'Standby script generated - save and execute on standby site';
    END IF;
    
    -- Build final result
    v_final_result := json_build_object(
        'coordinated_switch_status', 'primary_completed',
        'primary_result', v_primary_result,
        'standby_script_generated', p_generate_standby_script,
        'next_steps', ARRAY[
            'Execute the generated script on STANDBY site',
            'Verify both sites have consistent table structure',
            'Test application connectivity to both sites'
        ]
    );
    
    RETURN v_final_result;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- ROLLBACK FUNCTIONS
-- =====================================================

-- Function to rollback table switch
CREATE OR REPLACE FUNCTION rollback_table_switch(
    p_current_table TEXT,
    p_backup_table TEXT,
    p_force_on_standby BOOLEAN DEFAULT FALSE
)
RETURNS JSON AS $$
DECLARE
    v_temp_table TEXT;
    v_result JSON;
    v_start_time TIMESTAMP := NOW();
BEGIN
    -- Validate environment
    IF NOT is_primary_site() AND NOT p_force_on_standby THEN
        RAISE EXCEPTION 'Table rollback should be executed on primary site only. Use p_force_on_standby=TRUE to override.';
    END IF;
    
    -- Check if backup table exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_backup_table) THEN
        RAISE EXCEPTION 'Backup table % does not exist', p_backup_table;
    END IF;
    
    v_temp_table := p_current_table || '_rollback_temp_' || extract(epoch from now())::bigint;
    
    RAISE NOTICE 'Starting rollback: % -> %', p_current_table, p_backup_table;
    
    BEGIN
        -- Rename current table to temp
        EXECUTE format('ALTER TABLE %I RENAME TO %I', p_current_table, v_temp_table);
        
        -- Rename backup to current
        EXECUTE format('ALTER TABLE %I RENAME TO %I', p_backup_table, p_current_table);
        
        -- Drop temp table (the "new" table we're rolling back from)
        EXECUTE format('DROP TABLE IF EXISTS %I', v_temp_table);
        
        v_result := json_build_object(
            'status', 'success',
            'rollback_completed', TRUE,
            'current_table', p_current_table,
            'backup_table', p_backup_table,
            'start_time', v_start_time,
            'end_time', NOW(),
            'duration_seconds', EXTRACT(EPOCH FROM (NOW() - v_start_time))
        );
        
        RAISE NOTICE 'Rollback completed successfully';
        
    EXCEPTION WHEN OTHERS THEN
        v_result := json_build_object(
            'status', 'failed',
            'error', SQLERRM,
            'start_time', v_start_time,
            'end_time', NOW()
        );
        
        RAISE NOTICE 'Rollback failed: %', SQLERRM;
    END;
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- CLEANUP FUNCTIONS
-- =====================================================

-- Function to clean up backup tables after successful switch
CREATE OR REPLACE FUNCTION cleanup_backup_tables(
    p_table_pattern TEXT,
    p_older_than_days INTEGER DEFAULT 7,
    p_dry_run BOOLEAN DEFAULT TRUE
)
RETURNS JSON AS $$
DECLARE
    v_backup_table RECORD;
    v_drop_count INTEGER := 0;
    v_result JSON;
    v_tables_to_drop TEXT[] := '{}';
BEGIN
    -- Find backup tables matching pattern
    FOR v_backup_table IN
        SELECT table_name, 
               created_time
        FROM (
            SELECT table_name,
                   CASE 
                       WHEN table_name ~ '_backup_\d{8}_\d{6}$' THEN
                           to_timestamp(
                               substring(table_name from '_backup_(\d{8}_\d{6})$'),
                               'YYYYMMDD_HH24MISS'
                           )
                       ELSE NOW() -- If we can't parse date, treat as recent
                   END as created_time
            FROM information_schema.tables
            WHERE table_name LIKE '%' || p_table_pattern || '%backup%'
            AND table_schema = 'public'
        ) t
        WHERE created_time < NOW() - (p_older_than_days || ' days')::INTERVAL
    LOOP
        v_tables_to_drop := array_append(v_tables_to_drop, v_backup_table.table_name);
        
        IF NOT p_dry_run THEN
            EXECUTE format('DROP TABLE %I', v_backup_table.table_name);
            v_drop_count := v_drop_count + 1;
            RAISE NOTICE 'Dropped backup table: %', v_backup_table.table_name;
        ELSE
            RAISE NOTICE 'Would drop backup table: % (created: %)', 
                        v_backup_table.table_name, v_backup_table.created_time;
        END IF;
    END LOOP;
    
    v_result := json_build_object(
        'dry_run', p_dry_run,
        'tables_found', array_length(v_tables_to_drop, 1),
        'tables_dropped', v_drop_count,
        'backup_tables', v_tables_to_drop,
        'older_than_days', p_older_than_days
    );
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

/*
-- =====================================================
-- USAGE EXAMPLES:
-- =====================================================

-- 1. Validate switch readiness
SELECT validate_switch_readiness('orders', 'orders_new', TRUE);

-- 2. Execute coordinated switch (PRIMARY site only)
SELECT coordinated_table_switch('orders', 'orders_new', TRUE);

-- 3. Generate standby script separately
SELECT generate_standby_switch_script('orders', 'orders_new');

-- 4. Execute atomic switch on single site
SELECT atomic_table_switch('orders', 'orders_new');

-- 5. Rollback if needed
SELECT rollback_table_switch('orders', 'orders_backup_20241208_143022');

-- 6. Clean up old backup tables (dry run first)
SELECT cleanup_backup_tables('orders', 7, TRUE);

-- 7. Actually clean up old backups
SELECT cleanup_backup_tables('orders', 7, FALSE);

-- 8. Check site role
SELECT is_primary_site();
*/