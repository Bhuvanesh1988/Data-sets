-- =====================================================
-- BATCH EXECUTION PROCEDURE
-- Standalone procedure for migrating data in batches
-- No extra tables required, minimal logging
-- =====================================================

CREATE OR REPLACE FUNCTION batch_migrate_data(
    p_source_table TEXT,
    p_target_table TEXT,
    p_archive_table TEXT,
    p_cutoff_date TIMESTAMP,
    p_date_column TEXT DEFAULT 'created_date',
    p_batch_size INTEGER DEFAULT 10000,
    p_sleep_ms INTEGER DEFAULT 100,
    p_max_batches INTEGER DEFAULT 1000
)
RETURNS JSON AS $$
DECLARE
    v_batch_count INTEGER := 0;
    v_total_archived BIGINT := 0;
    v_total_migrated BIGINT := 0;
    v_batch_archived BIGINT;
    v_batch_migrated BIGINT;
    v_start_time TIMESTAMP := NOW();
    v_batch_start_time TIMESTAMP;
    v_archive_sql TEXT;
    v_migrate_sql TEXT;
    v_result JSON;
BEGIN
    -- Validate input parameters
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_source_table) THEN
        RAISE EXCEPTION 'Source table % does not exist', p_source_table;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_target_table) THEN
        RAISE EXCEPTION 'Target table % does not exist', p_target_table;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = p_archive_table) THEN
        RAISE EXCEPTION 'Archive table % does not exist', p_archive_table;
    END IF;
    
    RAISE NOTICE 'Starting batch migration: % -> % (archive: %), cutoff: %', 
                 p_source_table, p_target_table, p_archive_table, p_cutoff_date;
    
    -- Build archive SQL (for historical data)
    v_archive_sql := format(
        'WITH batch_data AS (
            SELECT * FROM %I 
            WHERE %I < $1 
            ORDER BY %I 
            LIMIT $2
        ),
        archived AS (
            INSERT INTO %I 
            SELECT * FROM batch_data 
            RETURNING 1
        )
        SELECT COUNT(*) FROM archived',
        p_source_table, p_date_column, p_date_column, p_archive_table
    );
    
    -- Build migration SQL (for recent data)
    v_migrate_sql := format(
        'WITH batch_data AS (
            SELECT * FROM %I 
            WHERE %I >= $1 
            ORDER BY %I 
            LIMIT $2
        ),
        migrated AS (
            INSERT INTO %I 
            SELECT * FROM batch_data 
            ON CONFLICT DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) FROM migrated',
        p_source_table, p_date_column, p_date_column, p_target_table
    );
    
    -- Archive historical data in batches
    RAISE NOTICE 'Phase 1: Archiving historical data (before %)', p_cutoff_date;
    
    LOOP
        v_batch_start_time := clock_timestamp();
        
        -- Execute archive batch
        EXECUTE v_archive_sql USING p_cutoff_date, p_batch_size INTO v_batch_archived;
        
        v_total_archived := v_total_archived + v_batch_archived;
        v_batch_count := v_batch_count + 1;
        
        RAISE NOTICE 'Archive batch %: % rows (%.2f ms) - Total: %', 
                     v_batch_count, v_batch_archived, 
                     EXTRACT(MILLISECONDS FROM clock_timestamp() - v_batch_start_time),
                     v_total_archived;
        
        -- Exit conditions
        EXIT WHEN v_batch_archived = 0;
        EXIT WHEN v_batch_count >= p_max_batches;
        
        -- Small delay to reduce system load
        IF p_sleep_ms > 0 THEN
            PERFORM pg_sleep(p_sleep_ms / 1000.0);
        END IF;
    END LOOP;
    
    -- Reset batch count for migration phase
    v_batch_count := 0;
    
    -- Migrate recent data in batches
    RAISE NOTICE 'Phase 2: Migrating recent data (from %)', p_cutoff_date;
    
    LOOP
        v_batch_start_time := clock_timestamp();
        
        -- Execute migration batch
        EXECUTE v_migrate_sql USING p_cutoff_date, p_batch_size INTO v_batch_migrated;
        
        v_total_migrated := v_total_migrated + v_batch_migrated;
        v_batch_count := v_batch_count + 1;
        
        RAISE NOTICE 'Migration batch %: % rows (%.2f ms) - Total: %', 
                     v_batch_count, v_batch_migrated,
                     EXTRACT(MILLISECONDS FROM clock_timestamp() - v_batch_start_time),
                     v_total_migrated;
        
        -- Exit conditions
        EXIT WHEN v_batch_migrated = 0;
        EXIT WHEN v_batch_count >= p_max_batches;
        
        -- Small delay to reduce system load
        IF p_sleep_ms > 0 THEN
            PERFORM pg_sleep(p_sleep_ms / 1000.0);
        END IF;
    END LOOP;
    
    -- Build result JSON
    v_result := json_build_object(
        'status', 'completed',
        'start_time', v_start_time,
        'end_time', NOW(),
        'duration_seconds', EXTRACT(EPOCH FROM (NOW() - v_start_time)),
        'total_archived', v_total_archived,
        'total_migrated', v_total_migrated,
        'cutoff_date', p_cutoff_date,
        'batch_size', p_batch_size,
        'source_table', p_source_table,
        'target_table', p_target_table,
        'archive_table', p_archive_table
    );
    
    RAISE NOTICE 'Batch migration completed: % archived, % migrated in % seconds',
                 v_total_archived, v_total_migrated, 
                 EXTRACT(EPOCH FROM (NOW() - v_start_time));
    
    RETURN v_result;
    
EXCEPTION WHEN OTHERS THEN
    v_result := json_build_object(
        'status', 'failed',
        'error', SQLERRM,
        'start_time', v_start_time,
        'end_time', NOW(),
        'total_archived', v_total_archived,
        'total_migrated', v_total_migrated
    );
    
    RAISE NOTICE 'Batch migration failed: %', SQLERRM;
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- HELPER FUNCTION: Calculate optimal cutoff date
-- =====================================================

CREATE OR REPLACE FUNCTION calculate_cutoff_date(
    p_table_name TEXT,
    p_date_column TEXT DEFAULT 'created_date',
    p_retention_percentage NUMERIC DEFAULT 0.1
)
RETURNS TIMESTAMP AS $$
DECLARE
    v_cutoff_date TIMESTAMP;
    v_total_rows BIGINT;
    v_target_rows BIGINT;
    v_sql TEXT;
BEGIN
    -- Get total row count
    v_sql := format('SELECT COUNT(*) FROM %I', p_table_name);
    EXECUTE v_sql INTO v_total_rows;
    
    -- Calculate target rows (recent X%)
    v_target_rows := FLOOR(v_total_rows * p_retention_percentage);
    
    -- Get cutoff date
    v_sql := format(
        'SELECT %I FROM %I ORDER BY %I DESC LIMIT 1 OFFSET %s',
        p_date_column, p_table_name, p_date_column, v_target_rows
    );
    EXECUTE v_sql INTO v_cutoff_date;
    
    RAISE NOTICE 'Cutoff calculation: total_rows=%, retention=%%, target_rows=%, cutoff_date=%',
                 v_total_rows, p_retention_percentage * 100, v_target_rows, v_cutoff_date;
    
    RETURN v_cutoff_date;
END;
$$ LANGUAGE plpgsql;

/*
-- =====================================================
-- USAGE EXAMPLE:
-- =====================================================

-- 1. Calculate cutoff date (keep last 10% of data)
SELECT calculate_cutoff_date('orders', 'created_date', 0.1);

-- 2. Execute batch migration
SELECT batch_migrate_data(
    'orders',                           -- source table
    'orders_new',                       -- target table
    'orders_archive',                   -- archive table
    '2024-01-01 00:00:00'::timestamp,   -- cutoff date (from step 1)
    'created_date',                     -- date column
    5000,                               -- batch size
    50,                                 -- sleep ms between batches
    2000                                -- max batches safety limit
);

-- Monitor progress in real-time via NOTICE messages
*/