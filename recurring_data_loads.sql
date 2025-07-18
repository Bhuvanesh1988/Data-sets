-- =====================================================
-- PL/pgSQL Code for Recurring Data Loads
-- =====================================================

-- 1. Basic Incremental Data Load Function
-- =====================================================
CREATE OR REPLACE FUNCTION load_incremental_data()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    last_load_time TIMESTAMP;
    rows_processed INTEGER := 0;
    error_msg TEXT;
BEGIN
    -- Get the last successful load timestamp
    SELECT COALESCE(MAX(last_updated), '1900-01-01'::timestamp)
    INTO last_load_time
    FROM data_load_log
    WHERE load_type = 'incremental' AND status = 'success';
    
    -- Log the start of the load
    INSERT INTO data_load_log (load_type, status, start_time, last_updated)
    VALUES ('incremental', 'running', NOW(), last_load_time);
    
    -- Perform the incremental load
    INSERT INTO target_table (id, name, data, created_at, updated_at)
    SELECT 
        s.id,
        s.name,
        s.data,
        s.created_at,
        NOW()
    FROM source_table s
    WHERE s.updated_at > last_load_time
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        data = EXCLUDED.data,
        updated_at = EXCLUDED.updated_at;
    
    GET DIAGNOSTICS rows_processed = ROW_COUNT;
    
    -- Update the log with success
    UPDATE data_load_log 
    SET status = 'success', 
        end_time = NOW(), 
        rows_processed = rows_processed,
        last_updated = NOW()
    WHERE load_type = 'incremental' 
    AND status = 'running';
    
    RETURN format('Incremental load completed successfully. Rows processed: %s', rows_processed);
    
EXCEPTION
    WHEN OTHERS THEN
        error_msg := SQLERRM;
        
        -- Log the error
        UPDATE data_load_log 
        SET status = 'failed', 
            end_time = NOW(), 
            error_message = error_msg
        WHERE load_type = 'incremental' 
        AND status = 'running';
        
        RAISE EXCEPTION 'Incremental load failed: %', error_msg;
END;
$$;

-- 2. Full Refresh Data Load Function
-- =====================================================
CREATE OR REPLACE FUNCTION load_full_refresh_data()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    rows_processed INTEGER := 0;
    backup_table_name TEXT;
    error_msg TEXT;
BEGIN
    -- Create backup table name with timestamp
    backup_table_name := 'target_table_backup_' || to_char(NOW(), 'YYYYMMDDHH24MISS');
    
    -- Log the start of the load
    INSERT INTO data_load_log (load_type, status, start_time)
    VALUES ('full_refresh', 'running', NOW());
    
    -- Create backup of existing data
    EXECUTE format('CREATE TABLE %I AS SELECT * FROM target_table', backup_table_name);
    
    -- Truncate target table
    TRUNCATE TABLE target_table;
    
    -- Load all data from source
    INSERT INTO target_table (id, name, data, created_at, updated_at)
    SELECT 
        id,
        name,
        data,
        created_at,
        NOW()
    FROM source_table;
    
    GET DIAGNOSTICS rows_processed = ROW_COUNT;
    
    -- Drop backup table if load was successful
    EXECUTE format('DROP TABLE %I', backup_table_name);
    
    -- Update the log with success
    UPDATE data_load_log 
    SET status = 'success', 
        end_time = NOW(), 
        rows_processed = rows_processed
    WHERE load_type = 'full_refresh' 
    AND status = 'running';
    
    RETURN format('Full refresh completed successfully. Rows processed: %s', rows_processed);
    
EXCEPTION
    WHEN OTHERS THEN
        error_msg := SQLERRM;
        
        -- Restore from backup if it exists
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = backup_table_name) THEN
            TRUNCATE TABLE target_table;
            EXECUTE format('INSERT INTO target_table SELECT * FROM %I', backup_table_name);
            EXECUTE format('DROP TABLE %I', backup_table_name);
        END IF;
        
        -- Log the error
        UPDATE data_load_log 
        SET status = 'failed', 
            end_time = NOW(), 
            error_message = error_msg
        WHERE load_type = 'full_refresh' 
        AND status = 'running';
        
        RAISE EXCEPTION 'Full refresh failed: %. Data restored from backup.', error_msg;
END;
$$;

-- 3. Delta/Change Data Capture Load Function
-- =====================================================
CREATE OR REPLACE FUNCTION load_delta_data()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    last_load_time TIMESTAMP;
    rows_inserted INTEGER := 0;
    rows_updated INTEGER := 0;
    rows_deleted INTEGER := 0;
    error_msg TEXT;
BEGIN
    -- Get the last successful load timestamp
    SELECT COALESCE(MAX(last_updated), '1900-01-01'::timestamp)
    INTO last_load_time
    FROM data_load_log
    WHERE load_type = 'delta' AND status = 'success';
    
    -- Log the start of the load
    INSERT INTO data_load_log (load_type, status, start_time, last_updated)
    VALUES ('delta', 'running', NOW(), last_load_time);
    
    -- Handle INSERTS
    INSERT INTO target_table (id, name, data, created_at, updated_at)
    SELECT 
        s.id,
        s.name,
        s.data,
        s.created_at,
        NOW()
    FROM source_table s
    WHERE s.operation = 'INSERT' 
    AND s.updated_at > last_load_time
    ON CONFLICT (id) DO NOTHING;
    
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    
    -- Handle UPDATES
    UPDATE target_table t
    SET 
        name = s.name,
        data = s.data,
        updated_at = NOW()
    FROM source_table s
    WHERE t.id = s.id
    AND s.operation = 'UPDATE'
    AND s.updated_at > last_load_time;
    
    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    
    -- Handle DELETES
    DELETE FROM target_table t
    USING source_table s
    WHERE t.id = s.id
    AND s.operation = 'DELETE'
    AND s.updated_at > last_load_time;
    
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    
    -- Update the log with success
    UPDATE data_load_log 
    SET status = 'success', 
        end_time = NOW(), 
        rows_processed = rows_inserted + rows_updated + rows_deleted,
        last_updated = NOW(),
        notes = format('Inserted: %s, Updated: %s, Deleted: %s', 
                      rows_inserted, rows_updated, rows_deleted)
    WHERE load_type = 'delta' 
    AND status = 'running';
    
    RETURN format('Delta load completed. Inserted: %s, Updated: %s, Deleted: %s', 
                  rows_inserted, rows_updated, rows_deleted);
    
EXCEPTION
    WHEN OTHERS THEN
        error_msg := SQLERRM;
        
        -- Log the error
        UPDATE data_load_log 
        SET status = 'failed', 
            end_time = NOW(), 
            error_message = error_msg
        WHERE load_type = 'delta' 
        AND status = 'running';
        
        RAISE EXCEPTION 'Delta load failed: %', error_msg;
END;
$$;

-- 4. Batch Processing Function with Configurable Batch Size
-- =====================================================
CREATE OR REPLACE FUNCTION load_data_in_batches(batch_size INTEGER DEFAULT 1000)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    total_rows INTEGER := 0;
    current_batch INTEGER := 0;
    rows_processed INTEGER := 0;
    batch_count INTEGER := 0;
    error_msg TEXT;
    last_id INTEGER := 0;
BEGIN
    -- Log the start of the load
    INSERT INTO data_load_log (load_type, status, start_time)
    VALUES ('batch_load', 'running', NOW());
    
    -- Get total count for progress tracking
    SELECT COUNT(*) INTO total_rows FROM source_table WHERE processed = FALSE;
    
    LOOP
        -- Process batch
        WITH batch_data AS (
            SELECT id, name, data, created_at
            FROM source_table 
            WHERE processed = FALSE 
            AND id > last_id
            ORDER BY id
            LIMIT batch_size
        )
        INSERT INTO target_table (id, name, data, created_at, updated_at)
        SELECT id, name, data, created_at, NOW()
        FROM batch_data
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            data = EXCLUDED.data,
            updated_at = EXCLUDED.updated_at;
        
        GET DIAGNOSTICS current_batch = ROW_COUNT;
        
        -- Update processed flag
        UPDATE source_table 
        SET processed = TRUE 
        WHERE id IN (
            SELECT id FROM source_table 
            WHERE processed = FALSE 
            AND id > last_id
            ORDER BY id
            LIMIT batch_size
        );
        
        -- Update last_id for next batch
        SELECT MAX(id) INTO last_id 
        FROM source_table 
        WHERE processed = TRUE;
        
        rows_processed := rows_processed + current_batch;
        batch_count := batch_count + 1;
        
        -- Log progress every 10 batches
        IF batch_count % 10 = 0 THEN
            RAISE NOTICE 'Processed % batches, % rows out of % total rows', 
                         batch_count, rows_processed, total_rows;
        END IF;
        
        -- Exit if no more rows to process
        EXIT WHEN current_batch = 0;
        
        -- Commit every batch (if in a transaction)
        COMMIT;
        
    END LOOP;
    
    -- Update the log with success
    UPDATE data_load_log 
    SET status = 'success', 
        end_time = NOW(), 
        rows_processed = rows_processed,
        notes = format('Processed in %s batches of size %s', batch_count, batch_size)
    WHERE load_type = 'batch_load' 
    AND status = 'running';
    
    RETURN format('Batch load completed. Processed %s rows in %s batches', 
                  rows_processed, batch_count);
    
EXCEPTION
    WHEN OTHERS THEN
        error_msg := SQLERRM;
        
        -- Log the error
        UPDATE data_load_log 
        SET status = 'failed', 
            end_time = NOW(), 
            error_message = error_msg
        WHERE load_type = 'batch_load' 
        AND status = 'running';
        
        RAISE EXCEPTION 'Batch load failed at batch %: %', batch_count, error_msg;
END;
$$;

-- 5. Main Orchestrator Function
-- =====================================================
CREATE OR REPLACE FUNCTION orchestrate_data_load(
    load_type TEXT DEFAULT 'incremental',
    batch_size INTEGER DEFAULT 1000
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    result_msg TEXT;
    start_time TIMESTAMP := NOW();
    end_time TIMESTAMP;
    duration INTERVAL;
BEGIN
    RAISE NOTICE 'Starting % data load at %', load_type, start_time;
    
    CASE load_type
        WHEN 'incremental' THEN
            result_msg := load_incremental_data();
        WHEN 'full_refresh' THEN
            result_msg := load_full_refresh_data();
        WHEN 'delta' THEN
            result_msg := load_delta_data();
        WHEN 'batch' THEN
            result_msg := load_data_in_batches(batch_size);
        ELSE
            RAISE EXCEPTION 'Invalid load_type: %. Valid options: incremental, full_refresh, delta, batch', load_type;
    END CASE;
    
    end_time := NOW();
    duration := end_time - start_time;
    
    RAISE NOTICE 'Data load completed in %', duration;
    
    RETURN format('%s. Duration: %s', result_msg, duration);
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Orchestration failed: %', SQLERRM;
END;
$$;

-- 6. Data Quality Validation Function
-- =====================================================
CREATE OR REPLACE FUNCTION validate_data_quality()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    validation_errors TEXT[] := ARRAY[]::TEXT[];
    null_count INTEGER;
    duplicate_count INTEGER;
    invalid_data_count INTEGER;
    error_msg TEXT;
BEGIN
    -- Check for NULL values in required fields
    SELECT COUNT(*) INTO null_count
    FROM target_table
    WHERE name IS NULL OR data IS NULL;
    
    IF null_count > 0 THEN
        validation_errors := array_append(validation_errors, 
                                        format('Found %s rows with NULL values in required fields', null_count));
    END IF;
    
    -- Check for duplicates
    SELECT COUNT(*) - COUNT(DISTINCT id) INTO duplicate_count
    FROM target_table;
    
    IF duplicate_count > 0 THEN
        validation_errors := array_append(validation_errors, 
                                        format('Found %s duplicate records', duplicate_count));
    END IF;
    
    -- Check for invalid data patterns (example: email validation)
    SELECT COUNT(*) INTO invalid_data_count
    FROM target_table
    WHERE data LIKE '%email%' 
    AND NOT (data ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
    
    IF invalid_data_count > 0 THEN
        validation_errors := array_append(validation_errors, 
                                        format('Found %s rows with invalid email format', invalid_data_count));
    END IF;
    
    -- Log validation results
    INSERT INTO data_validation_log (validation_date, errors, status)
    VALUES (NOW(), validation_errors, 
            CASE WHEN array_length(validation_errors, 1) > 0 THEN 'failed' ELSE 'passed' END);
    
    IF array_length(validation_errors, 1) > 0 THEN
        error_msg := array_to_string(validation_errors, '; ');
        RAISE EXCEPTION 'Data validation failed: %', error_msg;
    END IF;
    
    RETURN 'Data validation passed successfully';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Data validation error: %', SQLERRM;
END;
$$;

-- 7. Scheduled Load Function with Retry Logic
-- =====================================================
CREATE OR REPLACE FUNCTION scheduled_load_with_retry(
    load_type TEXT DEFAULT 'incremental',
    max_retries INTEGER DEFAULT 3,
    retry_delay_seconds INTEGER DEFAULT 60
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    attempt INTEGER := 1;
    result_msg TEXT;
    error_msg TEXT;
BEGIN
    WHILE attempt <= max_retries LOOP
        BEGIN
            -- Attempt the load
            result_msg := orchestrate_data_load(load_type);
            
            -- Validate data quality
            PERFORM validate_data_quality();
            
            -- If we get here, the load was successful
            RETURN format('Load completed successfully on attempt %s. %s', attempt, result_msg);
            
        EXCEPTION
            WHEN OTHERS THEN
                error_msg := SQLERRM;
                
                IF attempt = max_retries THEN
                    -- Final attempt failed, log and raise
                    INSERT INTO data_load_log (load_type, status, start_time, end_time, error_message)
                    VALUES (load_type || '_retry', 'failed', NOW(), NOW(), 
                           format('All %s retry attempts failed. Last error: %s', max_retries, error_msg));
                    
                    RAISE EXCEPTION 'Load failed after % attempts. Last error: %', max_retries, error_msg;
                ELSE
                    -- Log the retry attempt
                    INSERT INTO data_load_log (load_type, status, start_time, end_time, error_message)
                    VALUES (load_type || '_retry', 'retry', NOW(), NOW(), 
                           format('Attempt %s failed: %s. Retrying in %s seconds', 
                                  attempt, error_msg, retry_delay_seconds));
                    
                    -- Wait before retry
                    PERFORM pg_sleep(retry_delay_seconds);
                    attempt := attempt + 1;
                END IF;
        END;
    END LOOP;
    
    RETURN 'Unexpected end of retry loop';
END;
$$;

-- =====================================================
-- Supporting Tables (Create these first)
-- =====================================================

-- Data load logging table
CREATE TABLE IF NOT EXISTS data_load_log (
    id SERIAL PRIMARY KEY,
    load_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    last_updated TIMESTAMP,
    rows_processed INTEGER,
    error_message TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Data validation logging table
CREATE TABLE IF NOT EXISTS data_validation_log (
    id SERIAL PRIMARY KEY,
    validation_date TIMESTAMP NOT NULL,
    errors TEXT[],
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Example source and target tables
CREATE TABLE IF NOT EXISTS source_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    data TEXT,
    operation VARCHAR(10), -- For delta loads: INSERT, UPDATE, DELETE
    processed BOOLEAN DEFAULT FALSE, -- For batch processing
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS target_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    data TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- =====================================================
-- Usage Examples
-- =====================================================

/*
-- Basic incremental load
SELECT load_incremental_data();

-- Full refresh
SELECT load_full_refresh_data();

-- Delta/CDC load
SELECT load_delta_data();

-- Batch processing with custom batch size
SELECT load_data_in_batches(5000);

-- Orchestrated load with error handling
SELECT orchestrate_data_load('incremental');

-- Scheduled load with retry logic
SELECT scheduled_load_with_retry('incremental', 3, 30);

-- Data quality validation
SELECT validate_data_quality();

-- Check load history
SELECT * FROM data_load_log ORDER BY start_time DESC LIMIT 10;

-- Check validation history
SELECT * FROM data_validation_log ORDER BY validation_date DESC LIMIT 10;
*/

-- =====================================================
-- Cron Job Setup (PostgreSQL pg_cron extension)
-- =====================================================

/*
-- Enable pg_cron extension (requires superuser)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Schedule incremental load every hour
SELECT cron.schedule('hourly-incremental-load', '0 * * * *', 
                     'SELECT scheduled_load_with_retry(''incremental'', 3, 60);');

-- Schedule full refresh daily at 2 AM
SELECT cron.schedule('daily-full-refresh', '0 2 * * *', 
                     'SELECT scheduled_load_with_retry(''full_refresh'', 2, 300);');

-- Schedule data validation every 6 hours
SELECT cron.schedule('data-validation', '0 */6 * * *', 
                     'SELECT validate_data_quality();');

-- View scheduled jobs
SELECT * FROM cron.job;

-- Remove a scheduled job
SELECT cron.unschedule('job-name');
*/