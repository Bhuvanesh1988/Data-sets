-- =====================================================
-- BATCH JOB EXECUTION FRAMEWORK
-- =====================================================
-- Handles large data migrations in manageable batches
-- with progress tracking, error handling, and resumability

-- =====================================================
-- BATCH INFRASTRUCTURE SETUP
-- =====================================================

-- Batch job control table
CREATE TABLE IF NOT EXISTS batch_job_control (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) UNIQUE,
    table_name VARCHAR(255),
    operation VARCHAR(50),
    batch_size INTEGER DEFAULT 10000,
    total_rows BIGINT,
    processed_rows BIGINT DEFAULT 0,
    current_batch INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, RUNNING, PAUSED, COMPLETED, FAILED
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    last_processed_id BIGINT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Batch execution log for detailed tracking
CREATE TABLE IF NOT EXISTS batch_execution_log (
    log_id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES batch_job_control(job_id),
    batch_number INTEGER,
    batch_start_id BIGINT,
    batch_end_id BIGINT,
    rows_processed INTEGER,
    execution_time INTERVAL,
    status VARCHAR(20),
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- =====================================================
-- BATCH EXECUTION FUNCTIONS
-- =====================================================

-- Function to create a new batch job
CREATE OR REPLACE FUNCTION create_batch_job(
    p_job_name VARCHAR(255),
    p_table_name VARCHAR(255),
    p_operation VARCHAR(50),
    p_batch_size INTEGER DEFAULT 10000
)
RETURNS INTEGER AS $$
DECLARE
    job_id INTEGER;
    total_count BIGINT;
BEGIN
    -- Get total row count
    EXECUTE format('SELECT COUNT(*) FROM %I', p_table_name) INTO total_count;
    
    -- Insert job record
    INSERT INTO batch_job_control (job_name, table_name, operation, batch_size, total_rows)
    VALUES (p_job_name, p_table_name, p_operation, p_batch_size, total_count)
    RETURNING job_id INTO job_id;
    
    RAISE NOTICE 'Created batch job % for table % with % total rows', job_id, p_table_name, total_count;
    RETURN job_id;
END;
$$ LANGUAGE plpgsql;

-- Function to execute batch migration with archiving
CREATE OR REPLACE FUNCTION execute_batch_archive(
    p_job_id INTEGER,
    p_cutoff_date TIMESTAMP,
    p_archive_table VARCHAR(255),
    p_date_column VARCHAR(255) DEFAULT 'created_date'
)
RETURNS BOOLEAN AS $$
DECLARE
    job_rec RECORD;
    batch_count INTEGER := 0;
    rows_in_batch INTEGER;
    total_processed BIGINT := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    last_id BIGINT := 0;
    max_id BIGINT;
BEGIN
    -- Get job details
    SELECT * INTO job_rec FROM batch_job_control WHERE job_id = p_job_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job ID % not found', p_job_id;
    END IF;
    
    -- Update job status to RUNNING
    UPDATE batch_job_control 
    SET status = 'RUNNING', start_time = NOW(), updated_at = NOW()
    WHERE job_id = p_job_id;
    
    -- Get max ID to process
    EXECUTE format('SELECT COALESCE(MAX(id), 0) FROM %I WHERE %I < $1', 
                   job_rec.table_name, p_date_column) 
    USING p_cutoff_date INTO max_id;
    
    RAISE NOTICE 'Starting batch archive for job %. Max ID to process: %', p_job_id, max_id;
    
    -- Resume from last processed ID if job was interrupted
    last_id := COALESCE(job_rec.last_processed_id, 0);
    
    -- Process in batches
    WHILE last_id < max_id LOOP
        batch_count := batch_count + 1;
        start_time := NOW();
        
        BEGIN
            -- Archive batch of data
            EXECUTE format('
                WITH batch_data AS (
                    SELECT * FROM %I 
                    WHERE id > $1 AND id <= $2 AND %I < $3
                    ORDER BY id
                    LIMIT $4
                )
                INSERT INTO %I 
                SELECT *, NOW(), ''BATCH_ARCHIVE'', $5
                FROM batch_data',
                job_rec.table_name, p_date_column, p_archive_table
            ) USING last_id, last_id + job_rec.batch_size, p_cutoff_date, job_rec.batch_size, batch_count;
            
            GET DIAGNOSTICS rows_in_batch = ROW_COUNT;
            end_time := NOW();
            
            -- Update counters
            total_processed := total_processed + rows_in_batch;
            last_id := last_id + job_rec.batch_size;
            
            -- Log batch execution
            INSERT INTO batch_execution_log 
            (job_id, batch_number, batch_start_id, batch_end_id, rows_processed, execution_time, status)
            VALUES (p_job_id, batch_count, last_id - job_rec.batch_size + 1, last_id, 
                    rows_in_batch, end_time - start_time, 'COMPLETED');
            
            -- Update job progress
            UPDATE batch_job_control 
            SET processed_rows = total_processed, 
                current_batch = batch_count,
                last_processed_id = last_id,
                updated_at = NOW()
            WHERE job_id = p_job_id;
            
            RAISE NOTICE 'Batch %: Processed % rows (Total: %) in %', 
                         batch_count, rows_in_batch, total_processed, end_time - start_time;
            
            -- Exit if no more rows in this batch
            EXIT WHEN rows_in_batch = 0;
            
            -- Small delay to prevent overwhelming the system
            PERFORM pg_sleep(0.1);
            
        EXCEPTION WHEN OTHERS THEN
            -- Log error
            INSERT INTO batch_execution_log 
            (job_id, batch_number, batch_start_id, batch_end_id, rows_processed, execution_time, status, error_message)
            VALUES (p_job_id, batch_count, last_id - job_rec.batch_size + 1, last_id, 
                    0, NOW() - start_time, 'FAILED', SQLERRM);
            
            -- Update job status
            UPDATE batch_job_control 
            SET status = 'FAILED', error_message = SQLERRM, updated_at = NOW()
            WHERE job_id = p_job_id;
            
            RAISE EXCEPTION 'Batch % failed: %', batch_count, SQLERRM;
        END;
        
    END LOOP;
    
    -- Mark job as completed
    UPDATE batch_job_control 
    SET status = 'COMPLETED', end_time = NOW(), updated_at = NOW()
    WHERE job_id = p_job_id;
    
    RAISE NOTICE 'Batch job % completed. Total processed: % rows in % batches', 
                 p_job_id, total_processed, batch_count;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Function to execute batch data migration to new table
CREATE OR REPLACE FUNCTION execute_batch_migration(
    p_job_id INTEGER,
    p_source_table VARCHAR(255),
    p_target_table VARCHAR(255),
    p_cutoff_date TIMESTAMP,
    p_date_column VARCHAR(255) DEFAULT 'created_date',
    p_column_mapping TEXT DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    job_rec RECORD;
    batch_count INTEGER := 0;
    rows_in_batch INTEGER;
    total_processed BIGINT := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    last_id BIGINT := 0;
    max_id BIGINT;
    migration_sql TEXT;
BEGIN
    -- Get job details
    SELECT * INTO job_rec FROM batch_job_control WHERE job_id = p_job_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job ID % not found', p_job_id;
    END IF;
    
    -- Default column mapping if not provided
    IF p_column_mapping IS NULL THEN
        migration_sql := format('
            INSERT INTO %I (original_id, data_column1, data_column2, business_date, created_at, source_system)
            SELECT id, data_column1, data_column2, business_date, %I, ''BATCH_MIGRATION''
            FROM %I WHERE id > $1 AND id <= $2 AND %I >= $3
            ORDER BY id LIMIT $4',
            p_target_table, p_date_column, p_source_table, p_date_column);
    ELSE
        migration_sql := p_column_mapping;
    END IF;
    
    -- Update job status
    UPDATE batch_job_control 
    SET status = 'RUNNING', start_time = NOW(), updated_at = NOW()
    WHERE job_id = p_job_id;
    
    -- Get max ID to process
    EXECUTE format('SELECT COALESCE(MAX(id), 0) FROM %I WHERE %I >= $1', 
                   p_source_table, p_date_column) 
    USING p_cutoff_date INTO max_id;
    
    RAISE NOTICE 'Starting batch migration for job %. Max ID to process: %', p_job_id, max_id;
    
    -- Resume from last processed ID if job was interrupted
    last_id := COALESCE(job_rec.last_processed_id, 0);
    
    -- Process in batches
    WHILE last_id < max_id LOOP
        batch_count := batch_count + 1;
        start_time := NOW();
        
        BEGIN
            -- Migrate batch of data
            EXECUTE migration_sql 
            USING last_id, last_id + job_rec.batch_size, p_cutoff_date, job_rec.batch_size;
            
            GET DIAGNOSTICS rows_in_batch = ROW_COUNT;
            end_time := NOW();
            
            -- Update counters
            total_processed := total_processed + rows_in_batch;
            last_id := last_id + job_rec.batch_size;
            
            -- Log batch execution
            INSERT INTO batch_execution_log 
            (job_id, batch_number, batch_start_id, batch_end_id, rows_processed, execution_time, status)
            VALUES (p_job_id, batch_count, last_id - job_rec.batch_size + 1, last_id, 
                    rows_in_batch, end_time - start_time, 'COMPLETED');
            
            -- Update job progress
            UPDATE batch_job_control 
            SET processed_rows = total_processed, 
                current_batch = batch_count,
                last_processed_id = last_id,
                updated_at = NOW()
            WHERE job_id = p_job_id;
            
            RAISE NOTICE 'Batch %: Migrated % rows (Total: %) in %', 
                         batch_count, rows_in_batch, total_processed, end_time - start_time;
            
            -- Exit if no more rows in this batch
            EXIT WHEN rows_in_batch = 0;
            
            -- Small delay
            PERFORM pg_sleep(0.1);
            
        EXCEPTION WHEN OTHERS THEN
            -- Log error and fail the job
            INSERT INTO batch_execution_log 
            (job_id, batch_number, batch_start_id, batch_end_id, rows_processed, execution_time, status, error_message)
            VALUES (p_job_id, batch_count, last_id - job_rec.batch_size + 1, last_id, 
                    0, NOW() - start_time, 'FAILED', SQLERRM);
            
            UPDATE batch_job_control 
            SET status = 'FAILED', error_message = SQLERRM, updated_at = NOW()
            WHERE job_id = p_job_id;
            
            RAISE EXCEPTION 'Batch % failed: %', batch_count, SQLERRM;
        END;
        
    END LOOP;
    
    -- Mark job as completed
    UPDATE batch_job_control 
    SET status = 'COMPLETED', end_time = NOW(), updated_at = NOW()
    WHERE job_id = p_job_id;
    
    RAISE NOTICE 'Batch migration job % completed. Total processed: % rows in % batches', 
                 p_job_id, total_processed, batch_count;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Function to pause a running batch job
CREATE OR REPLACE FUNCTION pause_batch_job(p_job_id INTEGER)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE batch_job_control 
    SET status = 'PAUSED', updated_at = NOW()
    WHERE job_id = p_job_id AND status = 'RUNNING';
    
    IF FOUND THEN
        RAISE NOTICE 'Batch job % paused', p_job_id;
        RETURN TRUE;
    ELSE
        RAISE NOTICE 'Job % not found or not running', p_job_id;
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to resume a paused batch job
CREATE OR REPLACE FUNCTION resume_batch_job(p_job_id INTEGER)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE batch_job_control 
    SET status = 'RUNNING', updated_at = NOW()
    WHERE job_id = p_job_id AND status = 'PAUSED';
    
    IF FOUND THEN
        RAISE NOTICE 'Batch job % resumed', p_job_id;
        RETURN TRUE;
    ELSE
        RAISE NOTICE 'Job % not found or not paused', p_job_id;
        RETURN FALSE;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to get batch job progress
CREATE OR REPLACE FUNCTION get_batch_progress(p_job_id INTEGER)
RETURNS TABLE (
    job_name VARCHAR(255),
    table_name VARCHAR(255),
    status VARCHAR(20),
    progress_percentage NUMERIC(5,2),
    current_batch INTEGER,
    total_rows BIGINT,
    processed_rows BIGINT,
    estimated_time_remaining INTERVAL,
    last_batch_duration INTERVAL
) AS $$
DECLARE
    avg_batch_time INTERVAL;
    remaining_rows BIGINT;
BEGIN
    -- Calculate average batch time
    SELECT AVG(execution_time) INTO avg_batch_time
    FROM batch_execution_log 
    WHERE job_id = p_job_id AND status = 'COMPLETED';
    
    RETURN QUERY
    SELECT 
        bjc.job_name,
        bjc.table_name,
        bjc.status,
        CASE WHEN bjc.total_rows > 0 
             THEN ROUND((bjc.processed_rows::NUMERIC / bjc.total_rows::NUMERIC) * 100, 2)
             ELSE 0 END as progress_percentage,
        bjc.current_batch,
        bjc.total_rows,
        bjc.processed_rows,
        CASE WHEN avg_batch_time IS NOT NULL AND bjc.total_rows > bjc.processed_rows
             THEN avg_batch_time * CEIL((bjc.total_rows - bjc.processed_rows)::NUMERIC / bjc.batch_size::NUMERIC)
             ELSE NULL END as estimated_time_remaining,
        (SELECT execution_time FROM batch_execution_log 
         WHERE job_id = p_job_id ORDER BY timestamp DESC LIMIT 1) as last_batch_duration
    FROM batch_job_control bjc
    WHERE bjc.job_id = p_job_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- MONITORING AND REPORTING FUNCTIONS
-- =====================================================

-- View for monitoring all batch jobs
CREATE OR REPLACE VIEW batch_jobs_status AS
SELECT 
    bjc.job_id,
    bjc.job_name,
    bjc.table_name,
    bjc.operation,
    bjc.status,
    bjc.current_batch,
    CASE WHEN bjc.total_rows > 0 
         THEN ROUND((bjc.processed_rows::NUMERIC / bjc.total_rows::NUMERIC) * 100, 2)
         ELSE 0 END as progress_percentage,
    bjc.processed_rows,
    bjc.total_rows,
    bjc.start_time,
    bjc.updated_at,
    CASE WHEN bjc.status = 'RUNNING' 
         THEN NOW() - bjc.start_time 
         ELSE bjc.end_time - bjc.start_time END as duration
FROM batch_job_control bjc
ORDER BY bjc.job_id DESC;

-- Function to clean up old batch logs
CREATE OR REPLACE FUNCTION cleanup_batch_logs(p_days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM batch_execution_log 
    WHERE timestamp < NOW() - (p_days_to_keep || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    DELETE FROM batch_job_control 
    WHERE status IN ('COMPLETED', 'FAILED') 
    AND updated_at < NOW() - (p_days_to_keep || ' days')::INTERVAL;
    
    RAISE NOTICE 'Cleaned up % old batch log entries', deleted_count;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================

/*
-- Example: Create and execute a batch archive job
SELECT create_batch_job('archive_old_orders', 'orders', 'ARCHIVE', 5000);

-- Execute batch archiving
SELECT execute_batch_archive(
    1,  -- job_id
    '2023-01-01'::TIMESTAMP,  -- cutoff_date
    'orders_archive',  -- archive_table
    'created_date'  -- date_column
);

-- Monitor progress
SELECT * FROM get_batch_progress(1);

-- View all jobs status
SELECT * FROM batch_jobs_status;

-- Pause/Resume if needed
SELECT pause_batch_job(1);
SELECT resume_batch_job(1);
*/