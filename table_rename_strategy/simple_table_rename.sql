-- =====================================================
-- SIMPLIFIED TABLE RENAME STRATEGY
-- =====================================================
-- For specific known tables with minimal monitoring overhead
-- Reduces complexity from 12+ steps to 3 simple steps

-- =====================================================
-- STEP 1: SIMPLE SETUP (One-time)
-- =====================================================

-- Basic audit table (optional, can be skipped if not needed)
CREATE TABLE IF NOT EXISTS table_rename_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    operation VARCHAR(50),
    timestamp TIMESTAMP DEFAULT NOW(),
    rows_affected BIGINT,
    notes TEXT
);

-- Simple function to determine cutoff date (keep last 10% of data)
CREATE OR REPLACE FUNCTION get_cutoff_date(
    table_name TEXT, 
    date_column TEXT DEFAULT 'created_date'
)
RETURNS TIMESTAMP AS $$
DECLARE
    cutoff_date TIMESTAMP;
    total_rows BIGINT;
BEGIN
    -- Get total row count
    EXECUTE format('SELECT COUNT(*) FROM %I', table_name) INTO total_rows;
    
    -- Get cutoff date for last 10% of data
    EXECUTE format(
        'SELECT %I FROM %I ORDER BY %I DESC LIMIT 1 OFFSET %s',
        date_column, table_name, date_column, FLOOR(total_rows * 0.1)
    ) INTO cutoff_date;
    
    RETURN cutoff_date;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- STEP 2: EXECUTE MIGRATION (For each table)
-- =====================================================

-- Replace 'your_table_name' with your actual table name
DO $$
DECLARE
    table_name TEXT := 'your_table_name';  -- CHANGE THIS
    new_table TEXT := 'your_new_table';    -- CHANGE THIS  
    archive_table TEXT := 'your_archive_table'; -- CHANGE THIS
    cutoff_date TIMESTAMP;
    archived_count BIGINT;
    migrated_count BIGINT;
BEGIN
    -- Get cutoff date
    cutoff_date := get_cutoff_date(table_name, 'created_date'); -- CHANGE date column if needed
    
    RAISE NOTICE 'Starting migration for % with cutoff date %', table_name, cutoff_date;
    
    -- Create archive table
    EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING ALL)', archive_table, table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN archived_at TIMESTAMP DEFAULT NOW()', archive_table);
    
    -- Archive old data (older than cutoff)
    EXECUTE format(
        'INSERT INTO %I SELECT *, NOW() FROM %I WHERE created_date < $1', 
        archive_table, table_name
    ) USING cutoff_date;
    GET DIAGNOSTICS archived_count = ROW_COUNT;
    
    -- Create new table structure (customize as needed)
    EXECUTE format('
        CREATE TABLE %I (
            id BIGSERIAL PRIMARY KEY,
            original_id BIGINT,
            -- Add your actual columns here, example:
            data_column1 VARCHAR(255),
            data_column2 INTEGER,
            business_date DATE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )', new_table);
    
    -- Migrate recent data to new table
    EXECUTE format(
        'INSERT INTO %I (original_id, data_column1, data_column2, business_date, created_at)
         SELECT id, data_column1, data_column2, business_date, created_date 
         FROM %I WHERE created_date >= $1',
        new_table, table_name
    ) USING cutoff_date;
    GET DIAGNOSTICS migrated_count = ROW_COUNT;
    
    -- Log results
    INSERT INTO table_rename_log (table_name, operation, rows_affected, notes)
    VALUES (table_name, 'MIGRATION_COMPLETE', archived_count + migrated_count, 
            format('Archived: %s, Migrated: %s, Cutoff: %s', archived_count, migrated_count, cutoff_date));
    
    RAISE NOTICE 'Migration complete: % rows archived, % rows migrated', archived_count, migrated_count;
END $$;

-- =====================================================
-- STEP 3: SET UP SYNC TRIGGER (For each table)
-- =====================================================

-- Simple trigger function for ongoing sync
CREATE OR REPLACE FUNCTION sync_to_new_table()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Customize column mapping for your tables
        INSERT INTO your_new_table (original_id, data_column1, data_column2, business_date, created_at)
        VALUES (NEW.id, NEW.data_column1, NEW.data_column2, NEW.business_date, NEW.created_date);
        RETURN NEW;
        
    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE your_new_table 
        SET data_column1 = NEW.data_column1,
            data_column2 = NEW.data_column2,
            business_date = NEW.business_date,
            updated_at = NOW()
        WHERE original_id = NEW.id;
        RETURN NEW;
        
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM your_new_table WHERE original_id = OLD.id;
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger (replace table names)
CREATE TRIGGER sync_original_to_new_trigger
    AFTER INSERT OR UPDATE OR DELETE ON your_table_name
    FOR EACH ROW EXECUTE FUNCTION sync_to_new_table();

-- =====================================================
-- QUICK VALIDATION QUERIES
-- =====================================================

-- Check migration results
SELECT 
    'Original' as table_type, COUNT(*) as row_count FROM your_table_name
UNION ALL
SELECT 
    'New' as table_type, COUNT(*) as row_count FROM your_new_table  
UNION ALL
SELECT 
    'Archive' as table_type, COUNT(*) as row_count FROM your_archive_table;

-- Check recent activity (last hour)
SELECT COUNT(*) as new_records_last_hour 
FROM your_new_table 
WHERE created_at > NOW() - INTERVAL '1 hour';

-- View migration log
SELECT * FROM table_rename_log ORDER BY timestamp DESC;

-- =====================================================
-- CLEANUP (Run after validating everything works)
-- =====================================================

/*
-- Optionally drop the original table after validation
-- DROP TABLE your_table_name;

-- Or rename it for safety
-- ALTER TABLE your_table_name RENAME TO your_table_name_backup;
*/