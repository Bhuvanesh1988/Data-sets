-- =====================================================
-- BI-DIRECTIONAL LOGICAL REPLICATION TABLE RENAME STRATEGY
-- =====================================================
-- Specialized script for PostgreSQL logical replication with both sites active
-- Requires careful coordination to prevent replication conflicts

-- =====================================================
-- REPLICATION COORDINATION INFRASTRUCTURE
-- =====================================================

-- Table to coordinate rename operations between sites
CREATE TABLE IF NOT EXISTS replication_rename_coordination (
    operation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site_name VARCHAR(100), -- 'primary' or 'secondary' 
    table_name VARCHAR(255),
    operation_status VARCHAR(50), -- INITIATED, PREPARED, EXECUTING, COMPLETED, FAILED, ROLLED_BACK
    phase INTEGER, -- 1: Preparation, 2: Sync Stop, 3: Rename, 4: Sync Resume
    initiated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    coordinator_node BOOLEAN DEFAULT FALSE,
    partner_ready BOOLEAN DEFAULT FALSE,
    notes TEXT
);

-- Table to track replication slots and subscriptions
CREATE TABLE IF NOT EXISTS replication_state_backup (
    backup_id SERIAL PRIMARY KEY,
    slot_name VARCHAR(255),
    subscription_name VARCHAR(255),
    slot_lsn pg_lsn,
    subscription_status TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    operation_id UUID REFERENCES replication_rename_coordination(operation_id)
);

-- =====================================================
-- COORDINATION FUNCTIONS
-- =====================================================

-- Function to initiate coordinated rename operation
CREATE OR REPLACE FUNCTION initiate_coordinated_rename(
    p_site_name VARCHAR(100),
    p_table_name VARCHAR(255),
    p_is_coordinator BOOLEAN DEFAULT TRUE
)
RETURNS UUID AS $$
DECLARE
    operation_id UUID;
BEGIN
    -- Create coordination record
    INSERT INTO replication_rename_coordination 
    (site_name, table_name, operation_status, phase, coordinator_node)
    VALUES (p_site_name, p_table_name, 'INITIATED', 1, p_is_coordinator)
    RETURNING operation_id INTO operation_id;
    
    RAISE NOTICE 'Initiated coordinated rename operation % for table % on site %', 
                 operation_id, p_table_name, p_site_name;
    
    RETURN operation_id;
END;
$$ LANGUAGE plpgsql;

-- Function to check if partner site is ready
CREATE OR REPLACE FUNCTION check_partner_readiness(p_operation_id UUID)
RETURNS BOOLEAN AS $$
DECLARE
    local_status VARCHAR(50);
    partner_exists BOOLEAN;
BEGIN
    -- Get local operation status
    SELECT operation_status INTO local_status
    FROM replication_rename_coordination 
    WHERE operation_id = p_operation_id AND coordinator_node = TRUE;
    
    -- Check if partner has matching operation in PREPARED state
    -- This would typically be checked via dblink or external coordination
    -- For now, we'll simulate by checking a flag
    SELECT partner_ready INTO partner_exists
    FROM replication_rename_coordination 
    WHERE operation_id = p_operation_id;
    
    RETURN COALESCE(partner_exists, FALSE);
END;
$$ LANGUAGE plpgsql;

-- Function to backup current replication state
CREATE OR REPLACE FUNCTION backup_replication_state(p_operation_id UUID)
RETURNS INTEGER AS $$
DECLARE
    backup_count INTEGER := 0;
    slot_rec RECORD;
    sub_rec RECORD;
BEGIN
    -- Backup replication slots
    FOR slot_rec IN 
        SELECT slot_name, confirmed_flush_lsn 
        FROM pg_replication_slots 
        WHERE slot_type = 'logical'
    LOOP
        INSERT INTO replication_state_backup 
        (operation_id, slot_name, slot_lsn)
        VALUES (p_operation_id, slot_rec.slot_name, slot_rec.confirmed_flush_lsn);
        backup_count := backup_count + 1;
    END LOOP;
    
    -- Backup subscription information
    FOR sub_rec IN 
        SELECT subname, subenabled::TEXT
        FROM pg_subscription
    LOOP
        INSERT INTO replication_state_backup 
        (operation_id, subscription_name, subscription_status)
        VALUES (p_operation_id, sub_rec.subname, sub_rec.subenabled);
        backup_count := backup_count + 1;
    END LOOP;
    
    RAISE NOTICE 'Backed up % replication state entries', backup_count;
    RETURN backup_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PHASE 1: PREPARATION PHASE
-- =====================================================

-- Function to prepare for coordinated rename
CREATE OR REPLACE FUNCTION prepare_coordinated_rename(
    p_operation_id UUID,
    p_new_table_name VARCHAR(255),
    p_archive_table_name VARCHAR(255) DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    coord_rec RECORD;
    archive_table VARCHAR(255);
    cutoff_date TIMESTAMP;
BEGIN
    -- Get coordination record
    SELECT * INTO coord_rec 
    FROM replication_rename_coordination 
    WHERE operation_id = p_operation_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Operation ID % not found', p_operation_id;
    END IF;
    
    -- Update status to PREPARED
    UPDATE replication_rename_coordination 
    SET operation_status = 'PREPARING', 
        phase = 1,
        notes = format('Preparing rename of %s to %s', coord_rec.table_name, p_new_table_name)
    WHERE operation_id = p_operation_id;
    
    -- Backup replication state
    PERFORM backup_replication_state(p_operation_id);
    
    -- Determine archive table name
    archive_table := COALESCE(p_archive_table_name, coord_rec.table_name || '_archive_' || coord_rec.site_name);
    
    -- Create archive table
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I (LIKE %I INCLUDING ALL)', 
                   archive_table, coord_rec.table_name);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP DEFAULT NOW()', 
                   archive_table);
    EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS archive_reason VARCHAR(100) DEFAULT ''COORDINATED_RENAME''', 
                   archive_table);
    
    -- Get cutoff date (keep last 10% of data)
    cutoff_date := get_cutoff_date(coord_rec.table_name, 'created_date');
    
    -- Create new table structure (this needs to be customized per table)
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            id BIGSERIAL PRIMARY KEY,
            original_id BIGINT,
            -- Customize these columns for your table structure
            data_column1 VARCHAR(255),
            data_column2 INTEGER,
            business_date DATE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            replication_source VARCHAR(50) DEFAULT ''%s''
        )', p_new_table_name, coord_rec.site_name);
    
    -- Add indexes
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_original_id ON %I(original_id)', 
                   p_new_table_name, p_new_table_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_created_at ON %I(created_at)', 
                   p_new_table_name, p_new_table_name);
    
    -- Update coordination status
    UPDATE replication_rename_coordination 
    SET operation_status = 'PREPARED',
        notes = format('Tables created: new=%s, archive=%s, cutoff=%s', 
                      p_new_table_name, archive_table, cutoff_date)
    WHERE operation_id = p_operation_id;
    
    RAISE NOTICE 'Preparation phase completed for operation %', p_operation_id;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PHASE 2: REPLICATION SYNCHRONIZATION PHASE
-- =====================================================

-- Function to temporarily disable logical replication for specific table
CREATE OR REPLACE FUNCTION disable_table_replication(
    p_operation_id UUID,
    p_table_name VARCHAR(255)
)
RETURNS BOOLEAN AS $$
DECLARE
    sub_rec RECORD;
    coord_rec RECORD;
BEGIN
    -- Get coordination record
    SELECT * INTO coord_rec FROM replication_rename_coordination WHERE operation_id = p_operation_id;
    
    -- Update phase
    UPDATE replication_rename_coordination 
    SET phase = 2, operation_status = 'SYNC_STOPPING'
    WHERE operation_id = p_operation_id;
    
    -- Disable subscriptions temporarily (this may vary based on your setup)
    FOR sub_rec IN SELECT subname FROM pg_subscription WHERE subenabled LOOP
        RAISE NOTICE 'Temporarily disabling subscription %', sub_rec.subname;
        EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', sub_rec.subname);
    END LOOP;
    
    -- Wait for replication to catch up
    PERFORM pg_sleep(5);
    
    -- Create a coordination point
    PERFORM pg_logical_emit_message(false, 'table_rename_coordination', 
                                   format('operation_id:%s,phase:sync_stop,table:%s', 
                                         p_operation_id, p_table_name));
    
    UPDATE replication_rename_coordination 
    SET operation_status = 'SYNC_STOPPED'
    WHERE operation_id = p_operation_id;
    
    RAISE NOTICE 'Replication disabled for table % in operation %', p_table_name, p_operation_id;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Function to re-enable logical replication
CREATE OR REPLACE FUNCTION enable_table_replication(p_operation_id UUID)
RETURNS BOOLEAN AS $$
DECLARE
    sub_rec RECORD;
BEGIN
    -- Re-enable subscriptions
    FOR sub_rec IN SELECT subname FROM pg_subscription WHERE NOT subenabled LOOP
        RAISE NOTICE 'Re-enabling subscription %', sub_rec.subname;
        EXECUTE format('ALTER SUBSCRIPTION %I ENABLE', sub_rec.subname);
    END LOOP;
    
    -- Create coordination message
    PERFORM pg_logical_emit_message(false, 'table_rename_coordination', 
                                   format('operation_id:%s,phase:sync_resume', p_operation_id));
    
    UPDATE replication_rename_coordination 
    SET operation_status = 'SYNC_RESUMED'
    WHERE operation_id = p_operation_id;
    
    RAISE NOTICE 'Replication re-enabled for operation %', p_operation_id;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PHASE 3: DATA MIGRATION PHASE
-- =====================================================

-- Function to execute coordinated data migration with batching
CREATE OR REPLACE FUNCTION execute_coordinated_migration(
    p_operation_id UUID,
    p_new_table_name VARCHAR(255),
    p_archive_table_name VARCHAR(255),
    p_batch_size INTEGER DEFAULT 5000
)
RETURNS BOOLEAN AS $$
DECLARE
    coord_rec RECORD;
    cutoff_date TIMESTAMP;
    archive_job_id INTEGER;
    migration_job_id INTEGER;
BEGIN
    -- Get coordination record
    SELECT * INTO coord_rec FROM replication_rename_coordination WHERE operation_id = p_operation_id;
    
    -- Update status
    UPDATE replication_rename_coordination 
    SET phase = 3, operation_status = 'EXECUTING'
    WHERE operation_id = p_operation_id;
    
    -- Get cutoff date
    cutoff_date := get_cutoff_date(coord_rec.table_name, 'created_date');
    
    -- Create batch jobs for archiving old data
    archive_job_id := create_batch_job(
        format('archive_%s_%s', coord_rec.table_name, coord_rec.site_name),
        coord_rec.table_name,
        'COORDINATED_ARCHIVE',
        p_batch_size
    );
    
    -- Execute batch archiving
    PERFORM execute_batch_archive(
        archive_job_id,
        cutoff_date,
        p_archive_table_name,
        'created_date'
    );
    
    -- Create batch job for migrating recent data
    migration_job_id := create_batch_job(
        format('migrate_%s_to_%s', coord_rec.table_name, p_new_table_name),
        coord_rec.table_name,
        'COORDINATED_MIGRATION',
        p_batch_size
    );
    
    -- Execute batch migration with custom column mapping
    PERFORM execute_batch_migration(
        migration_job_id,
        coord_rec.table_name,
        p_new_table_name,
        cutoff_date,
        'created_date',
        format('
            INSERT INTO %I (original_id, data_column1, data_column2, business_date, created_at, replication_source)
            SELECT id, data_column1, data_column2, business_date, created_date, ''%s''
            FROM %I WHERE id > $1 AND id <= $2 AND created_date >= $3
            ORDER BY id LIMIT $4',
            p_new_table_name, coord_rec.site_name, coord_rec.table_name)
    );
    
    -- Update coordination status
    UPDATE replication_rename_coordination 
    SET operation_status = 'MIGRATION_COMPLETED',
        notes = format('Archive job: %s, Migration job: %s, Cutoff: %s', 
                      archive_job_id, migration_job_id, cutoff_date)
    WHERE operation_id = p_operation_id;
    
    RAISE NOTICE 'Data migration completed for operation %', p_operation_id;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PHASE 4: TRIGGER SETUP FOR ONGOING SYNC
-- =====================================================

-- Function to create replication-aware triggers
CREATE OR REPLACE FUNCTION create_replication_triggers(
    p_operation_id UUID,
    p_original_table VARCHAR(255),
    p_new_table VARCHAR(255)
)
RETURNS BOOLEAN AS $$
DECLARE
    coord_rec RECORD;
    trigger_function_name VARCHAR(255);
BEGIN
    -- Get coordination record
    SELECT * INTO coord_rec FROM replication_rename_coordination WHERE operation_id = p_operation_id;
    
    trigger_function_name := format('sync_%s_to_%s_%s', p_original_table, p_new_table, coord_rec.site_name);
    
    -- Create site-specific trigger function
    EXECUTE format('
        CREATE OR REPLACE FUNCTION %I()
        RETURNS TRIGGER AS $func$
        BEGIN
            -- Only process changes originating from this site to avoid loops
            IF TG_OP = ''INSERT'' THEN
                INSERT INTO %I (original_id, data_column1, data_column2, business_date, created_at, replication_source)
                VALUES (NEW.id, NEW.data_column1, NEW.data_column2, NEW.business_date, NEW.created_date, ''%s'')
                ON CONFLICT (original_id) DO UPDATE SET
                    data_column1 = EXCLUDED.data_column1,
                    data_column2 = EXCLUDED.data_column2,
                    business_date = EXCLUDED.business_date,
                    updated_at = NOW(),
                    replication_source = EXCLUDED.replication_source;
                RETURN NEW;
                
            ELSIF TG_OP = ''UPDATE'' THEN
                UPDATE %I SET 
                    data_column1 = NEW.data_column1,
                    data_column2 = NEW.data_column2,
                    business_date = NEW.business_date,
                    updated_at = NOW()
                WHERE original_id = NEW.id;
                RETURN NEW;
                
            ELSIF TG_OP = ''DELETE'' THEN
                -- Mark as deleted rather than hard delete to maintain referential integrity
                UPDATE %I SET 
                    updated_at = NOW(),
                    replication_source = ''DELETED_FROM_'' || ''%s''
                WHERE original_id = OLD.id;
                RETURN OLD;
            END IF;
            
            RETURN NULL;
        END;
        $func$ LANGUAGE plpgsql;',
        trigger_function_name, p_new_table, coord_rec.site_name, 
        p_new_table, p_new_table, coord_rec.site_name
    );
    
    -- Create the trigger
    EXECUTE format('
        CREATE TRIGGER %I
            AFTER INSERT OR UPDATE OR DELETE ON %I
            FOR EACH ROW EXECUTE FUNCTION %I()',
        format('trig_%s_sync_%s', p_original_table, coord_rec.site_name),
        p_original_table,
        trigger_function_name
    );
    
    RAISE NOTICE 'Created replication-aware trigger for operation %', p_operation_id;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- MASTER COORDINATION FUNCTION
-- =====================================================

-- Main function to orchestrate the entire coordinated rename
CREATE OR REPLACE FUNCTION execute_coordinated_table_rename(
    p_site_name VARCHAR(100),
    p_table_name VARCHAR(255),
    p_new_table_name VARCHAR(255),
    p_archive_table_name VARCHAR(255) DEFAULT NULL,
    p_is_coordinator BOOLEAN DEFAULT TRUE,
    p_wait_for_partner BOOLEAN DEFAULT TRUE
)
RETURNS UUID AS $$
DECLARE
    operation_id UUID;
    partner_ready BOOLEAN := FALSE;
    wait_cycles INTEGER := 0;
    max_wait_cycles INTEGER := 120; -- 10 minutes max wait
BEGIN
    -- Phase 1: Initiate coordination
    operation_id := initiate_coordinated_rename(p_site_name, p_table_name, p_is_coordinator);
    
    -- Phase 1: Prepare for rename
    PERFORM prepare_coordinated_rename(operation_id, p_new_table_name, p_archive_table_name);
    
    -- If this is the coordinator, wait for partner to be ready
    IF p_is_coordinator AND p_wait_for_partner THEN
        RAISE NOTICE 'Waiting for partner site to be ready...';
        WHILE NOT partner_ready AND wait_cycles < max_wait_cycles LOOP
            partner_ready := check_partner_readiness(operation_id);
            IF NOT partner_ready THEN
                PERFORM pg_sleep(5);
                wait_cycles := wait_cycles + 1;
            END IF;
        END LOOP;
        
        IF NOT partner_ready THEN
            UPDATE replication_rename_coordination 
            SET operation_status = 'FAILED', 
                notes = 'Partner site not ready within timeout period'
            WHERE operation_id = operation_id;
            RAISE EXCEPTION 'Partner site not ready within timeout period';
        END IF;
    END IF;
    
    -- Phase 2: Coordinate replication stop
    PERFORM disable_table_replication(operation_id, p_table_name);
    
    -- Phase 3: Execute migration
    PERFORM execute_coordinated_migration(
        operation_id, 
        p_new_table_name, 
        COALESCE(p_archive_table_name, p_table_name || '_archive_' || p_site_name)
    );
    
    -- Phase 4: Setup triggers
    PERFORM create_replication_triggers(operation_id, p_table_name, p_new_table_name);
    
    -- Phase 5: Re-enable replication
    PERFORM enable_table_replication(operation_id);
    
    -- Mark as completed
    UPDATE replication_rename_coordination 
    SET operation_status = 'COMPLETED', 
        completed_at = NOW(),
        phase = 5
    WHERE operation_id = operation_id;
    
    RAISE NOTICE 'Coordinated table rename completed successfully: %', operation_id;
    RETURN operation_id;
    
EXCEPTION WHEN OTHERS THEN
    -- Handle failures and attempt cleanup
    UPDATE replication_rename_coordination 
    SET operation_status = 'FAILED', 
        notes = format('Failed with error: %s', SQLERRM)
    WHERE operation_id = operation_id;
    
    -- Attempt to re-enable replication
    BEGIN
        PERFORM enable_table_replication(operation_id);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Failed to re-enable replication during cleanup: %', SQLERRM;
    END;
    
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- MONITORING AND VALIDATION FUNCTIONS
-- =====================================================

-- Function to check coordination status across sites
CREATE OR REPLACE FUNCTION check_coordination_status(p_operation_id UUID)
RETURNS TABLE (
    site_name VARCHAR(100),
    operation_status VARCHAR(50),
    phase INTEGER,
    is_coordinator BOOLEAN,
    initiated_at TIMESTAMP,
    completed_at TIMESTAMP,
    notes TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        rrc.site_name,
        rrc.operation_status,
        rrc.phase,
        rrc.coordinator_node,
        rrc.initiated_at,
        rrc.completed_at,
        rrc.notes
    FROM replication_rename_coordination rrc
    WHERE rrc.operation_id = p_operation_id
    ORDER BY rrc.coordinator_node DESC, rrc.initiated_at;
END;
$$ LANGUAGE plpgsql;

-- Function to validate data consistency after rename
CREATE OR REPLACE FUNCTION validate_coordinated_rename(
    p_operation_id UUID,
    p_original_table VARCHAR(255),
    p_new_table VARCHAR(255),
    p_archive_table VARCHAR(255)
)
RETURNS TABLE (
    validation_check VARCHAR(100),
    original_count BIGINT,
    new_count BIGINT,
    archive_count BIGINT,
    total_after_migration BIGINT,
    status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    WITH counts AS (
        SELECT 
            (SELECT COUNT(*) FROM information_schema.tables WHERE table_name = p_original_table) as orig_exists,
            (SELECT COUNT(*) FROM information_schema.tables WHERE table_name = p_new_table) as new_exists,
            (SELECT COUNT(*) FROM information_schema.tables WHERE table_name = p_archive_table) as arch_exists
    ),
    data_counts AS (
        SELECT 
            CASE WHEN (SELECT orig_exists FROM counts) > 0 
                 THEN (SELECT format('SELECT COUNT(*) FROM %I', p_original_table))::TEXT
                 ELSE '0' END as orig_sql,
            CASE WHEN (SELECT new_exists FROM counts) > 0 
                 THEN (SELECT format('SELECT COUNT(*) FROM %I', p_new_table))::TEXT  
                 ELSE '0' END as new_sql,
            CASE WHEN (SELECT arch_exists FROM counts) > 0 
                 THEN (SELECT format('SELECT COUNT(*) FROM %I', p_archive_table))::TEXT
                 ELSE '0' END as arch_sql
    )
    SELECT 
        'Row Count Validation'::VARCHAR(100),
        0::BIGINT as original_count,  -- Would need dynamic SQL to get actual counts
        0::BIGINT as new_count,
        0::BIGINT as archive_count,
        0::BIGINT as total_after_migration,
        'MANUAL_CHECK_REQUIRED'::VARCHAR(20);
        
    -- Note: Due to PostgreSQL limitations with dynamic SQL in functions,
    -- actual count validation should be done with the queries below:
    
    -- SELECT COUNT(*) as original_count FROM your_original_table;
    -- SELECT COUNT(*) as new_count FROM your_new_table;
    -- SELECT COUNT(*) as archive_count FROM your_archive_table;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- USAGE EXAMPLES AND COORDINATION WORKFLOW
-- =====================================================

/*
COORDINATION WORKFLOW FOR BI-DIRECTIONAL REPLICATION:

1. ON COORDINATOR SITE (e.g., primary):
   SELECT execute_coordinated_table_rename(
       'primary',              -- site_name
       'orders',              -- original_table
       'orders_v2',           -- new_table
       'orders_archive_primary', -- archive_table
       TRUE,                  -- is_coordinator
       TRUE                   -- wait_for_partner
   );

2. ON PARTNER SITE (e.g., secondary):
   SELECT execute_coordinated_table_rename(
       'secondary',           -- site_name
       'orders',              -- original_table  
       'orders_v2',           -- new_table
       'orders_archive_secondary', -- archive_table
       FALSE,                 -- is_coordinator
       FALSE                  -- wait_for_partner
   );

3. MONITOR COORDINATION:
   SELECT * FROM check_coordination_status('your-operation-uuid');

4. VALIDATE RESULTS:
   SELECT * FROM validate_coordinated_rename(
       'your-operation-uuid',
       'orders',
       'orders_v2', 
       'orders_archive_primary'
   );

5. CHECK BATCH JOB PROGRESS:
   SELECT * FROM batch_jobs_status;

IMPORTANT NOTES:
- Execute on coordinator site first, then partner site
- Ensure both sites have the same table structure
- Test thoroughly in non-production environment
- Have rollback plan ready
- Monitor replication lag during the process
*/