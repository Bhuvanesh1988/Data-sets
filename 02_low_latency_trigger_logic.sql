-- =====================================================
-- LOW LATENCY TRIGGER LOGIC
-- Optimized for minimal transaction impact
-- Handles all DML scenarios (INSERT, UPDATE, DELETE)
-- =====================================================

-- Control flag for enabling/disabling sync (in-memory, fast)
CREATE OR REPLACE FUNCTION create_sync_control()
RETURNS VOID AS $$
BEGIN
    -- Create a simple control mechanism using a temporary table or config
    -- This avoids expensive lookups during trigger execution
    CREATE TEMP TABLE IF NOT EXISTS sync_control (
        table_name TEXT PRIMARY KEY,
        sync_enabled BOOLEAN DEFAULT TRUE,
        target_table TEXT,
        error_count INTEGER DEFAULT 0,
        last_error_time TIMESTAMP
    );
END;
$$ LANGUAGE plpgsql;

-- Fast sync control check (optimized for trigger use)
CREATE OR REPLACE FUNCTION is_sync_enabled(p_source_table TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    -- Fast in-memory check, fallback to enabled if not found
    RETURN COALESCE(
        (SELECT sync_enabled FROM sync_control WHERE table_name = p_source_table),
        TRUE
    );
EXCEPTION WHEN OTHERS THEN
    -- If temp table doesn't exist, assume sync is enabled
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql STABLE;

-- =====================================================
-- OPTIMIZED TRIGGER FUNCTION
-- Minimal overhead, maximum performance
-- =====================================================

CREATE OR REPLACE FUNCTION sync_table_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_target_table TEXT;
    v_sync_enabled BOOLEAN;
    v_insert_sql TEXT;
    v_update_sql TEXT;
    v_delete_sql TEXT;
BEGIN
    -- Fast sync check - exit immediately if disabled
    v_sync_enabled := is_sync_enabled(TG_TABLE_NAME);
    IF NOT v_sync_enabled THEN
        RETURN COALESCE(NEW, OLD);
    END IF;
    
    -- Get target table name from trigger name convention
    -- Expected format: sync_[source]_to_[target]_trigger
    v_target_table := split_part(TG_NAME, '_to_', 2);
    v_target_table := split_part(v_target_table, '_trigger', 1);
    
    -- Early exit if target table name not found
    IF v_target_table IS NULL OR v_target_table = '' THEN
        RETURN COALESCE(NEW, OLD);
    END IF;
    
    -- Handle different operations with minimal overhead
    IF TG_OP = 'INSERT' THEN
        -- Fast INSERT with conflict handling
        v_insert_sql := format(
            'INSERT INTO %I SELECT $1.* ON CONFLICT DO NOTHING',
            v_target_table
        );
        
        BEGIN
            EXECUTE v_insert_sql USING NEW;
        EXCEPTION WHEN OTHERS THEN
            -- Log error without blocking transaction
            -- In production, consider async error logging
            NULL; -- Silent failure to avoid blocking main transaction
        END;
        
        RETURN NEW;
        
    ELSIF TG_OP = 'UPDATE' THEN
        -- Efficient UPDATE using primary key (assumes id column exists)
        v_update_sql := format(
            'UPDATE %I SET 
             data = $1, 
             updated_at = NOW() 
             WHERE id = $2',
            v_target_table
        );
        
        BEGIN
            EXECUTE v_update_sql USING NEW, NEW.id;
        EXCEPTION WHEN OTHERS THEN
            -- Silent failure to avoid blocking main transaction
            NULL;
        END;
        
        RETURN NEW;
        
    ELSIF TG_OP = 'DELETE' THEN
        -- Handle DELETE based on business requirements
        -- Option 1: Hard delete
        v_delete_sql := format('DELETE FROM %I WHERE id = $1', v_target_table);
        
        -- Option 2: Soft delete (uncomment if preferred)
        -- v_delete_sql := format(
        --     'UPDATE %I SET deleted_at = NOW(), is_deleted = TRUE WHERE id = $1',
        --     v_target_table
        -- );
        
        BEGIN
            EXECUTE v_delete_sql USING OLD.id;
        EXCEPTION WHEN OTHERS THEN
            -- Silent failure to avoid blocking main transaction
            NULL;
        END;
        
        RETURN OLD;
    END IF;
    
    RETURN COALESCE(NEW, OLD);
    
EXCEPTION WHEN OTHERS THEN
    -- Ultimate fallback - never block the main transaction
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- SPECIALIZED TRIGGER FUNCTIONS FOR DIFFERENT SCENARIOS
-- =====================================================

-- 1. MINIMAL OVERHEAD TRIGGER (for high-volume tables)
CREATE OR REPLACE FUNCTION sync_minimal_overhead()
RETURNS TRIGGER AS $$
DECLARE
    v_target_table TEXT := TG_ARGV[0]; -- Pass target table as argument
BEGIN
    -- Skip all checks for maximum performance
    IF TG_OP = 'INSERT' THEN
        EXECUTE format('INSERT INTO %I SELECT ($1).*', v_target_table) USING NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        EXECUTE format('UPDATE %I SET updated_at = NOW() WHERE id = $1', v_target_table) USING NEW.id;
    ELSIF TG_OP = 'DELETE' THEN
        EXECUTE format('DELETE FROM %I WHERE id = $1', v_target_table) USING OLD.id;
    END IF;
    
    RETURN COALESCE(NEW, OLD);
    
EXCEPTION WHEN OTHERS THEN
    -- Never block the main transaction
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- 2. ASYNC TRIGGER (for non-critical sync)
CREATE OR REPLACE FUNCTION sync_async_notify()
RETURNS TRIGGER AS $$
DECLARE
    v_payload JSON;
BEGIN
    -- Build lightweight payload for async processing
    v_payload := json_build_object(
        'table', TG_TABLE_NAME,
        'operation', TG_OP,
        'id', COALESCE(NEW.id, OLD.id),
        'timestamp', EXTRACT(EPOCH FROM NOW())
    );
    
    -- Send async notification (processed by external worker)
    PERFORM pg_notify('table_sync_' || TG_TABLE_NAME, v_payload::TEXT);
    
    RETURN COALESCE(NEW, OLD);
    
EXCEPTION WHEN OTHERS THEN
    -- Never block the main transaction
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- 3. CONDITIONAL TRIGGER (sync only specific conditions)
CREATE OR REPLACE FUNCTION sync_conditional()
RETURNS TRIGGER AS $$
DECLARE
    v_target_table TEXT := TG_ARGV[0];
    v_condition_column TEXT := TG_ARGV[1];
    v_condition_value TEXT := TG_ARGV[2];
BEGIN
    -- Only sync if condition is met
    IF (TG_OP = 'INSERT' AND NEW IS NOT NULL) THEN
        -- Check condition on NEW record
        IF (SELECT (to_jsonb(NEW) ->> v_condition_column)) = v_condition_value THEN
            EXECUTE format('INSERT INTO %I SELECT ($1).*', v_target_table) USING NEW;
        END IF;
    ELSIF (TG_OP = 'UPDATE' AND NEW IS NOT NULL) THEN
        -- Sync if condition met on NEW or OLD record
        IF (SELECT (to_jsonb(NEW) ->> v_condition_column)) = v_condition_value OR
           (SELECT (to_jsonb(OLD) ->> v_condition_column)) = v_condition_value THEN
            EXECUTE format('UPDATE %I SET updated_at = NOW() WHERE id = $1', v_target_table) USING NEW.id;
        END IF;
    ELSIF (TG_OP = 'DELETE' AND OLD IS NOT NULL) THEN
        -- Always sync deletes regardless of condition
        EXECUTE format('DELETE FROM %I WHERE id = $1', v_target_table) USING OLD.id;
    END IF;
    
    RETURN COALESCE(NEW, OLD);
    
EXCEPTION WHEN OTHERS THEN
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- TRIGGER MANAGEMENT FUNCTIONS
-- =====================================================

-- Function to create optimized trigger
CREATE OR REPLACE FUNCTION create_sync_trigger(
    p_source_table TEXT,
    p_target_table TEXT,
    p_trigger_type TEXT DEFAULT 'standard', -- 'standard', 'minimal', 'async', 'conditional'
    p_condition_column TEXT DEFAULT NULL,
    p_condition_value TEXT DEFAULT NULL
)
RETURNS TEXT AS $$
DECLARE
    v_trigger_name TEXT;
    v_function_name TEXT;
    v_trigger_sql TEXT;
    v_args TEXT := '';
BEGIN
    v_trigger_name := format('sync_%s_to_%s_trigger', p_source_table, p_target_table);
    
    -- Select appropriate function based on type
    CASE p_trigger_type
        WHEN 'minimal' THEN
            v_function_name := 'sync_minimal_overhead';
            v_args := quote_literal(p_target_table);
        WHEN 'async' THEN
            v_function_name := 'sync_async_notify';
        WHEN 'conditional' THEN
            v_function_name := 'sync_conditional';
            v_args := format('%s, %s, %s', 
                           quote_literal(p_target_table),
                           quote_literal(p_condition_column),
                           quote_literal(p_condition_value));
        ELSE -- 'standard'
            v_function_name := 'sync_table_changes';
    END CASE;
    
    -- Drop existing trigger
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', v_trigger_name, p_source_table);
    
    -- Create new trigger
    v_trigger_sql := format(
        'CREATE TRIGGER %I
         AFTER INSERT OR UPDATE OR DELETE ON %I
         FOR EACH ROW EXECUTE FUNCTION %s(%s)',
        v_trigger_name, p_source_table, v_function_name, v_args
    );
    
    EXECUTE v_trigger_sql;
    
    -- Update sync control
    PERFORM create_sync_control();
    
    INSERT INTO sync_control (table_name, sync_enabled, target_table)
    VALUES (p_source_table, TRUE, p_target_table)
    ON CONFLICT (table_name) DO UPDATE SET
        sync_enabled = TRUE,
        target_table = EXCLUDED.target_table;
    
    RETURN format('Created %s trigger: %s', p_trigger_type, v_trigger_name);
    
END;
$$ LANGUAGE plpgsql;

-- Function to enable/disable sync for a table
CREATE OR REPLACE FUNCTION toggle_sync(
    p_table_name TEXT,
    p_enabled BOOLEAN
)
RETURNS TEXT AS $$
BEGIN
    PERFORM create_sync_control();
    
    UPDATE sync_control 
    SET sync_enabled = p_enabled
    WHERE table_name = p_table_name;
    
    IF NOT FOUND THEN
        INSERT INTO sync_control (table_name, sync_enabled)
        VALUES (p_table_name, p_enabled);
    END IF;
    
    RETURN format('Sync %s for table %s', 
                  CASE WHEN p_enabled THEN 'ENABLED' ELSE 'DISABLED' END,
                  p_table_name);
END;
$$ LANGUAGE plpgsql;

-- Function to remove sync trigger
CREATE OR REPLACE FUNCTION remove_sync_trigger(
    p_source_table TEXT,
    p_target_table TEXT
)
RETURNS TEXT AS $$
DECLARE
    v_trigger_name TEXT;
BEGIN
    v_trigger_name := format('sync_%s_to_%s_trigger', p_source_table, p_target_table);
    
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', v_trigger_name, p_source_table);
    
    DELETE FROM sync_control WHERE table_name = p_source_table;
    
    RETURN format('Removed trigger: %s', v_trigger_name);
END;
$$ LANGUAGE plpgsql;

/*
-- =====================================================
-- USAGE EXAMPLES:
-- =====================================================

-- 1. Create standard trigger (full sync with error handling)
SELECT create_sync_trigger('orders', 'orders_new', 'standard');

-- 2. Create minimal overhead trigger (for high-volume tables)
SELECT create_sync_trigger('transactions', 'transactions_new', 'minimal');

-- 3. Create async trigger (for non-critical sync)
SELECT create_sync_trigger('logs', 'logs_new', 'async');

-- 4. Create conditional trigger (sync only active records)
SELECT create_sync_trigger('users', 'users_new', 'conditional', 'status', 'active');

-- 5. Temporarily disable sync without dropping trigger
SELECT toggle_sync('orders', FALSE);

-- 6. Re-enable sync
SELECT toggle_sync('orders', TRUE);

-- 7. Remove trigger completely
SELECT remove_sync_trigger('orders', 'orders_new');

-- 8. Check current sync status
SELECT * FROM sync_control;
*/