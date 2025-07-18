# PL/pgSQL Triggers: Comprehensive Guide for All Use Cases

## Table of Contents
1. [Trigger Fundamentals](#trigger-fundamentals)
2. [Trigger Types by Timing](#trigger-types-by-timing)
3. [Trigger Types by Events](#trigger-types-by-events)
4. [Row-Level vs Statement-Level Triggers](#row-level-vs-statement-level-triggers)
5. [Advanced Trigger Features](#advanced-trigger-features)
6. [Real-World Use Cases](#real-world-use-cases)
7. [Best Practices](#best-practices)

## Trigger Fundamentals

### Basic Trigger Syntax
```sql
CREATE [OR REPLACE] TRIGGER trigger_name
    {BEFORE | AFTER | INSTEAD OF} {event [OR ...]}
    ON table_name
    [FOR [EACH] {ROW | STATEMENT}]
    [WHEN (condition)]
    EXECUTE FUNCTION function_name();
```

### Trigger Function Template
```sql
CREATE OR REPLACE FUNCTION trigger_function_name()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Trigger logic here
    RETURN NEW; -- or OLD or NULL
END;
$$;
```

## Trigger Types by Timing

### 1. BEFORE Triggers
Execute before the triggering event occurs.

#### BEFORE INSERT Example
```sql
-- Function for BEFORE INSERT trigger
CREATE OR REPLACE FUNCTION validate_and_modify_before_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Validate data
    IF NEW.email IS NULL OR NEW.email = '' THEN
        RAISE EXCEPTION 'Email cannot be empty';
    END IF;
    
    -- Auto-generate values
    NEW.created_at := NOW();
    NEW.updated_at := NOW();
    NEW.email := LOWER(NEW.email);
    
    -- Generate ID if not provided
    IF NEW.id IS NULL THEN
        NEW.id := nextval('users_id_seq');
    END IF;
    
    RETURN NEW;
END;
$$;

-- Create the trigger
CREATE TRIGGER users_before_insert_trigger
    BEFORE INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION validate_and_modify_before_insert();
```

#### BEFORE UPDATE Example
```sql
CREATE OR REPLACE FUNCTION validate_before_update()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Prevent updates to certain fields
    IF OLD.id != NEW.id THEN
        RAISE EXCEPTION 'ID cannot be changed';
    END IF;
    
    -- Update timestamp
    NEW.updated_at := NOW();
    
    -- Version control
    NEW.version := OLD.version + 1;
    
    -- Log changes if email changed
    IF OLD.email != NEW.email THEN
        INSERT INTO email_change_log (user_id, old_email, new_email, changed_at)
        VALUES (OLD.id, OLD.email, NEW.email, NOW());
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER users_before_update_trigger
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION validate_before_update();
```

#### BEFORE DELETE Example
```sql
CREATE OR REPLACE FUNCTION soft_delete_user()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Implement soft delete instead of hard delete
    UPDATE users 
    SET deleted_at = NOW(), 
        is_active = FALSE 
    WHERE id = OLD.id;
    
    -- Prevent actual deletion
    RETURN NULL;
END;
$$;

CREATE TRIGGER users_soft_delete_trigger
    BEFORE DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION soft_delete_user();
```

### 2. AFTER Triggers
Execute after the triggering event has completed successfully.

#### AFTER INSERT Example
```sql
CREATE OR REPLACE FUNCTION log_user_creation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log user creation
    INSERT INTO user_audit_log (
        user_id, 
        action, 
        performed_at, 
        details
    ) VALUES (
        NEW.id, 
        'CREATE', 
        NOW(), 
        format('User created: %s (%s)', NEW.name, NEW.email)
    );
    
    -- Send welcome email (placeholder for external system)
    INSERT INTO email_queue (
        to_email, 
        subject, 
        body, 
        created_at
    ) VALUES (
        NEW.email,
        'Welcome!',
        format('Welcome %s! Your account has been created.', NEW.name),
        NOW()
    );
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER users_after_insert_trigger
    AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_creation();
```

#### AFTER UPDATE Example
```sql
CREATE OR REPLACE FUNCTION track_user_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    changes_json JSONB := '{}';
BEGIN
    -- Track what changed
    IF OLD.name != NEW.name THEN
        changes_json := changes_json || jsonb_build_object('name', 
            jsonb_build_object('old', OLD.name, 'new', NEW.name));
    END IF;
    
    IF OLD.email != NEW.email THEN
        changes_json := changes_json || jsonb_build_object('email', 
            jsonb_build_object('old', OLD.email, 'new', NEW.email));
    END IF;
    
    -- Only log if there were actual changes
    IF changes_json != '{}' THEN
        INSERT INTO user_audit_log (
            user_id, 
            action, 
            performed_at, 
            changes
        ) VALUES (
            NEW.id, 
            'UPDATE', 
            NOW(), 
            changes_json
        );
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER users_after_update_trigger
    AFTER UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION track_user_changes();
```

#### AFTER DELETE Example
```sql
CREATE OR REPLACE FUNCTION cleanup_after_user_deletion()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Clean up related data
    DELETE FROM user_sessions WHERE user_id = OLD.id;
    DELETE FROM user_preferences WHERE user_id = OLD.id;
    
    -- Archive user data
    INSERT INTO deleted_users_archive (
        original_id,
        name,
        email,
        deleted_at,
        deletion_reason
    ) VALUES (
        OLD.id,
        OLD.name,
        OLD.email,
        NOW(),
        'User account deleted'
    );
    
    -- Log the deletion
    INSERT INTO user_audit_log (
        user_id, 
        action, 
        performed_at, 
        details
    ) VALUES (
        OLD.id, 
        'DELETE', 
        NOW(), 
        format('User deleted: %s (%s)', OLD.name, OLD.email)
    );
    
    RETURN OLD;
END;
$$;

CREATE TRIGGER users_after_delete_trigger
    AFTER DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION cleanup_after_user_deletion();
```

### 3. INSTEAD OF Triggers
Used with views to make them updatable.

```sql
-- Create a view
CREATE VIEW user_summary AS
SELECT 
    u.id,
    u.name,
    u.email,
    up.theme,
    up.language
FROM users u
LEFT JOIN user_preferences up ON u.id = up.user_id;

-- INSTEAD OF INSERT trigger for the view
CREATE OR REPLACE FUNCTION insert_user_summary()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert into users table
    INSERT INTO users (name, email)
    VALUES (NEW.name, NEW.email);
    
    -- Get the new user ID
    NEW.id := currval('users_id_seq');
    
    -- Insert preferences if provided
    IF NEW.theme IS NOT NULL OR NEW.language IS NOT NULL THEN
        INSERT INTO user_preferences (user_id, theme, language)
        VALUES (NEW.id, NEW.theme, NEW.language);
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER user_summary_instead_of_insert
    INSTEAD OF INSERT ON user_summary
    FOR EACH ROW
    EXECUTE FUNCTION insert_user_summary();

-- INSTEAD OF UPDATE trigger for the view
CREATE OR REPLACE FUNCTION update_user_summary()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Update users table
    UPDATE users 
    SET name = NEW.name, email = NEW.email
    WHERE id = OLD.id;
    
    -- Update or insert preferences
    INSERT INTO user_preferences (user_id, theme, language)
    VALUES (NEW.id, NEW.theme, NEW.language)
    ON CONFLICT (user_id) DO UPDATE SET
        theme = EXCLUDED.theme,
        language = EXCLUDED.language;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER user_summary_instead_of_update
    INSTEAD OF UPDATE ON user_summary
    FOR EACH ROW
    EXECUTE FUNCTION update_user_summary();
```

## Trigger Types by Events

### 1. INSERT Triggers

#### Auto-numbering and Sequence Management
```sql
CREATE OR REPLACE FUNCTION auto_generate_invoice_number()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.invoice_number IS NULL THEN
        NEW.invoice_number := 'INV-' || 
            TO_CHAR(NOW(), 'YYYY') || '-' || 
            LPAD(nextval('invoice_sequence')::TEXT, 6, '0');
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER invoices_auto_number_trigger
    BEFORE INSERT ON invoices
    FOR EACH ROW
    EXECUTE FUNCTION auto_generate_invoice_number();
```

#### Data Validation and Transformation
```sql
CREATE OR REPLACE FUNCTION validate_product_data()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Validate price
    IF NEW.price <= 0 THEN
        RAISE EXCEPTION 'Product price must be greater than 0';
    END IF;
    
    -- Normalize product name
    NEW.name := TRIM(UPPER(NEW.name));
    
    -- Calculate discount price
    IF NEW.discount_percentage > 0 THEN
        NEW.discounted_price := NEW.price * (1 - NEW.discount_percentage / 100.0);
    ELSE
        NEW.discounted_price := NEW.price;
    END IF;
    
    -- Set default category if not provided
    IF NEW.category_id IS NULL THEN
        SELECT id INTO NEW.category_id 
        FROM categories 
        WHERE name = 'Uncategorized';
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER products_validation_trigger
    BEFORE INSERT ON products
    FOR EACH ROW
    EXECUTE FUNCTION validate_product_data();
```

### 2. UPDATE Triggers

#### Optimistic Locking
```sql
CREATE OR REPLACE FUNCTION implement_optimistic_locking()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if the version matches
    IF OLD.version != NEW.version THEN
        RAISE EXCEPTION 'Record has been modified by another user. Please refresh and try again.';
    END IF;
    
    -- Increment version
    NEW.version := OLD.version + 1;
    NEW.updated_at := NOW();
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER documents_optimistic_lock_trigger
    BEFORE UPDATE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION implement_optimistic_locking();
```

#### Field-Specific Change Tracking
```sql
CREATE OR REPLACE FUNCTION track_salary_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Only track if salary actually changed
    IF OLD.salary != NEW.salary THEN
        INSERT INTO salary_history (
            employee_id,
            old_salary,
            new_salary,
            change_date,
            change_reason,
            changed_by
        ) VALUES (
            NEW.id,
            OLD.salary,
            NEW.salary,
            NOW(),
            COALESCE(NEW.salary_change_reason, 'Not specified'),
            current_user
        );
        
        -- Clear the reason field after logging
        NEW.salary_change_reason := NULL;
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER employees_salary_tracking_trigger
    AFTER UPDATE ON employees
    FOR EACH ROW
    WHEN (OLD.salary IS DISTINCT FROM NEW.salary)
    EXECUTE FUNCTION track_salary_changes();
```

### 3. DELETE Triggers

#### Cascade Deletion with Logging
```sql
CREATE OR REPLACE FUNCTION cascade_delete_with_logging()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    related_count INTEGER;
BEGIN
    -- Count related records
    SELECT COUNT(*) INTO related_count
    FROM orders WHERE customer_id = OLD.id;
    
    -- Log the deletion
    INSERT INTO deletion_log (
        table_name,
        record_id,
        deleted_at,
        related_records_count,
        deleted_by
    ) VALUES (
        'customers',
        OLD.id,
        NOW(),
        related_count,
        current_user
    );
    
    -- Delete related records
    DELETE FROM order_items 
    WHERE order_id IN (
        SELECT id FROM orders WHERE customer_id = OLD.id
    );
    
    DELETE FROM orders WHERE customer_id = OLD.id;
    
    RETURN OLD;
END;
$$;

CREATE TRIGGER customers_cascade_delete_trigger
    BEFORE DELETE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION cascade_delete_with_logging();
```

### 4. TRUNCATE Triggers

```sql
CREATE OR REPLACE FUNCTION log_table_truncation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO truncation_log (
        table_name,
        truncated_at,
        truncated_by
    ) VALUES (
        TG_TABLE_NAME,
        NOW(),
        current_user
    );
    
    RETURN NULL;
END;
$$;

CREATE TRIGGER users_truncate_log_trigger
    AFTER TRUNCATE ON users
    FOR EACH STATEMENT
    EXECUTE FUNCTION log_table_truncation();
```

## Row-Level vs Statement-Level Triggers

### Statement-Level Triggers

#### Bulk Operation Logging
```sql
CREATE OR REPLACE FUNCTION log_bulk_operations()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO bulk_operation_log (
        table_name,
        operation,
        executed_at,
        executed_by
    ) VALUES (
        TG_TABLE_NAME,
        TG_OP,
        NOW(),
        current_user
    );
    
    RETURN NULL;
END;
$$;

CREATE TRIGGER products_bulk_operation_trigger
    AFTER INSERT OR UPDATE OR DELETE ON products
    FOR EACH STATEMENT
    EXECUTE FUNCTION log_bulk_operations();
```

#### Performance Monitoring
```sql
CREATE OR REPLACE FUNCTION monitor_large_operations()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    operation_id UUID;
BEGIN
    -- Generate operation ID and log start
    operation_id := gen_random_uuid();
    start_time := clock_timestamp();
    
    INSERT INTO operation_monitor (
        operation_id,
        table_name,
        operation_type,
        start_time
    ) VALUES (
        operation_id,
        TG_TABLE_NAME,
        TG_OP,
        start_time
    );
    
    -- Store in session variable for AFTER trigger
    PERFORM set_config('app.current_operation_id', operation_id::text, true);
    
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION complete_operation_monitoring()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    operation_id UUID;
    duration INTERVAL;
BEGIN
    -- Get operation ID from session
    operation_id := current_setting('app.current_operation_id')::UUID;
    
    -- Update with completion time
    UPDATE operation_monitor 
    SET end_time = clock_timestamp(),
        duration = clock_timestamp() - start_time
    WHERE operation_id = operation_monitor.operation_id;
    
    RETURN NULL;
END;
$$;

CREATE TRIGGER orders_start_monitor_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON orders
    FOR EACH STATEMENT
    EXECUTE FUNCTION monitor_large_operations();

CREATE TRIGGER orders_end_monitor_trigger
    AFTER INSERT OR UPDATE OR DELETE ON orders
    FOR EACH STATEMENT
    EXECUTE FUNCTION complete_operation_monitoring();
```

## Advanced Trigger Features

### 1. Conditional Triggers with WHEN Clause

```sql
-- Only trigger for high-value orders
CREATE OR REPLACE FUNCTION notify_high_value_order()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO high_value_notifications (
        order_id,
        customer_id,
        total_amount,
        created_at
    ) VALUES (
        NEW.id,
        NEW.customer_id,
        NEW.total_amount,
        NOW()
    );
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER high_value_order_trigger
    AFTER INSERT ON orders
    FOR EACH ROW
    WHEN (NEW.total_amount > 10000)
    EXECUTE FUNCTION notify_high_value_order();
```

### 2. Multi-Event Triggers

```sql
CREATE OR REPLACE FUNCTION comprehensive_audit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    audit_action TEXT;
    old_values JSONB;
    new_values JSONB;
BEGIN
    -- Determine the action
    IF TG_OP = 'INSERT' THEN
        audit_action := 'INSERT';
        old_values := NULL;
        new_values := to_jsonb(NEW);
    ELSIF TG_OP = 'UPDATE' THEN
        audit_action := 'UPDATE';
        old_values := to_jsonb(OLD);
        new_values := to_jsonb(NEW);
    ELSIF TG_OP = 'DELETE' THEN
        audit_action := 'DELETE';
        old_values := to_jsonb(OLD);
        new_values := NULL;
    END IF;
    
    -- Insert audit record
    INSERT INTO audit_trail (
        table_name,
        record_id,
        action,
        old_values,
        new_values,
        changed_at,
        changed_by
    ) VALUES (
        TG_TABLE_NAME,
        COALESCE(NEW.id, OLD.id),
        audit_action,
        old_values,
        new_values,
        NOW(),
        current_user
    );
    
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER comprehensive_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON sensitive_data
    FOR EACH ROW
    EXECUTE FUNCTION comprehensive_audit();
```

### 3. Trigger with Exception Handling

```sql
CREATE OR REPLACE FUNCTION safe_external_integration()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    api_response TEXT;
    error_msg TEXT;
BEGIN
    BEGIN
        -- Simulate external API call
        INSERT INTO external_sync_queue (
            table_name,
            record_id,
            operation,
            data,
            created_at,
            status
        ) VALUES (
            TG_TABLE_NAME,
            NEW.id,
            TG_OP,
            to_jsonb(NEW),
            NOW(),
            'pending'
        );
        
    EXCEPTION 
        WHEN OTHERS THEN
            -- Log the error but don't fail the main operation
            GET STACKED DIAGNOSTICS error_msg = MESSAGE_TEXT;
            
            INSERT INTO error_log (
                source,
                error_message,
                occurred_at,
                context_data
            ) VALUES (
                'safe_external_integration',
                error_msg,
                NOW(),
                jsonb_build_object(
                    'table', TG_TABLE_NAME,
                    'operation', TG_OP,
                    'record_id', NEW.id
                )
            );
    END;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER safe_integration_trigger
    AFTER INSERT OR UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION safe_external_integration();
```

## Real-World Use Cases

### 1. E-commerce Inventory Management

```sql
-- Inventory tracking trigger
CREATE OR REPLACE FUNCTION manage_inventory()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    current_stock INTEGER;
    reserved_stock INTEGER;
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Check stock availability when order is created
        SELECT stock_quantity, reserved_quantity 
        INTO current_stock, reserved_stock
        FROM products 
        WHERE id = NEW.product_id;
        
        IF (current_stock - reserved_stock) < NEW.quantity THEN
            RAISE EXCEPTION 'Insufficient stock. Available: %, Requested: %', 
                (current_stock - reserved_stock), NEW.quantity;
        END IF;
        
        -- Reserve the stock
        UPDATE products 
        SET reserved_quantity = reserved_quantity + NEW.quantity
        WHERE id = NEW.product_id;
        
    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
        IF NEW.status = 'shipped' THEN
            -- Reduce actual stock when shipped
            UPDATE products 
            SET stock_quantity = stock_quantity - NEW.quantity,
                reserved_quantity = reserved_quantity - NEW.quantity
            WHERE id = NEW.product_id;
            
        ELSIF NEW.status = 'cancelled' THEN
            -- Release reserved stock when cancelled
            UPDATE products 
            SET reserved_quantity = reserved_quantity - NEW.quantity
            WHERE id = NEW.product_id;
        END IF;
    END IF;
    
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER inventory_management_trigger
    AFTER INSERT OR UPDATE ON order_items
    FOR EACH ROW
    EXECUTE FUNCTION manage_inventory();
```

### 2. Financial Transactions with Double-Entry Bookkeeping

```sql
CREATE OR REPLACE FUNCTION create_journal_entries()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    entry_id UUID;
BEGIN
    -- Generate journal entry ID
    entry_id := gen_random_uuid();
    
    -- Create debit entry
    INSERT INTO journal_entries (
        id,
        transaction_id,
        account_id,
        entry_type,
        amount,
        description,
        created_at
    ) VALUES (
        gen_random_uuid(),
        NEW.id,
        NEW.debit_account_id,
        'DEBIT',
        NEW.amount,
        NEW.description,
        NOW()
    );
    
    -- Create credit entry
    INSERT INTO journal_entries (
        id,
        transaction_id,
        account_id,
        entry_type,
        amount,
        description,
        created_at
    ) VALUES (
        gen_random_uuid(),
        NEW.id,
        NEW.credit_account_id,
        'CREDIT',
        NEW.amount,
        NEW.description,
        NOW()
    );
    
    -- Update account balances
    UPDATE accounts 
    SET balance = balance + NEW.amount 
    WHERE id = NEW.debit_account_id;
    
    UPDATE accounts 
    SET balance = balance - NEW.amount 
    WHERE id = NEW.credit_account_id;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER financial_transaction_trigger
    AFTER INSERT ON financial_transactions
    FOR EACH ROW
    EXECUTE FUNCTION create_journal_entries();
```

### 3. Data Archival and Retention

```sql
CREATE OR REPLACE FUNCTION auto_archive_old_records()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Archive records older than 1 year when new ones are inserted
    IF TG_OP = 'INSERT' THEN
        WITH archived_records AS (
            DELETE FROM user_activities 
            WHERE created_at < NOW() - INTERVAL '1 year'
            RETURNING *
        )
        INSERT INTO user_activities_archive 
        SELECT *, NOW() as archived_at 
        FROM archived_records;
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER auto_archive_trigger
    AFTER INSERT ON user_activities
    FOR EACH STATEMENT
    EXECUTE FUNCTION auto_archive_old_records();
```

### 4. Real-time Notifications and Cache Invalidation

```sql
CREATE OR REPLACE FUNCTION handle_realtime_updates()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    notification_payload JSONB;
BEGIN
    -- Prepare notification payload
    notification_payload := jsonb_build_object(
        'table', TG_TABLE_NAME,
        'operation', TG_OP,
        'record_id', COALESCE(NEW.id, OLD.id),
        'timestamp', extract(epoch from NOW())
    );
    
    -- Send real-time notification
    PERFORM pg_notify('data_changes', notification_payload::text);
    
    -- Invalidate related cache entries
    INSERT INTO cache_invalidation_queue (
        cache_key,
        created_at
    ) VALUES (
        format('%s:%s', TG_TABLE_NAME, COALESCE(NEW.id, OLD.id)),
        NOW()
    );
    
    -- For user changes, also invalidate user-specific caches
    IF TG_TABLE_NAME = 'users' THEN
        INSERT INTO cache_invalidation_queue (cache_key, created_at) VALUES 
        (format('user_profile:%s', COALESCE(NEW.id, OLD.id)), NOW()),
        (format('user_permissions:%s', COALESCE(NEW.id, OLD.id)), NOW());
    END IF;
    
    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER realtime_updates_trigger
    AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION handle_realtime_updates();
```

## Best Practices

### 1. Performance Considerations

```sql
-- Efficient trigger with minimal processing
CREATE OR REPLACE FUNCTION efficient_audit_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Only log significant changes
    IF TG_OP = 'UPDATE' AND OLD.* IS NOT DISTINCT FROM NEW.* THEN
        RETURN NEW; -- No actual changes
    END IF;
    
    -- Use COPY for bulk inserts when possible
    -- Avoid complex queries in triggers
    -- Keep trigger logic simple and fast
    
    INSERT INTO simple_audit_log (table_name, record_id, operation, changed_at)
    VALUES (TG_TABLE_NAME, COALESCE(NEW.id, OLD.id), TG_OP, NOW());
    
    RETURN COALESCE(NEW, OLD);
END;
$$;
```

### 2. Error Handling and Debugging

```sql
CREATE OR REPLACE FUNCTION robust_trigger_with_logging()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    error_context TEXT;
BEGIN
    BEGIN
        -- Main trigger logic here
        IF NEW.amount < 0 THEN
            RAISE EXCEPTION 'Amount cannot be negative: %', NEW.amount;
        END IF;
        
        -- Process the record
        NEW.processed_at := NOW();
        
        RETURN NEW;
        
    EXCEPTION 
        WHEN OTHERS THEN
            -- Capture error context
            GET STACKED DIAGNOSTICS 
                error_context = PG_EXCEPTION_CONTEXT;
            
            -- Log detailed error information
            INSERT INTO trigger_error_log (
                trigger_name,
                table_name,
                operation,
                error_message,
                error_context,
                record_data,
                occurred_at
            ) VALUES (
                TG_NAME,
                TG_TABLE_NAME,
                TG_OP,
                SQLERRM,
                error_context,
                CASE 
                    WHEN NEW IS NOT NULL THEN to_jsonb(NEW)
                    ELSE to_jsonb(OLD)
                END,
                NOW()
            );
            
            -- Re-raise the exception
            RAISE;
    END;
END;
$$;
```

### 3. Trigger Security

```sql
-- Secure trigger with permission checks
CREATE OR REPLACE FUNCTION secure_sensitive_data_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER -- Run with function owner's privileges
AS $$
BEGIN
    -- Check if current user has permission for this operation
    IF NOT EXISTS (
        SELECT 1 FROM user_permissions 
        WHERE user_name = current_user 
        AND table_name = TG_TABLE_NAME 
        AND operation = TG_OP
    ) THEN
        RAISE EXCEPTION 'Access denied: User % not authorized for % on %', 
            current_user, TG_OP, TG_TABLE_NAME;
    END IF;
    
    -- Mask sensitive data for non-admin users
    IF NOT EXISTS (
        SELECT 1 FROM user_roles 
        WHERE user_name = current_user 
        AND role_name = 'admin'
    ) THEN
        NEW.ssn := 'XXX-XX-' || RIGHT(NEW.ssn, 4);
        NEW.credit_card := 'XXXX-XXXX-XXXX-' || RIGHT(NEW.credit_card, 4);
    END IF;
    
    RETURN NEW;
END;
$$;
```

### 4. Maintenance and Monitoring

```sql
-- Trigger for monitoring trigger performance
CREATE OR REPLACE FUNCTION monitor_trigger_performance()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    execution_time INTERVAL;
BEGIN
    start_time := clock_timestamp();
    
    -- Your trigger logic here
    -- ... main processing ...
    
    end_time := clock_timestamp();
    execution_time := end_time - start_time;
    
    -- Log slow triggers
    IF execution_time > INTERVAL '100 milliseconds' THEN
        INSERT INTO slow_trigger_log (
            trigger_name,
            table_name,
            execution_time,
            logged_at
        ) VALUES (
            TG_NAME,
            TG_TABLE_NAME,
            execution_time,
            NOW()
        );
    END IF;
    
    RETURN NEW;
END;
$$;
```

## Conclusion

This comprehensive guide covers all major aspects of PL/pgSQL trigger creation:

- **Timing**: BEFORE, AFTER, INSTEAD OF triggers
- **Events**: INSERT, UPDATE, DELETE, TRUNCATE triggers  
- **Levels**: Row-level vs statement-level triggers
- **Advanced Features**: Conditional triggers, multi-event triggers, error handling
- **Real-world Use Cases**: Inventory management, financial transactions, auditing, notifications
- **Best Practices**: Performance, security, debugging, monitoring

Key takeaways:
1. Choose the right trigger timing and level for your use case
2. Keep trigger logic simple and efficient
3. Implement proper error handling and logging
4. Consider security implications
5. Monitor trigger performance
6. Use triggers judiciously - not every business rule needs a trigger

Remember that triggers execute automatically and can significantly impact database performance if not designed carefully. Always test thoroughly and consider alternatives like application-level logic when appropriate.