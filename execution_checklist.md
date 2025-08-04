# Table Rename Strategy Execution Checklist

## Pre-Execution Phase

### 1. Environment Assessment ✓
- [ ] **Verify replication status**
  - [ ] Confirm only one site is active for writes
  - [ ] Check replication lag is within acceptable limits
  - [ ] Verify passive site is in sync
  - [ ] Document current replication configuration

- [ ] **Resource verification**
  - [ ] Confirm sufficient disk space (3x largest table size)
  - [ ] Verify backup space availability
  - [ ] Check CPU and memory resources
  - [ ] Ensure network bandwidth for replication

- [ ] **Dependency mapping**
  - [ ] Identify all tables that will be affected
  - [ ] Map foreign key relationships
  - [ ] Document application dependencies
  - [ ] List views, stored procedures, triggers that reference the tables

### 2. Backup and Safety Measures ✓
- [ ] **Create full backup of active site**
  ```sql
  -- Example backup command
  pg_dump -h active_host -U username -d database_name > pre_migration_backup.sql
  ```

- [ ] **Create point-in-time recovery checkpoint**
  ```sql
  SELECT pg_create_restore_point('before_table_rename_strategy');
  ```

- [ ] **Test restore procedures on test environment**
- [ ] **Verify rollback plan is executable**

### 3. Communication and Scheduling ✓
- [ ] **Schedule maintenance window**
  - Minimum recommended: 4-8 hours for large tables
  - Consider business impact and data volume

- [ ] **Notify stakeholders**
  - [ ] Database administrators
  - [ ] Application teams
  - [ ] Business users
  - [ ] Operations team

- [ ] **Prepare monitoring dashboard**
- [ ] **Set up alerting for critical thresholds**

## Execution Phase

### Phase 1: Infrastructure Setup (Active Site) ✓

#### Step 1.1: Deploy Migration Framework
- [ ] **Execute infrastructure setup scripts**
  ```sql
  \i implementation_scripts.sql
  -- Run PART 1: INFRASTRUCTURE SETUP
  ```

- [ ] **Verify schema creation**
  ```sql
  SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'table_migration';
  ```

- [ ] **Test utility functions**
  ```sql
  SELECT table_migration.get_config('data_retention_percentage');
  ```

#### Step 1.2: Configure Migration Parameters
- [ ] **Set retention percentage**
  ```sql
  UPDATE table_migration.migration_config 
  SET config_value = '0.1'  -- 10% recent data
  WHERE config_key = 'data_retention_percentage';
  ```

- [ ] **Set batch size based on system capacity**
  ```sql
  UPDATE table_migration.migration_config 
  SET config_value = '10000'  -- Adjust based on performance testing
  WHERE config_key = 'batch_size';
  ```

#### Step 1.3: Analyze Target Tables
- [ ] **Run data volume analysis for each table**
  ```sql
  -- For each table to be migrated:
  SELECT 
      DATE_TRUNC('month', created_date) as month,
      COUNT(*) as record_count,
      ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) as percentage
  FROM your_table_name
  GROUP BY DATE_TRUNC('month', created_date)
  ORDER BY month DESC;
  ```

- [ ] **Document cutoff dates for each table**
  ```sql
  SELECT table_migration.get_data_cutoff_date('your_table_name', 'created_date');
  ```

### Phase 2: Table-by-Table Migration ✓

For each table in your migration list, follow these steps:

#### Step 2.1: Create New Table Structure
- [ ] **Design optimized new table**
  ```sql
  CREATE TABLE new_table_name (
      id BIGSERIAL PRIMARY KEY,
      original_id BIGINT,
      -- Add your business columns here
      business_date DATE,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      is_migrated BOOLEAN DEFAULT FALSE,
      source_system VARCHAR(50) DEFAULT 'NEW_SYSTEM'
  );
  
  -- Add performance indexes
  CREATE INDEX idx_new_table_business_date ON new_table_name(business_date);
  CREATE INDEX idx_new_table_created_at ON new_table_name(created_at);
  CREATE INDEX idx_new_table_original_id ON new_table_name(original_id);
  ```

- [ ] **Validate table structure**
  ```sql
  SELECT table_migration.validate_table_structure('original_table', 'new_table');
  ```

#### Step 2.2: Execute Migration
- [ ] **Run dry run first**
  ```sql
  SELECT table_migration.execute_table_rename_strategy(
      'original_table_name', 
      'new_table_name', 
      NULL,  -- Let system generate archive table name
      'created_date',
      TRUE   -- DRY RUN
  );
  ```

- [ ] **Review dry run results**
- [ ] **Execute actual migration**
  ```sql
  SELECT table_migration.execute_table_rename_strategy(
      'original_table_name', 
      'new_table_name', 
      NULL,
      'created_date',
      FALSE  -- ACTUAL RUN
  );
  ```

- [ ] **Monitor migration progress**
  ```sql
  -- Check progress
  SELECT * FROM table_migration.generate_migration_report()
  WHERE table_name IN ('original_table_name', 'new_table_name')
  ORDER BY operation_time DESC;
  ```

#### Step 2.3: Set Up Change Capture
- [ ] **Create sync trigger**
  ```sql
  CREATE TRIGGER sync_original_to_new_trigger
      AFTER INSERT OR UPDATE OR DELETE ON original_table_name
      FOR EACH ROW EXECUTE FUNCTION table_migration.sync_changes_trigger();
  ```

- [ ] **Test trigger functionality**
  ```sql
  -- Insert test record
  INSERT INTO original_table_name (data_column1, data_column2) 
  VALUES ('test_data', 123);
  
  -- Verify it appears in new table
  SELECT COUNT(*) FROM new_table_name WHERE source_system = 'TRIGGER_SYNC';
  ```

### Phase 3: Replication Synchronization ✓

#### Step 3.1: Prepare Passive Site
- [ ] **Verify DDL replication is working**
  ```sql
  -- On passive site, check if migration schema exists
  SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'table_migration';
  ```

- [ ] **Manual DDL sync if needed**
  - If DDL doesn't replicate automatically, manually execute:
    - Infrastructure setup scripts
    - New table creation scripts
    - Trigger creation scripts

#### Step 3.2: Verify Replication Health
- [ ] **Check replication lag**
  ```sql
  -- PostgreSQL example
  SELECT 
      client_addr,
      state,
      pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) AS flush_lag_bytes,
      pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_lag_bytes
  FROM pg_stat_replication;
  ```

- [ ] **Verify data consistency between sites**
  ```sql
  -- Compare row counts
  SELECT 'new_table' as table_type, COUNT(*) as row_count FROM new_table_name
  UNION ALL
  SELECT 'archive_table' as table_type, COUNT(*) as row_count FROM archive_table_name;
  ```

#### Step 3.3: Re-enable Full Replication
- [ ] **Enable replication for migrated tables**
  ```sql
  UPDATE table_migration.replication_control 
  SET replication_enabled = TRUE,
      last_sync_time = NOW(),
      notes = 'Migration completed, replication re-enabled'
  WHERE table_name = 'original_table_name';
  ```

### Phase 4: Validation and Testing ✓

#### Step 4.1: Data Integrity Validation
- [ ] **Run comprehensive validation queries**
  ```sql
  -- Validation summary
  WITH validation_summary AS (
      SELECT 
          'Original Table' as table_type,
          COUNT(*) as record_count,
          MIN(created_date) as earliest_date,
          MAX(created_date) as latest_date
      FROM original_table_name
      UNION ALL
      SELECT 
          'New Table' as table_type,
          COUNT(*) as record_count,
          MIN(created_at) as earliest_date,
          MAX(created_at) as latest_date
      FROM new_table_name
      UNION ALL
      SELECT 
          'Archive Table' as table_type,
          COUNT(*) as record_count,
          MIN(created_date) as earliest_date,
          MAX(created_date) as latest_date
      FROM archive_table_name
  )
  SELECT * FROM validation_summary;
  ```

- [ ] **Verify no data loss**
  ```sql
  -- Check total records are preserved
  SELECT 
      (SELECT COUNT(*) FROM new_table_name) + 
      (SELECT COUNT(*) FROM archive_table_name) as total_after_migration,
      (SELECT COUNT(*) FROM original_table_name) as original_count;
  ```

#### Step 4.2: Performance Testing
- [ ] **Test query performance on new table**
- [ ] **Verify trigger overhead is acceptable**
- [ ] **Test application functionality**
- [ ] **Monitor system resources during peak load**

#### Step 4.3: Application Testing
- [ ] **Test read operations from new table**
- [ ] **Test write operations through triggers**
- [ ] **Verify application error handling**
- [ ] **Test failover scenarios**

### Phase 5: Monitoring Setup ✓

#### Step 5.1: Configure Ongoing Monitoring
- [ ] **Set up health check monitoring**
  ```sql
  -- Create monitoring view
  CREATE OR REPLACE VIEW daily_migration_health AS
  SELECT * FROM table_migration.health_check();
  ```

- [ ] **Schedule periodic health checks**
  ```sql
  -- Example: Create a cron job or scheduled task
  -- */15 * * * * psql -d database -c "SELECT * FROM daily_migration_health;"
  ```

#### Step 5.2: Alert Configuration
- [ ] **Set up alerts for:**
  - [ ] Replication lag exceeding threshold
  - [ ] Failed trigger executions
  - [ ] Unusual data volume changes
  - [ ] Archive table growth rate

- [ ] **Create dashboard for operations team**

## Post-Migration Phase

### Immediate Tasks (First 24 Hours) ✓
- [ ] **Monitor system stability**
- [ ] **Verify data sync is working**
- [ ] **Check application performance**
- [ ] **Review error logs**
- [ ] **Validate replication health**

### Short-term Tasks (First Week) ✓
- [ ] **Daily data integrity checks**
- [ ] **Performance monitoring and tuning**
- [ ] **Review and optimize trigger performance**
- [ ] **User feedback collection**
- [ ] **Document lessons learned**

### Long-term Tasks (First Month) ✓
- [ ] **Plan original table cleanup** (after business approval)
  ```sql
  -- Example cleanup after 30 days
  DROP TABLE original_table_name_backup;  -- If created
  ```

- [ ] **Archive old audit logs**
- [ ] **Review and update documentation**
- [ ] **Train team on new procedures**
- [ ] **Plan for future table migrations**

## Rollback Procedures ✓

### Immediate Rollback (If issues detected within first few hours)
1. **Drop triggers**
   ```sql
   DROP TRIGGER IF EXISTS sync_original_to_new_trigger ON original_table_name;
   ```

2. **Restore from backup if needed**
3. **Re-enable original table usage**
4. **Notify stakeholders**

### Partial Rollback (If new table has issues but archive is good)
1. **Disable triggers**
2. **Restore recent data from original table to new table**
3. **Fix issues and re-enable**

### Full Rollback (Complete restoration)
1. **Restore from point-in-time backup**
2. **Re-sync replication**
3. **Validate data consistency**
4. **Perform root cause analysis**

## Success Criteria Checklist ✓

- [ ] **All new data flows through new table structure**
- [ ] **Historical data preserved in archive tables**
- [ ] **Replication lag within acceptable limits (<5 minutes)**
- [ ] **Application performance maintained or improved**
- [ ] **No data loss confirmed through validation**
- [ ] **Monitoring and alerting operational**
- [ ] **Team trained on new procedures**
- [ ] **Documentation updated and complete**

## Emergency Contacts ✓

| Role | Name | Phone | Email | Escalation |
|------|------|-------|-------|------------|
| DBA Lead | [Name] | [Phone] | [Email] | Primary |
| App Team Lead | [Name] | [Phone] | [Email] | Secondary |
| Operations | [Name] | [Phone] | [Email] | 24/7 |
| Business Owner | [Name] | [Phone] | [Email] | Executive |

## Notes and Observations

### Migration Date: ________________
### Executed By: ___________________
### Duration: ______________________

**Issues Encountered:**
- 
- 
- 

**Performance Observations:**
- 
- 
- 

**Recommendations for Future Migrations:**
- 
- 
- 