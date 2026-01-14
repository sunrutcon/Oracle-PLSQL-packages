create or replace PACKAGE BODY PKG_SCD2 AS

  /*
    -- source table structure
    CREATE TABLE src_table (
      -- natural key columns
      key_column1 VARCHAR2(100),
      key_column2 VARCHAR2(100),
      -- columns to compare
      compare_column1 VARCHAR2(100),
      compare_column2 VARCHAR2(100),
      -- other columns
      other_column1 VARCHAR2(100),
      other_column2 VARCHAR2(100)
    );

    -- intermediate table structure
    CREATE TABLE int_table (
      -- primary key column
      pk_column number,
      -- natural key columns
      key_column1 VARCHAR2(100),
      key_column2 VARCHAR2(100),
      -- columns to compare
      compare_column1 VARCHAR2(100),
      compare_column2 VARCHAR2(100),
      -- other columns
      other_column1 VARCHAR2(100),
      other_column2 VARCHAR2(100),
      -- action column
      action VARCHAR2(1),
      -- version id column
      version_id NUMBER
    );

    -- target table structure
    CREATE TABLE trg_table (
      -- primary key column
      pk_column number,
      -- natural key columns
      key_column1 VARCHAR2(100),
      key_column2 VARCHAR2(100),
      -- columns to compare
      compare_column1 VARCHAR2(100),
      compare_column2 VARCHAR2(100),
      -- other columns
      other_column1 VARCHAR2(100),
      other_column2 VARCHAR2(100),
      -- SCD2 columns
      valid_from date,
      valid_to date,
      version_id NUMBER,
      is_current number,
      is_deleted number,
      -- technical date of the row when the row is created
      date_modified date
    );

  */
  procedure load_data(
    p_src_table varchar2, 
    p_int_table varchar2, 
    p_trg_table varchar2,
    p_pk_column varchar2,
    p_key_columns varchar2,
    p_compare_columns clob,
    p_other_columns varchar2,
    p_verbose varchar2:='Y',
    p_execute varchar2:='N'
  ) AS
    -- flags for duplicate records and null key columns
    v_dupl_rec_cnt NUMBER;
    v_null_key_cols_cnt NUMBER;
    
    -- sql statements
    v_null_key_columns_sql clob;
    v_duplicate_records_sql clob;
    v_new_records_sql clob;
    v_closing_records_sql clob;
    v_update_records_sql clob;
    v_update_target_table_sql clob;
    v_insert_into_target_table_sql clob;
    
    -- sql text for on join condition
    v_on_join_condition clob;
    v_comp_cols_condition clob;
    
    -- arrays for columns
    type t_vc_tab is table of varchar2(128 char) index by binary_integer;
    v_key_cols_tab t_vc_tab;
    v_comp_cols_tab t_vc_tab;
    v_other_cols_tab t_vc_tab;

    v_valid_from date;
    v_valid_to date;
    v_valid_to_inf date;

    v_valid_from_str varchar2(100);
    v_valid_to_str varchar2(100);
    v_valid_to_inf_str varchar2(100);

    v_pk_column varchar2(100):=replace(p_pk_column, ' ', '');
    v_key_columns varchar2(2000):=replace(p_key_columns, ' ', '');
    v_compare_columns clob:=replace(p_compare_columns, ' ', '');
    v_other_columns clob:=replace(p_other_columns, ' ', '');
    
    v_proc_params_txt clob;
    
    v_evt_name varchar2(64 char) := 'PKG_SCD2.LOAD_DATA';
    v_main_evt_id int;
    v_child_evt_id int;
    v_step_id int := 0;
    
  begin
    -- print calling procedure and  parameters: parameter name = [parameter value]
    v_proc_params_txt := '<PKG_SCD2.load_data>' || chr(10)
      || ' • p_src_table = [' || p_src_table || ']' || chr(10) 
      || ' • p_int_table = [' || p_int_table || ']' || chr(10) 
      || ' • p_trg_table = [' || p_trg_table || ']' || chr(10) 
      || ' • p_pk_column = [' || p_pk_column || ']' || chr(10) 
      || ' • p_key_columns = [' || p_key_columns || ']' || chr(10) 
      || ' • p_compare_columns = [' || p_compare_columns || ']' || chr(10) 
      || ' • p_other_columns = [' || p_other_columns || ']' || chr(10) 
      || ' • p_verbose = [' || p_verbose || ']' || chr(10) 
      || ' • p_execute = [' || p_execute || ']' || chr(10);
      
    ----dbms_output.put_line(v_proc_params_txt);
    
    -- log start -- 
    v_main_evt_id := LOG_START (p_evt_name => v_evt_name, p_table_name =>  p_trg_table, p_info =>  v_proc_params_txt, p_clob =>  null);


    v_valid_from := sysdate;
    v_valid_to_inf := to_date('9999-01-01', 'yyyy-mm-dd');

    -- select v_valid_from minus one second into v_valid_to
    select v_valid_from - interval '1' second into v_valid_to from dual;

    v_valid_from_str := to_char(v_valid_from, 'yyyy-mm-dd hh24:mi:ss');
    v_valid_to_str := to_char(v_valid_to, 'yyyy-mm-dd hh24:mi:ss');
    v_valid_to_inf_str := to_char(v_valid_to_inf, 'yyyy-mm-dd hh24:mi:ss');

    -- split key, comp and other columns by coma and put them into arrays	

    -- split key columns and put result into v_key_cols_tab
    for i in 1..regexp_count(v_key_columns, '[^,]+') loop
      v_key_cols_tab(i) := REGEXP_SUBSTR(v_key_columns, '[^,]+', 1, i);
    end loop;

    -- split compare columns and put result into v_comp_cols_tab
    for i in 1..regexp_count(v_compare_columns, '[^,]+') loop
      v_comp_cols_tab(i) := REGEXP_SUBSTR(v_compare_columns, '[^,]+', 1, i);
    end loop;

    -- split other columns and put result into v_other_cols_tab
    for i in 1..regexp_count(v_other_columns, '[^,]+') loop
      v_other_cols_tab(i) := REGEXP_SUBSTR(v_other_columns, '[^,]+', 1, i);
    end loop;
    
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- check for duplicate records in p_src_table with v_key_columns
    -- using dynamic sql
    v_duplicate_records_sql := 'SELECT COUNT(1) FROM (select count(1) from ' || p_src_table || ' WHERE 1=1 group by ' || v_key_columns || ' HAVING COUNT(1) > 1)';
    
    -- log start --
    v_child_evt_id := LOG_START (p_evt_name => v_evt_name || '_' || v_step_id, p_parent_id => v_main_evt_id, p_table_name =>  p_src_table, p_info =>  'Check for duplicates', p_clob =>  v_duplicate_records_sql);
    
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE v_duplicate_records_sql INTO v_dupl_rec_cnt;
    end if;
    
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Duplicate records sql');
      ----dbms_output.put_line(v_duplicate_records_sql);
      dbms_output.put_line(';');
    end if;
    
    LOG_END ( p_evt_id => v_child_evt_id, p_row_count => v_dupl_rec_cnt, p_info =>  '');
    
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- check that all key columns in source tables are not null
    -- if there are null values in key columns then raise exception
    -- first build sql text checking each key column is not null
    v_null_key_columns_sql := '';
    FOR i IN v_key_cols_tab.first..v_key_cols_tab.last LOOP
      IF i = 1 THEN
        v_null_key_columns_sql := 'SELECT COUNT(*) FROM ' || p_src_table || chr(10) || ' WHERE 1=1' || chr(10)
          || '  and (' || chr(10)
          || '    ' || v_key_cols_tab(i) || ' IS NULL' || chr(10);
      ELSE
        v_null_key_columns_sql := v_null_key_columns_sql || '    OR ' || v_key_cols_tab(i) || ' IS NULL' || chr(10);
      END IF;
      
      IF i = v_key_cols_tab.last then
        v_null_key_columns_sql := v_null_key_columns_sql || '  )';
      end if;
    END LOOP;
    
    -- log STEP -- Null in keys
    v_child_evt_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_main_evt_id, 
      p_table_name =>  p_src_table, 
      p_info =>  'Check for null in keys', 
      p_clob =>  v_null_key_columns_sql
    );

    -- execute sql text
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE v_null_key_columns_sql INTO v_null_key_cols_cnt;
    end if;
    
    
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Null key columns sql');
      ----dbms_output.put_line(v_null_key_columns_sql);
      dbms_output.put_line(';');
    end if;
    
    -- log STEP - END
    LOG_END ( p_evt_id => v_child_evt_id, p_row_count => v_null_key_cols_cnt, p_info =>  '');

    -- if there are duplicate records in p_src_table with v_key_columns
    -- then raise exception
    IF v_dupl_rec_cnt > 0 THEN
      RAISE_APPLICATION_ERROR(-20001, 'Duplicate records found in ' || p_src_table || ' with ' || v_key_columns);
    END IF;
    
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- log STEP --
    v_child_evt_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_main_evt_id, 
      p_table_name =>  p_src_table, 
      p_info =>  'TRUNCATE TABLE ' || p_int_table, 
      p_clob =>  'TRUNCATE TABLE ' || p_int_table
    );
    
    -- truncate intermediate table
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || p_int_table;
    end if;
    
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Truncate intermediate table');
      dbms_output.put_line('TRUNCATE TABLE ' || p_int_table);
      dbms_output.put_line(';');
    end if;
    
    LOG_END ( 
      p_evt_id => v_child_evt_id, 
      p_row_count => v_dupl_rec_cnt, 
      p_info =>  ''
    );
    
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- 1. NEW RECORDS 
    -- insert all records from source table into intermediate table that does not exists in target table

    -- build on join condition for source and target table
    -- split v_key_columns by coma
    -- for each column add join condition
    -- join condition is: t_src.column = t_trg.column
    -- if it is first column then add ON clause
    -- if it is not first column then add AND clause
    v_on_join_condition := '';
    FOR i IN v_key_cols_tab.first..v_key_cols_tab.last LOOP
      IF i = 1 THEN
        v_on_join_condition := '  ON ';
      ELSE
        v_on_join_condition := v_on_join_condition || '  AND ';
      END IF;
      v_on_join_condition := v_on_join_condition || 't_src.' || v_key_cols_tab(i) || ' = t_trg.' || v_key_cols_tab(i);
      if i < v_key_cols_tab.last then
        v_on_join_condition := v_on_join_condition || chr(10);
      end if;
    END LOOP;

    -- insert into intermediate table with action I, gnum = 1, bus_date_from = sysdate
    -- detect new records in p_src_table
    -- select all records from source table, join to target table on key columns, filter ony columns that are not in target table
    v_new_records_sql := '--+(insert new recors into intermediate table)+--' || chr(10) 
      || 'insert into ' || p_int_table || ' ( ' || chr(10)
      || '  ' || replace(v_key_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
      || '  ' || replace(v_compare_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
      || '  ' || replace(v_other_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
      || '  action, ' || chr(10)
      || '  version_id ' || chr(10)
      || ') ' || chr(10)
      || 'select ' || chr(10)
      || '  ' || replace(regexp_replace(v_key_columns,'([^,]+)','t_src.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
      || '  ' || replace(regexp_replace(v_compare_columns,'([^,]+)','t_src.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
      || '  ' || replace(regexp_replace(v_other_columns,'([^,]+)','t_src.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
      || '  ''I'' as action, ' || chr(10)
      || '  1 as version_id ' || chr(10)
      || 'from ' || p_src_table || ' t_src' || chr(10)
      || 'left join ' || p_trg_table || ' t_trg ' || chr(10)
      || v_on_join_condition || chr(10)
      || q'#  and t_trg.valid_to >= to_date('9999-01-01', 'yyyy-mm-dd') #' || chr(10)
      || 'where t_trg.' || v_pk_column || ' is null';
    
    -- log STEP --
    v_child_evt_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_main_evt_id, 
      p_table_name =>  p_src_table, 
      p_info =>  '1. New records - insert into intermediate table', 
      p_clob =>  v_new_records_sql
    );
    
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE v_new_records_sql;
    end if;
    
    -- log STEP end --
    LOG_END ( 
      p_evt_id => v_child_evt_id, 
      p_row_count => SQL%ROWCOUNT, 
      p_info =>  ''
    );
    
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Insert new records sql');
      ----dbms_output.put_line(v_new_records_sql);
      dbms_output.put_line(';');
      dbms_output.put_line('');
      dbms_output.put_line('select * from ' || p_int_table || ';');
    end if;
        
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- 2. CLOSING RECORDS 
    -- detect records that does not exists in source table anymore
    -- select all records from target table, left join to source table on key columns, filter only columns that are not in source table
    -- insert into intermediate table with action C, bus_date_from = to_date('9999-01-01', 'yyyy-mm-dd')
    v_closing_records_sql :=
      'insert into ' || p_int_table || ' ( '  || chr(10)
        || '  ' || v_pk_column || ','  || chr(10)
        || '  ' || replace(v_key_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(v_compare_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(v_other_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  action, ' || chr(10)
        || '  version_id' || chr(10)
      || ')' || chr(10)
      || 'select ' || chr(10)
        || '  t_trg.' || v_pk_column || ',' || CHR(10)
        || '  ' || replace(regexp_replace(v_key_columns,'([^,]+)','t_trg.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(regexp_replace(v_compare_columns,'([^,]+)','t_trg.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(regexp_replace(v_other_columns,'([^,]+)','t_trg.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ''C'' as action, ' || chr(10)
        || '  t_trg.version_id + 1' || chr(10)
      || 'from ' || p_trg_table || ' t_trg' || chr(10)
      || 'left join ' || p_src_table || ' t_src ' || chr(10)
      || v_on_join_condition || chr(10)
      || 'where t_trg.valid_to >= to_date(''9999-01-01'', ''yyyy-mm-dd'')' || chr(10)
      -- and first key column is null, if first is null then all are null, because key columns should not be null, check on the beginning of the procedure
      || '  and t_src.' || v_key_cols_tab(1) || ' is null'
    ;
    
    -- log STEP --
    v_child_evt_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_main_evt_id, 
      p_table_name =>  p_src_table, 
      p_info =>  '2. Closing records - insert into intermediate table', 
      p_clob =>  v_closing_records_sql
    );
    
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE v_closing_records_sql;
    end if;  
    
    -- log STEP end --
    LOG_END ( 
      p_evt_id => v_child_evt_id, 
      p_row_count => SQL%ROWCOUNT, 
      p_info =>  ''
    );
    
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Insert closing records sql');
      ----dbms_output.put_line(v_closing_records_sql);
      dbms_output.put_line(';');
    end if;
    
    
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- 3. UPDATE RECORDS
    -- detect records that are in source table and in target table
    -- select all records from source table, join to target table on key columns, filter only columns that are different in source and target table
    -- insert into intermediate table with action U, bus_date_from = sysdate
    v_comp_cols_condition := '';
    FOR i IN v_comp_cols_tab.first..v_comp_cols_tab.last LOOP
      IF i = 1 THEN
        v_comp_cols_condition := '    1=2';
      end if;
      
      v_comp_cols_condition := v_comp_cols_condition || chr(10) || '   or ' || chr(10)
        || '    case ' || chr(10)
        || '      when t_src.' || v_comp_cols_tab(i) || ' is null and t_trg.' || v_comp_cols_tab(i) || ' is null then 0 ' || chr(10)
        || '      when t_src.' || v_comp_cols_tab(i) || ' = t_trg.' || v_comp_cols_tab(i) || ' then 0 ' || chr(10)
        || '      else 1 ' || chr(10)
        || '    end = 1'
      ;
    END LOOP;

    v_update_records_sql :=
      'insert into ' || p_int_table || ' ( ' || chr(10)
        || '  ' || v_pk_column || ','  || chr(10)
        || '  ' || replace(v_key_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(v_compare_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(v_other_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  action, ' || chr(10)
        || '  version_id' || chr(10)
      || ') ' || chr(10)
      || 'select ' || chr(10)
        || '  t_trg.' || v_pk_column || ',' || chr(10)
        || '  ' || replace(regexp_replace(v_key_columns,'([^,]+)','t_src.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(regexp_replace(v_compare_columns,'([^,]+)','t_src.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(regexp_replace(v_other_columns,'([^,]+)','t_src.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ''U'' as action, ' || chr(10)
        || '  t_trg.version_id + 1 ' || chr(10)
      || '  from ' || p_src_table || ' t_src' || chr(10)
      || '  join ' || p_trg_table || ' t_trg ' || chr(10)
      || v_on_join_condition || chr(10)
      || 'where t_trg.valid_to >= to_date(''9999-01-01'', ''yyyy-mm-dd'')' || chr(10)
        || '  and t_src.' || v_key_cols_tab(1) || ' is not null' || chr(10)
        || '  and t_trg.' || v_pk_column || ' is not null' || chr(10) -- stavi pk
        || '  and (' || chr(10)
          || v_comp_cols_condition || chr(10)
        || '  )'
    ;
    
    -- log STEP --
    v_child_evt_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_main_evt_id, 
      p_table_name =>  p_src_table, 
      p_info =>  '3. Updated records - insert into intermediate table', 
      p_clob =>  v_update_records_sql
    );
    
    -- execute update records sql
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE v_update_records_sql;
    end if;
    
    -- log STEP end --
    LOG_END ( 
      p_evt_id => v_child_evt_id, 
      p_row_count => SQL%ROWCOUNT, 
      p_info =>  ''
    );
    
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Insert update records sql');
      ----dbms_output.put_line(v_update_records_sql);
      dbms_output.put_line(';');
    end if;  
    
    
    
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- 4. update target table for closing records and update records
    -- update target table for closing records and update records
    -- set bus_date_until = sysdate - 1
    -- where bus_date_until = to_date('9999-01-01', 'yyyy-mm-dd')
    -- and v_pk_column in (select v_pk_column from intermediate table where action in ('C', 'U'))
    v_update_target_table_sql :=
      'merge into ' || p_trg_table || ' t_trg' || chr(10)
      || 'using ' || p_int_table || ' t_int' || chr(10)
      || '  on (t_trg.' || v_pk_column || ' = t_int.' || v_pk_column || ' and t_int.action in (''C'', ''U''))' || chr(10)
      || 'when matched then update set ' || chr(10)
      || '  valid_to = to_date(''' || v_valid_to_str || ''', ''yyyy-mm-dd hh24:mi:ss''),' || chr(10)
      || '  is_current = 0' || chr(10)
    ;
    
    -- log STEP --
    v_child_evt_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_main_evt_id, 
      p_table_name =>  p_src_table, 
      p_info =>  '4. update target table for closing records and update records', 
      p_clob =>  v_update_target_table_sql
    );
    
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE v_update_target_table_sql;
    end if;
    
    -- log STEP end --
    LOG_END ( 
      p_evt_id => v_child_evt_id, 
      p_row_count => SQL%ROWCOUNT, 
      p_info =>  ''
    );
        
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Update target table sql');
      --dbms_output.put_line(v_update_target_table_sql);
      dbms_output.put_line(';');
    end if;
    
    -- STEP UP --
    -----------------------------------------------------------------------------------------------------------------
    v_step_id := v_step_id + 1;
    
    -- 5. insert new records, closing records, update records into target table
    v_insert_into_target_table_sql :=
      'insert into ' || p_trg_table || ' ( ' 
        || '  ' || replace(v_key_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(v_compare_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(v_other_columns,',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  valid_from, ' || chr(10)
        || '  valid_to, ' || chr(10)
        || '  version_id, ' || chr(10)
        || '  is_current, ' || chr(10)
        || '  is_deleted, ' || chr(10)
        || '  date_modified' || chr(10)
      || ') ' || chr(10)
      || 'select ' || chr(10)
        || '  ' || replace(regexp_replace(v_key_columns,'([^,]+)','t_int.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(regexp_replace(v_compare_columns,'([^,]+)','t_int.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  ' || replace(regexp_replace(v_other_columns,'([^,]+)','t_int.\1'),',',', ' || chr(10) || '  ') || ', ' || chr(10)
        || '  to_date(''' || v_valid_from_str || ''', ''yyyy-mm-dd hh24:mi:ss'') as valid_from, ' || chr(10)
        || '  to_date(''' || v_valid_to_inf_str || ''', ''yyyy-mm-dd hh24:mi:ss'') as valid_to, ' || chr(10)
        || '  t_int.version_id, ' || chr(10)
        || '  1 as is_current, ' || chr(10)
        || '  case when t_int.action = ''C'' then 1 else 0 end as is_deleted, ' || chr(10)
        || '  to_date(''' || v_valid_from_str || ''', ''yyyy-mm-dd hh24:mi:ss'') as date_modified ' || chr(10)
      || 'from ' || p_int_table || ' t_int ' || chr(10)
      || 'where t_int.action in (''I'', ''C'', ''U'')'
    ;
    
    -- log STEP --
    v_child_evt_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_main_evt_id, 
      p_table_name =>  p_src_table, 
      p_info =>  '5. insert new records, closing records, update records into target table', 
      p_clob =>  v_insert_into_target_table_sql
    );
    
    if p_execute = 'Y' then
      EXECUTE IMMEDIATE v_insert_into_target_table_sql;
    end if;
    
    -- log STEP end --
    LOG_END ( 
      p_evt_id => v_child_evt_id, 
      p_row_count => SQL%ROWCOUNT, 
      p_info =>  ''
    );
    
    if p_verbose = 'Y' then
      dbms_output.put_line('-------------------------------------------------------------------------------------------');
      dbms_output.put_line('-- Insert into target table sql');
      --dbms_output.put_line(v_insert_into_target_table_sql);
      dbms_output.put_line(';');
      
      -- printe select from target table
      dbms_output.put_line('select * from ' || p_trg_table || ' order by user_id, application_name, valid_to desc;');
      dbms_output.put_line('-- # --------------------------------------------------------------------------------------- # --');
    end if;
    
    LOG_END ( p_evt_id => v_main_evt_id, p_row_count => null, p_info =>  'END');
  exception when others then
    LOG_ERROR ( p_evt_id => v_main_evt_id);
    raise;
  end;
  
  
  -- primary key
  function fn_get_pk(p_table_name varchar2) return varchar2 is
    v_pk varchar2(128 char);
  begin
    -- ne uzimamo pk iz tablice, koji je unid, jer bude uvik null, pa ne radi dobro, ovaj pk ubacujemo u copmare atribute
    --select column_name into v_pk from groupdwh.gdwh_column where table_name = p_table_name and primary_key_flag = 'Y';
    v_pk := 'SCD2_ID';
    return v_pk;
  end;
  
  --------------------
  -- business key --
  --------------------
  function fn_get_biz_key(p_table_name varchar2) return varchar2 is
    v_biz_key varchar2(1000 char);
  begin
    select listagg(column_name, ',') within group (order by column_name) into v_biz_key 
    from groupdwh.gdwh_column where table_name = p_table_name and business_key = 'Y';
    return v_biz_key;
  end;
  
  --------------------------------------------------------------
  -- cols to compare, exclude pk, business key and natural key
  --------------------------------------------------------------
  function fn_get_comp_flds(p_table_name varchar2) return clob is
    v_comp_flds clob;
  begin
    
    for rec in (
      select 
        column_name
      from groupdwh.gdwh_column where table_name = 'ST_ACCOUNT'
        --and primary_key_flag = 'N' 
        and business_key = 'N' 
        and natural_key_flag = 'N'
        and column_name != 'ML_DATA_SET_NAME'
    ) loop
      if v_comp_flds is null then
        v_comp_flds := rec.column_name;
      else
        v_comp_flds := v_comp_flds || ',' || rec.column_name;
      end if;
    end loop;
    
    return v_comp_flds;
  end;
  
  --------------------------------------------------------------------------------
  -- cols to ignore, exclude pk, business key, leave just natural key
  --------------------------------------------------------------------------------
  function fn_get_othr_flds(p_table_name varchar2) return varchar2 is
    v_othr_flds varchar2(2000 char);
  begin
    select listagg(column_name, ',') within group (order by column_name) into v_othr_flds
    from groupdwh.gdwh_column where table_name = p_table_name and primary_key_flag = 'N' and business_key = 'N' and natural_key_flag = 'Y';
    return v_othr_flds;
  end;
  
  --------------------------------------------------------------------------------
  -- procedure to create scd2 tables
  -- when creating scd2 tables, do not use schema name, use table name only
  -- from table eg. st_account, get all columns, create src, int and trg table
  -- src table is copy of st_account, name = scd2_st_account_src, 0 rows
  -- int table is copy of st_account, name = scd2_st_account_int, 0 rows
    -- with additional columns: version_id int, action varchar2(1 char)
  -- trg table is copy of st_account, name = scd2_st_account_trg, 0 rows
    -- with additional columns: valid_from date, valid_to date, version_id int, is_current varchar2(1 char), is_deleted varchar2(1 char), date_modified date
  --------------------------------------------------------------------------------
  procedure create_scd2_tables(p_table_name varchar2, p_trg_schema varchar2) is
    v_sql varchar2(2000 char);
    v_parent_log_id number;
    v_child_log_id number;
    v_step_id int;
    v_evt_name varchar2(64 char):='PKG_SCD2.Create_Scd2_Tables';
  begin
    v_step_id := 0;
    -- log start of the procedure
    v_parent_log_id := LOG_START (p_evt_name => v_evt_name, p_table_name =>  p_table_name, p_info =>  null, p_clob =>  null);

    ---------------------
    -- create src table --
    v_step_id := v_step_id + 1;
    v_sql := 'create table ' || p_trg_schema || '.scd2_' || p_table_name || '_src as select * from PRESTAGE.' || p_table_name || ' where 1=0';
    
    v_child_log_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_parent_log_id, 
      p_table_name => 'scd2_' || p_table_name || '_src',
      p_info =>  'Create src scd2 table.', 
      p_clob =>  v_sql)
    ;
    
    execute immediate v_sql;

    LOG_END ( p_evt_id => v_child_log_id, p_row_count => null, p_info =>  '');

    ---------------------
    -- create int table
    v_step_id := v_step_id + 1;
    v_sql := 'create table ' || p_trg_schema || '.scd2_' || p_table_name || '_int as select * from PRESTAGE.' || p_table_name || ' where 1=0';

    v_child_log_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_parent_log_id, 
      p_table_name =>  'scd2_' || p_table_name || '_int',
      p_info =>  'Create int scd2 table.', 
      p_clob =>  v_sql
    );

    execute immediate v_sql;

    LOG_END ( p_evt_id => v_child_log_id, p_row_count => null, p_info =>  '');
    
    ---------------------
    -- create trg table
    v_step_id := v_step_id + 1;
    v_sql := 'create table ' || p_trg_schema || '.scd2_' || p_table_name || '_trg as select * from PRESTAGE.' || p_table_name || ' where 1=0';

    v_child_log_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_parent_log_id, 
      p_table_name =>  'scd2_' || p_table_name || '_trg', 
      p_info =>  'Create trg scd2 table.', 
      p_clob =>  v_sql
    );

    execute immediate v_sql;

    LOG_END ( p_evt_id => v_child_log_id, p_row_count => null, p_info =>  '');
    
    ------------------------------------------
    -- add additional columns to int table
    v_step_id := v_step_id + 1;
    v_sql := 'alter table ' || p_trg_schema || '.scd2_' || p_table_name || '_int add (version_id int, action varchar2(1 char), scd2_id int)';

    v_child_log_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_parent_log_id, 
      p_table_name =>  'scd2_' || p_table_name || '_int', 
      p_info =>  'Add additional columns to int scd2 table.', 
      p_clob =>  v_sql
    );

    execute immediate v_sql;

    LOG_END ( p_evt_id => v_child_log_id, p_row_count => null, p_info =>  '');

    ------------------------------------------
    -- add additional columns to trg table
    v_step_id := v_step_id + 1;
    v_sql := 'alter table ' || p_trg_schema || '.scd2_' || p_table_name || '_trg add (' ||
      ' valid_from date,'  ||
      ' valid_to date, ' ||
      ' version_id int, ' ||
      ' is_current varchar2(1 char), ' ||
      ' is_deleted varchar2(1 char), ' ||
      ' date_modified date,' ||
      ' SCD2_ID INT GENERATED BY DEFAULT AS IDENTITY START WITH 1 INCREMENT BY 1 ORDER NOCYCLE' ||
    ')'
    ;

    v_child_log_id := LOG_START (
      p_evt_name => v_evt_name || '_' || v_step_id, 
      p_parent_id => v_parent_log_id, 
      p_table_name =>  'scd2_' || p_table_name || '_trg', 
      p_info =>  'Add additional columns to trg scd2 table.', 
      p_clob =>  v_sql
    );

    execute immediate v_sql;

    LOG_END ( p_evt_id => v_child_log_id, p_row_count => null, p_info =>  '');

    -- log end of the procedure
    LOG_END ( p_evt_id => v_parent_log_id, p_row_count => null, p_info =>  '');
  exception when others then
    LOG_ERROR ( p_evt_id => v_parent_log_id);
    raise;
  end;
  
  ---------------
  -- LOG_START --
  ---------------
  FUNCTION LOG_START (
    p_evt_name in varchar2,
    p_parent_id in int:=null,
    p_table_name in varchar2:=null,
    p_info in varchar2:=null, 
    p_clob in clob:= null
  ) return number AS
    l_evt_id int;
    l_clob_id int;
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin

    if p_clob is not null then
      insert into HRAGSL.scd2_CLOB_LOG (EVT_NAME, LOG_CLOB) values (substr(p_evt_name,1,128), p_clob) returning id into l_clob_id;
      null;
    end if;
    
    INSERT INTO HRAGSL.scd2_EVT_LOG (id, evt_name, table_name, INFO_MESSAGE, clob_id, parent_id)
    values (HRAGSL.SEQ_scd2_EVT_LOG.nextval, p_evt_name, p_table_name, substr(p_info,1,500), l_clob_id, p_parent_id) returning id into l_evt_id;
    COMMIT;

    return l_evt_id;
  END LOG_START;

  ---------------
  -- LOG_END --
  ---------------
  PROCEDURE LOG_END (p_evt_id in int, p_row_count int:=null, p_info in varchar2:=null) AS
    l_end_time timestamp;
    l_begin_time timestamp;
    l_duration_sec number(20,6);
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin
    l_end_time:=systimestamp;

    UPDATE HRAGSL.scd2_EVT_LOG
    SET DATETIME_END = l_end_time,
        duration_sec = HRAGSL.pkg_utils.timestamp_diff_seconds(l_end_time,DATETIME_START),
        STATUS='OK',
        ROW_COUNT = p_row_count,
        INFO_MESSAGE = substr(INFO_MESSAGE || p_info || ' [duration: ' || to_char(l_end_time-DATETIME_START) || ']',1,500)
    where ID = p_evt_id;

    commit;
  END LOG_END;

  ---------------
  -- LOG_ERROR --
  ---------------
  PROCEDURE LOG_ERROR (p_evt_id in int) AS
    l_end_time timestamp;
    l_begin_time timestamp;
    l_duration_sec number(20,6);
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin
    l_end_time:=systimestamp;

    UPDATE HRAGSL.scd2_EVT_LOG
    SET DATETIME_END = l_end_time,
        duration_sec = HRAGSL.pkg_utils.timestamp_diff_seconds(l_end_time,DATETIME_START),
        STATUS='ERROR',
        ERROR_MESSAGE=substr(DBMS_UTILITY.format_error_stack||chr(10)||chr(10)||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,2000)
    where ID=p_evt_id;

    commit;
  END LOG_ERROR;

END PKG_SCD2;
