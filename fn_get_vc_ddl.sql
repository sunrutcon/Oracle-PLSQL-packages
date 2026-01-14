-- create table with all varchar columns
-- used to import data from excel when formats are not ok
-- then we transform data in sql and insert into target table
create or replace function fn_get_vc_ddl( p_schema varchar2, p_table  varchar2) return clob is
  --
  l_ddl clob;
  --
begin
  --
  l_ddl := 'CREATE TABLE ' || p_schema || '.' || p_table || '_VC (';
  --
  for rec in (select * from all_tab_cols where owner = p_schema and table_name = p_table order by column_id) loop
      if rec.column_id > 1 then
        l_ddl := l_ddl || ',';
      end if;
      l_ddl := l_ddl || chr(10) || '  ' || rec.column_name || ' varchar2(1024 char)';
  end loop;
  --
  l_ddl := l_ddl || chr(10) || ');';
  --
  --dbms_output.put_line(l_ddl);
  return l_ddl;
  --
end;
