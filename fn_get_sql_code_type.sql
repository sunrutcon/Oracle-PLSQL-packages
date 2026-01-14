create or replace function fn_get_sql_code_type(p_sql_code in int) return varchar2 as
--declare
  TYPE t_assoc_array_int_str IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER; -- dictionary: key - int, value - string
  l_code_type t_assoc_array_int_str;
  l_return varchar2(50 char);
begin
  l_code_type(0)   := 'SUCCESS';
  l_code_type(1)   := 'VARCHAR';
  l_code_type(2)   := 'NUMBER';
  l_code_type(9)   := 'VARCHAR2';
  l_code_type(2)   := 'DATE';
  l_code_type(58)  := 'OPAQUE';
  l_code_type(95)  := 'RAW';
  l_code_type(96)  := 'CHAR';
  l_code_type(105) := 'MLSLABEL';
  l_code_type(113) := 'BLOB';
  l_code_type(114) := 'BFILE';
  l_code_type(112) := 'CLOB';
  l_code_type(115) := 'CFILE';
  l_code_type(187) := 'TIMESTAMP';
  l_code_type(188) := 'TIMESTAMP_TZ';
  l_code_type(232) := 'TIMESTAMP_LTZ';
  l_code_type(189) := 'INTERVAL_YM';
  l_code_type(190) := 'INTERVAL_DS';
  l_code_type(110) := 'REF';
  l_code_type(108) := 'OBJECT';
  l_code_type(247) := 'VARRAY';
  l_code_type(248) := 'TABLE';
  l_code_type(122) := 'NAMEDCOLLECTION';
  l_code_type(286) := 'NCHAR';
  l_code_type(287) := 'NVARCHAR2';
  l_code_type(288) := 'NCLOB';
  l_code_type(100) := 'BFLOAT';
  l_code_type(101) := 'BDOUBLE';
  l_code_type(104) := 'UROWID';
  l_code_type(100) := 'NO_DATA';
  if l_code_type.exists(p_sql_code) then
    l_return := l_code_type(p_sql_code);
  else
    l_return := null;
  end if;

  return l_return;
end;
