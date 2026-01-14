create or replace FUNCTION fn_CLOB2BLOB (AClob CLOB) return BLOB is
  Result BLOB;
  o1 integer;
  o2 integer;
  c integer;
  w integer;
begin
  o1 := 1;
  o2 := 1;
  c := 0;
  w := 0;
  DBMS_LOB.CreateTemporary(Result, true);
  DBMS_LOB.ConvertToBlob(Result, AClob, length(AClob), o1, o2, 0, c, w);
  return(Result);
end;
