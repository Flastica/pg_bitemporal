CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_insert(p_table text
,p_list_of_fields text
,p_list_of_values TEXT
,p_effective bitemporal_internal.timeperiod 
,p_asserted bitemporal_internal.timeperiod ) 
RETURNS INTEGER
AS
 $BODY$
DECLARE
v_rowcount INTEGER;
BEGIN
 EXECUTE format ($i$INSERT INTO %s (%s, effective, asserted )  
                 VALUES (%s,%L,%L) RETURNING * $i$
                ,p_table
                ,p_list_of_fields
                ,p_list_of_values
                ,p_effective
                ,p_asserted) ;
     GET DIAGNOSTICS v_rowcount:=ROW_COUNT; 
     RETURN v_rowcount;         
     END;    
$BODY$ LANGUAGE plpgsql;

