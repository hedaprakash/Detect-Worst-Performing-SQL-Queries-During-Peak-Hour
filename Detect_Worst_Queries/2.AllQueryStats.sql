select 
	@@servername as servername
	,isnull(db_name(QueryText.dbid),'PreparedSQL') as DBName 
    ,SUBSTRING(QueryText.text, (QueryStats.statement_start_offset/2)+1, 
		(isnull((
			CASE QueryStats.statement_end_offset
			  WHEN -1 THEN DATALENGTH(QueryText.text)
			  WHEN 0 THEN DATALENGTH(QueryText.text)
			 ELSE QueryStats.statement_end_offset
			 END - QueryStats.statement_start_offset),0)/2) 
			 + 1)  AS QueryExecuted
	,total_worker_time AS total_worker_time
	,QueryStats.execution_count as execution_count
	,total_worker_time /execution_count as AvgCPUTime
	,statement_start_offset,statement_end_offset
	,(case when QueryText.dbid is null then OBJECT_NAME(QueryText.objectid)  else OBJECT_NAME(QueryText.objectid, QueryText.dbid)  end) as ObjectName
	,OBJECT_SCHEMA_NAME(QueryText.objectid, QueryText.dbid) AS QueryText_schema_name
	,query_hash
	,plan_handle
	,sql_handle
from sys.dm_exec_query_stats as QueryStats
cross apply sys.dm_exec_sql_text(QueryStats.sql_handle) as QueryText
-- order by execution_count desc
order by AvgCPUTime desc

-- select * from sys.dm_exec_query_stats as QueryStats where query_hash = 0x044DE9A79CE03870

