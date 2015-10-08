IF  object_id('tempdb..##tmp_set1') is not null DROP TABLE ##tmp_set1
go
select 
	sum(execution_count) as execution_count,min(creation_time) as creation_time,max(last_execution_time) as last_execution_time
	,sum(total_worker_time) as total_worker_time,sum(total_physical_reads) as total_physical_reads
	,sum(total_logical_writes) as total_logical_writes,sum(total_logical_reads) as total_logical_reads,sum(total_clr_time) as total_clr_time
	,sum(total_elapsed_time) as total_elapsed_time,sum(total_rows) as total_rows
	,sql_handle,statement_start_offset,statement_end_offset,max(query_hash) as query_hash
	,max(plan_generation_num) as plan_generation_num,max(plan_handle) as plan_handle, max(query_plan_hash) as query_plan_hash
	,count(*) as NoOfExePlans, getdate() as runtime
into ##tmp_set1
from sys.dm_exec_query_stats as QueryStats
group by sql_handle,statement_start_offset,statement_end_offset
go

-- select * from ##tmp_set1
IF  object_id('tempdb..##tmp_set_diff_temp') is not null DROP TABLE ##tmp_set_diff_temp
go
select * into ##tmp_set_diff_temp from ##tmp_set1 where 1=2
GO
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by execution_count desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_worker_time/execution_count desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_worker_time desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_physical_reads/execution_count desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_physical_reads desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_logical_writes desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_logical_writes/execution_count desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_logical_reads desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_logical_reads/execution_count desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_logical_reads desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_rows desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_elapsed_time desc

insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_clr_time/execution_count desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by total_clr_time desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set1 order by NoOfExePlans desc

IF  object_id('tempdb..##tmp_set_diff') is not null DROP TABLE ##tmp_set_diff
go
select distinct * into ##tmp_set_diff from ##tmp_set_diff_temp 
GO
-- select * from ##tmp_set_diff 
delete from ##tmp_set_diff where execution_count<10 and total_worker_time<10001
GO
IF  object_id('tempdb..##tmp_set_diff_Detail') is not null DROP TABLE ##tmp_set_diff_Detail
go
select 
	SUBSTRING(QueryText.text, (statement_start_offset /2)+1, 
	(isnull(CASE statement_end_offset
			  WHEN -1 THEN DATALENGTH(QueryText.text)
			  WHEN 0 THEN DATALENGTH(QueryText.text)
			 ELSE statement_end_offset
			 END,'') - statement_start_offset/2)) AS QueryExecuted
	,QueryStats.*
	,QueryText.dbid as QueryText_DatabaseID, db_name(QueryText.dbid) as QueryText_DatabaseName,QueryText.objectid as QueryText_Objectid,object_name(QueryText.objectid, QueryText.dbid) as QueryText_ObjectName
	,OBJECT_SCHEMA_NAME(QueryText.objectid, QueryText.dbid) AS QueryText_schema_name
	,QueryText.number as QueryText_number,QueryText.encrypted as QueryText_encrypted,QueryText.text as QueryText_text
into ##tmp_set_diff_Detail
from ##tmp_set_diff as QueryStats
cross apply sys.dm_exec_sql_text(QueryStats.sql_handle) as QueryText

IF  object_id('tempdb..##tmp_set_diff_Detail_plan') is not null DROP TABLE ##tmp_set_diff_Detail_plan
go
select QueryStats.*
	,QueryPlan.dbid as QueryPlan_DatabaseID, db_name(QueryPlan.dbid) as QueryPlan_DatabaseName,QueryPlan.objectid as QueryPlan_Objectid,object_name(QueryPlan.objectid, QueryPlan.dbid) as QueryPlan_ObjectName
	,QueryPlan.number,QueryPlan.encrypted,QueryPlan.query_plan,runtime as PostRunime
into ##tmp_set_diff_Detail_plan
from ##tmp_set_diff_Detail QueryStats
outer apply sys.dm_exec_query_plan(QueryStats.plan_handle) QueryPlan
GO

select 
	query_plan,QueryExecuted,isnull(QueryText_DatabaseName,'PreparedSQL') as QueryText_DatabaseName,QueryText_ObjectName,QueryText_schema_name,execution_count,total_worker_time/execution_count as AvgExecution,total_worker_time,total_physical_reads,total_logical_writes,total_logical_reads,total_clr_time,total_elapsed_time,total_rows,sql_handle,statement_start_offset,statement_end_offset,query_hash,plan_generation_num,plan_handle,query_plan_hash,NoOfExePlans,runtime,QueryText_DatabaseID,PostRunime
from ##tmp_set_diff_Detail_plan
order by total_worker_time desc

