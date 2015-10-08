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

--select * from sys.dm_exec_query_stats as QueryStats where query_hash = 0x044DE9A79CE03870
GO
WAITFOR DELAY '00:00:05'
GO
IF  object_id('tempdb..##tmp_set2') is not null DROP TABLE ##tmp_set2
go
select 
	sum(execution_count) as execution_count,min(creation_time) as creation_time,max(last_execution_time) as last_execution_time
	,sum(total_worker_time) as total_worker_time,sum(total_physical_reads) as total_physical_reads
	,sum(total_logical_writes) as total_logical_writes,sum(total_logical_reads) as total_logical_reads,sum(total_clr_time) as total_clr_time
	,sum(total_elapsed_time) as total_elapsed_time,sum(total_rows) as total_rows
	,sql_handle,statement_start_offset,statement_end_offset,max(query_hash) as query_hash
	,max(plan_generation_num) as plan_generation_num,max(plan_handle) as plan_handle, max(query_plan_hash) as query_plan_hash
	,count(*) as NoOfExePlans, getdate() as runtime
into ##tmp_set2
from sys.dm_exec_query_stats as QueryStats
group by sql_handle,statement_start_offset,statement_end_offset,query_hash
GO

IF  object_id('tempdb..##tmp_set_diff_all') is not null DROP TABLE ##tmp_set_diff_all
go
-- Showing queries executed in last 5 min with execution stats
select 
isnull(b.execution_count,0) as Total_execution_count
,a.execution_count-isnull(b.execution_count,0) as diff_execution_count
,(a.total_worker_time - isnull(b.total_worker_time,0)) /(a.execution_count-isnull(b.execution_count,0)) as AvgCPUTime
,(a.total_worker_time - isnull(b.total_worker_time,0))  as TotCPUTime
,(a.total_physical_reads - isnull(b.total_physical_reads,0)) /(a.execution_count-isnull(b.execution_count,0)) as AvgPhysicalRead
,(a.total_physical_reads - isnull(b.total_physical_reads,0))  as TotPhysicalRead
,(a.total_logical_reads - isnull(b.total_logical_reads,0)) /(a.execution_count-isnull(b.execution_count,0)) as AvgLogicalRead
,(a.total_logical_reads - isnull(b.total_logical_reads,0)) as TotLogicalRead
,(a.total_logical_writes - isnull(b.total_logical_writes,0)) /(a.execution_count-isnull(b.execution_count,0)) as AvgLogicalWrite
,(a.total_logical_writes - isnull(b.total_logical_writes,0)) as TotLogicalwrite
,(a.total_clr_time - isnull(b.total_clr_time,0)) /(a.execution_count-isnull(b.execution_count,0)) as Avg_clr_time
,(a.total_clr_time - isnull(b.total_clr_time,0))  as Tot_clr_time
,(a.total_rows - isnull(b.total_rows,0)) /(a.execution_count-isnull(b.execution_count,0)) as AvgRows
,(a.total_rows - isnull(b.total_rows,0))  as TotRows
,(a.NoOfExePlans - isnull(b.NoOfExePlans,0)) as DiffNoOfExePlans
,a.NoOfExePlans  as FinalNoOfExePlans
,a.*,b.runtime as PreRunime
,isnull(convert(varchar(200),datediff(ss,b.runtime, a.runtime)),'New') as PollInt
into ##tmp_set_diff_all
from ##tmp_set2 a
left outer join ##tmp_set1 b
on a.sql_handle=b.sql_handle
and a.statement_start_offset=b.statement_start_offset
and a.statement_end_offset=b.statement_end_offset
where  (a.execution_count > isnull(b.execution_count,0)
or b.execution_count is null)
and a.execution_count-isnull(b.execution_count,0)>1


IF  object_id('tempdb..##tmp_set_diff_temp') is not null DROP TABLE ##tmp_set_diff_temp
go
select * into ##tmp_set_diff_temp from ##tmp_set_diff_all where 1=2
GO

insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by diff_execution_count desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by AvgCPUTime desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by TotCPUTime desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by AvgPhysicalRead desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by TotPhysicalRead desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by AvgLogicalRead desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by TotLogicalRead desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by AvgRows desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by TotRows desc

insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by Avg_clr_time desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by Tot_clr_time desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by DiffNoOfExePlans desc
insert into ##tmp_set_diff_temp select top 5 * from ##tmp_set_diff_all order by FinalNoOfExePlans desc

IF  object_id('tempdb..##tmp_set_diff') is not null DROP TABLE ##tmp_set_diff
go
select distinct * into ##tmp_set_diff from ##tmp_set_diff_temp 
GO

delete from ##tmp_set_diff where diff_execution_count<10 and AvgCPUTime<1001


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
	,QueryPlan.dbid as QueryPlan_DatabaseID, db_name(QueryPlan.dbid) as QueryPlan_DatabaseName,QueryPlan.objectid as QueryPlan_Objectid
	,object_name(QueryPlan.objectid, QueryPlan.dbid) as QueryPlan_ObjectName
	,QueryPlan.number,QueryPlan.encrypted,QueryPlan.query_plan,runtime as PostRunime
into ##tmp_set_diff_Detail_plan
from ##tmp_set_diff_Detail QueryStats
outer apply sys.dm_exec_query_plan(QueryStats.plan_handle) QueryPlan

GO

--select * from ##tmp_set_diff_all

IF  object_id('master..TopQueries') is not null DROP TABLE master..TopQueries
go
select 
	PollInt,query_plan,QueryExecuted,QueryText_DatabaseName,QueryText_ObjectName,QueryText_schema_name,Total_execution_count,diff_execution_count,AvgCPUTime,TotCPUTime,AvgPhysicalRead,TotPhysicalRead,AvgLogicalRead,TotLogicalRead,AvgLogicalWrite,TotLogicalwrite,AvgRows,TotRows,Avg_clr_time,Tot_clr_time,DiffNoOfExePlans,FinalNoOfExePlans,sql_handle,statement_start_offset,statement_end_offset,plan_handle,PreRunime,PostRunime
into master..TopQueries
from ##tmp_set_diff_Detail_plan
--where diff_execution_count>20 or TotCPUTime>5000 or TotLogicalRead>5000 or TotPhysicalRead>2000
order by AvgCPUTime desc

select * from master..TopQueries order by TotCPUTime desc

