/*------------------------------------------------------------------------------+    
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :  |    
#|{>/------------------------------------------------------------------------\<}| 
#|: | Script Name:	FindTopDiskQueries.sql										| 
#|: | Author	 :	Prakash Heda												| 
#|: | Email		 :	prakash@sqlfeatures.com	 Blog: http://www.sqlfeatures.com	|
#|: | Description:	This script return top queries taxing sql server 			|
#|: | 				disks drives												|
#|: | SQL Version:	SQL 2012, SQL 2008 R2, SQL 2008								|
#|: | Copyright	 :	Free to use and share 	/^(o.o)^\							|
#|: | 																			|
#|: | Create Date:	01-15-2012  Version: 1.0        							|
#|: | Revision	 :	01-20-2012  Version: 1.1  updated with standard variables   |
#|:	| History		02-21-2012  Version: 1.2  updated with query_hash logic		|
#|{>\------------------------------------------------------------------------/<}|  
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :	|  
#+-----------------------------------------------------------------------------*/  
use tempdb
go
IF  object_id('tempdb..##FindTopDiskQueries_set1') is not null DROP TABLE [dbo].[##FindTopDiskQueries_set1]
GO
declare @ServerTime datetime = getdate()
, @ConvertMiliSeconds bigint = 1000
, @FilterMoreThanMiliSeconds bigint = 1
, @FilterHours bigint = 20
, @execution_count bigint = 1
, @debugFlg bit = 0

if @debugFlg=1  select @ServerTime as ServerTime, @ConvertMiliSeconds as ConvertMiliSeconds
, @FilterMoreThanMiliSeconds  as FilterMoreThanMiliSeconds, @FilterHours as FilterHours 
, @execution_count as execution_count

declare @topqueries table (query_hash varbinary(64))
insert into @topqueries
	select top 100 QueryStatsBaseTable.query_hash 
	from sys.dm_exec_query_stats QueryStatsBaseTable
	where 
	(QueryStatsBaseTable.total_logical_reads/QueryStatsBaseTable.execution_count)/@ConvertMiliSeconds > @FilterMoreThanMiliSeconds
	AND last_execution_time > DATEADD(hh,-@FilterHours,getdate())
	order by (QueryStatsBaseTable.total_logical_reads/QueryStatsBaseTable.execution_count) desc

insert into @topqueries
select top 100 QueryStatsBaseTable.query_hash 
	from sys.dm_exec_query_stats QueryStatsBaseTable
	where 
	(QueryStatsBaseTable.total_physical_reads/QueryStatsBaseTable.execution_count) /@ConvertMiliSeconds > @FilterMoreThanMiliSeconds
	AND last_execution_time > DATEADD(hh,-@FilterHours,getdate())
	order by (QueryStatsBaseTable.total_physical_reads/QueryStatsBaseTable.execution_count) desc

insert into @topqueries
select top 100 QueryStatsBaseTable.query_hash 
	from sys.dm_exec_query_stats QueryStatsBaseTable
	where 
	(QueryStatsBaseTable.total_logical_writes/QueryStatsBaseTable.execution_count)/@ConvertMiliSeconds > @FilterMoreThanMiliSeconds
	AND last_execution_time > DATEADD(hh,-@FilterHours,getdate())
	order by (QueryStatsBaseTable.total_logical_writes/QueryStatsBaseTable.execution_count) desc
if @debugFlg=1  select * from @topqueries order by 1

select TOP 100
	@@servername as servername,@ServerTime as runtime,
	isnull(db_name(QueryText.dbid),'PreparedSQL') as DBName 
    ,SUBSTRING(QueryText.text, (QueryStats.statement_start_offset/2)+1, 
		(isnull((
			CASE QueryStats.statement_end_offset
			  WHEN -1 THEN DATALENGTH(QueryText.text)
			  WHEN 0 THEN DATALENGTH(QueryText.text)
			 ELSE QueryStats.statement_end_offset
			 END - QueryStats.statement_start_offset),0)/2) 
			 + 1)  AS QueryExecuted
	,(total_logical_reads) AS total_logical_reads
	,(total_physical_reads) AS total_physical_reads
	,(total_logical_writes) AS total_logical_writes
	,QueryStats.execution_count as execution_count
	,(case when QueryText.dbid is null then OBJECT_NAME(QueryText.objectid)  else OBJECT_NAME(QueryText.objectid, QueryText.dbid)  end) as ObjectName
	,query_hash
	,plan_handle
	,sql_handle
into ##FindTopDiskQueries_set1
from sys.dm_exec_query_stats as QueryStats
cross apply sys.dm_exec_sql_text(QueryStats.sql_handle) as QueryText
where QueryStats.query_hash IN 
(
select distinct query_hash from @topqueries
)
ORDER BY total_physical_reads/execution_count DESC;

if @debugFlg=1  select * from ##FindTopDiskQueries_set1 order by QueryExecuted

IF  object_id('tempdb..##FindTopDiskQueries_set2') is not null DROP TABLE [dbo].[##FindTopDiskQueries_set2]

select 
	servername,runtime,max(DBName) as DBName,max(QueryExecuted) as QueryExecuted
	,sum(execution_count) as execution_count
	,(sum(total_physical_reads)/sum(execution_count))/@ConvertMiliSeconds as Avg_total_physical_reads
	,(sum(total_logical_reads)/sum(execution_count))/@ConvertMiliSeconds as Avg_total_logical_reads
	,(sum(total_logical_writes)/sum(execution_count))/@ConvertMiliSeconds as Avg_total_logical_writes
	,query_hash
	,max(ObjectName) as ObjectName
into ##FindTopDiskQueries_set2
from ##FindTopDiskQueries_set1
group by query_hash,servername,runtime
order by Avg_total_physical_reads desc


select * from ##FindTopDiskQueries_set2
--where QueryExecuted like 'select TOP 300%'
order by Avg_total_physical_reads desc


