/*------------------------------------------------------------------------------+    
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :  |    
#|{>/------------------------------------------------------------------------\<}| 
#|: | Script Name:	FindTopQueriesInPeakHour.sql								| 
#|: | Author	 :	Prakash Heda												| 
#|: | Email		 :	prakash@sqlfeatures.com	 Blog: http://www.sqlfeatures.com	|
#|: | Description:	This script compares 2 snapshot of top CPU consuming queries|
#|: | 				and return queries executed in last 5 min					|
#|: | SQL Version:	SQL 2012, SQL 2008 R2, SQL 2008								|
#|: | Copyright	 :	Free to use and share 	/^(o.o)^\							|
#|: | 																			|
#|: | Create Date:	01-15-2012  Version: 1.0        							|
#|: | Revision	 :	01-19-2012  Version: 1.1  updated standard table aliases	|
#|:	| History		02-21-2012  Version: 1.2  Updated criteria					|
#|{>\------------------------------------------------------------------------/<}|  
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :	|  
#+-----------------------------------------------------------------------------*/  

use tempdb
go
declare @ServerName varchar(200) ,@CurrentRuntime datetime, @OldRuntime datetime; set @ServerName='VSACSQLBAK02'
SELECT	@CurrentRuntime = MAX(runtime)  FROM [dbastuff].[dbo].[CollectTopCPUQueries]  where [servername] = @ServerName
SELECT	@OldRuntime = MAX(runtime)  FROM [dbastuff].[dbo].[CollectTopCPUQueries]  where [servername] = @ServerName
and runtime < DATEADD(ss,-5,@CurrentRuntime)

if object_id('tempdb..##tmpServerCompare') is not null drop table ##tmpServerCompare 
create table ##tmpServerCompare 
(
ServerName varchar(200),
CurrentRuntime datetime,
OldRuntime datetime
)
insert into ##tmpServerCompare values (@ServerName,@CurrentRuntime , @OldRuntime )
select * from ##tmpServerCompare
go
declare @ServerName varchar(200),@CurrentRuntime datetime, @OldRuntime datetime; select @ServerName = ServerName,@CurrentRuntime = CurrentRuntime, @OldRuntime = OldRuntime from ##tmpServerCompare
if object_id('tempdb..##topQueries_before') is not null drop table ##topQueries_before
/****** Script for SelectTopNRows command from SSMS  ******/
SELECT  [servername]
      ,[runtime]
      ,[DBName]
      ,[QueryExecuted]
      ,[AvgCPUTime]
      ,[SamePlanUsed]
      ,[ObjectName]
into ##topQueries_before
  FROM [dbastuff].[dbo].[CollectTopCPUQueries]
  where [servername] = @ServerName 
  and [runtime] in (@OldRuntime)
  order by [servername]

if object_id('tempdb..##topQueries_after') is not null drop table ##topQueries_after
SELECT  [servername]
      ,[runtime]
      ,[DBName]
      ,[QueryExecuted]
      ,[AvgCPUTime]
      ,[SamePlanUsed]
      ,[ObjectName]
into ##topQueries_after
  FROM [dbastuff].[dbo].[CollectTopCPUQueries]
  where [servername] = @ServerName
 and [runtime] in (@CurrentRuntime)
  order by [servername]
  
if object_id('tempdb..##topQueries_after_summ') is not null drop table ##topQueries_after_summ 
select 
	servername,runtime,DBName,QueryExecuted,sum(AvgCPUTime) as AvgCPUTime,sum(SamePlanUsed) as SamePlanUsed,ObjectName
into ##topQueries_after_summ 
from ##topQueries_after 
group by servername,runtime,DBName,QueryExecuted,ObjectName 

if object_id('tempdb..##topQueries_before_summ') is not null drop table ##topQueries_before_summ
select 
	servername,runtime,DBName,QueryExecuted,sum(AvgCPUTime) as AvgCPUTime,sum(SamePlanUsed) as SamePlanUsed,ObjectName
into ##topQueries_before_summ 
from ##topQueries_before  
group by servername,runtime,DBName,QueryExecuted,ObjectName 

if object_id('tempdb..##topQueries_summ_diff') is not null drop table ##topQueries_summ_diff 
select 
	a.servername,a.runtime as CurrentRuntime,b.runtime as oldRuntime,a.DBName,a.QueryExecuted,a.AvgCPUTime as AvgCPUTime,(a.AvgCPUTime-b.AvgCPUTime) as AvgCPUTimeDiff,a.SamePlanUsed,(a.SamePlanUsed-b.SamePlanUsed) as SamePlanUsedDiff,a.ObjectName
into ##topQueries_summ_diff 
from ##topQueries_after_summ a
join  dbo.##topQueries_before_summ b
on a.QueryExecuted = b.QueryExecuted

select * from ##topQueries_summ_diff
where SamePlanUsedDiff>1

