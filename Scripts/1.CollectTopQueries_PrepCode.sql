/*------------------------------------------------------------------------------+    
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :  |    
#|{>/------------------------------------------------------------------------\<}| 
#|: | Script Name:	CollectTopQueries_PrepCode.sql								| 
#|: | Author	 :	Prakash Heda												| 
#|: | Email		 :	prakash@sqlfeatures.com	 Blog: http://www.sqlfeatures.com	|
#|: | Description:	This script prepares pre steps for this exercise			|
#|: | 																			|
#|: | SQL Version:	SQL 2012, SQL 2008 R2, SQL 2008								|
#|: | Copyright	 :	Free to use and share 	/^(o.o)^\							|
#|: | 																			|
#|: | Create Date:	01-15-2012  Version: 1.0        							|
#|: | Revision	 :	01-21-2012  Version: 1.1  added proc procGenerateLoad code	|
#|:	| History		02-19-2012  Version: 1.2  updated test data population logic|
#|{>\------------------------------------------------------------------------/<}|  
#| = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :	|  
#+-----------------------------------------------------------------------------*/  

USE dbastuff
GO
set nocount on
go
IF  object_id('CollectTopCPUQueries') is not null DROP TABLE [dbo].[CollectTopCPUQueries]
GO

CREATE TABLE [dbo].[CollectTopCPUQueries](
	[servername] [nvarchar](2000) NULL,
	[runtime] [datetime] NULL,
	[DBName] [nvarchar](2000) NOT NULL,
	[QueryExecuted] [nvarchar](max) NULL,
	[AvgCPUTime] [bigint] NULL,
	[SamePlanUsed] [bigint] NOT NULL,
	[Query_Hash] [nvarchar](2000) NOT NULL,
	[ObjectName] [nvarchar](2000) NULL
) ON [PRIMARY]

GO

IF  object_id('TopCpuQueries') is not null DROP TABLE [dbo].TopCpuQueries
GO


CREATE TABLE [dbo].[TopCpuQueries](
	[servername] [nvarchar](2000) NULL,
	[CurrentRuntime] [datetime] NULL,
	[oldRuntime] [datetime] NULL,
	[DBName] [nvarchar](2000) NOT NULL,
	[QueryExecuted] [nvarchar](max) NULL,
	[AvgCPUTime] [bigint] NULL,
	[AvgCPUTimeDiff] [bigint] NULL,
	[SamePlanUsed] [bigint] NULL,
	[SamePlanUsedDiff] [bigint] NULL,
	[ObjectName] [nvarchar](2000) NULL
) ON [PRIMARY]

GO


USE testdb
GO

IF  object_id('procGenerateLoad') is not null DROP proc  [dbo].procGenerateLoad
GO

create proc procGenerateLoad as 
select top 320000 * from testdb..TestTableSet3

delete from testdb..TestTableSet4
GO



use testdb

go

IF  object_id('TestTableSet2') is not null DROP TABLE [dbo].TestTableSet2
GO
create table TestTableSet2
(
	MyKeyField bigint,
	MyDate1 datetime,
	MyDate2 datetime,
	MyDate3 datetime,
	MyDate4 datetime,
	MyDate5 datetime
)
go
IF  object_id('TestTableSet3') is not null DROP TABLE [dbo].TestTableSet3
GO
create table TestTableSet3
(
	MyKeyField bigint,
	MyDate1 datetime,
	MyDate2 datetime,
	MyDate3 datetime,
	MyDate4 datetime,
	MyDate5 datetime
)
go
IF  object_id('TestTableSet4') is not null DROP TABLE [dbo].TestTableSet4
GO
create table TestTableSet4
(
	MyKeyField bigint,
	MyDate1 datetime,
	MyDate2 datetime,
	MyDate3 datetime,
	MyDate4 datetime,
	MyDate5 datetime
)
go


DECLARE @RowCount INT
DECLARE @RowString VARCHAR(10)
DECLARE @Random INT
DECLARE @Upper INT
DECLARE @Lower INT
DECLARE @InsertDate DATETIME

SET @Lower = -730
SET @Upper = -1
SET @RowCount = 0

WHILE @RowCount < 3000000
BEGIN
	SET @RowString = CAST(@RowCount AS VARCHAR(10))
	SELECT @Random = ROUND(((@Upper - @Lower -1) * RAND() + @Lower), 0)
	SET @InsertDate = DATEADD(dd, @Random, GETDATE())
	
	INSERT INTO TestTableSet3
		(MyKeyField
		,MyDate1
		,MyDate2
		,MyDate3
		,MyDate4
		,MyDate5)
	VALUES
		(REPLICATE('0', 10 - DATALENGTH(@RowString)) + @RowString
		, @InsertDate
		,DATEADD(dd, 1, @InsertDate)
		,DATEADD(dd, 2, @InsertDate)
		,DATEADD(dd, 3, @InsertDate)
		,DATEADD(dd, 4, @InsertDate))

	SET @RowCount = @RowCount + 1
END




1