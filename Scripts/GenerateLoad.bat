::------------------------------------------------------------------------------+    
:: = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :  |    
::{>/------------------------------------------------------------------------\<}| 
::: | Batch Name:	GenerateLoad.sql					| 
::: | Author	 :	Prakash Heda						| 
::: | Email	 :	prakash@sqlfeatures.com	 Blog:http://www.sqlfeatures.com|
::: | Description:	Sample load generator to create test scenario’s		|
::: | 										|
::: | SQL Version:	SQL 2012, SQL 2008 R2, SQL 2008				|
::: | Copyright	 :	Free to use and share 	/^(o.o)^\			|
::: | 										|
::: | Create Date:	01-15-2012  Version: 1.0        			|
::: | Revision	 :	02-21-2012  Version: 1.1  updated code for queryhash	|
:::	| History								|
::{>\------------------------------------------------------------------------/<}|  
:: = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = : = :	|  
::-----------------------------------------------------------------------------*/



sqlcmd -d testdb -Q"insert into testdb..TestTableSet4 select top 2900000 * from testdb..TestTableSet3"
sqlcmd -d testdb -Q"exec procGenerateLoad" >tmp1.txt

sqlcmd -d testdb -Q"insert into testdb..TestTableSet4 select top 3000000 * from testdb..TestTableSet3"
sqlcmd -d testdb -Q"exec procGenerateLoad">tmp1.txt


sqlcmd -d testdb -Q"insert into testdb..TestTableSet4 select top 3100000 * from testdb..TestTableSet3"
sqlcmd -d testdb -Q"exec procGenerateLoad">tmp1.txt



sqlcmd -d testdb -Q"insert into testdb..TestTableSet4 select top 3100000 * from testdb..TestTableSet3"
sqlcmd -d testdb -Q"delete from testdb..TestTableSet4"

sqlcmd -d testdb -Q"insert into testdb..TestTableSet4 select top 3100000 * from testdb..TestTableSet3"
sqlcmd -d testdb -Q"delete from testdb..TestTableSet4"

sqlcmd -d testdb -Q"insert into testdb..TestTableSet4 select top 3100000 * from testdb..TestTableSet3"
sqlcmd -d testdb -Q"delete from testdb..TestTableSet4 -- delete test data"
