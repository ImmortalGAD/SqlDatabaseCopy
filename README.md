# SqlDatabaseCopy
Simple console tool uses SMO scripting capabilities and SqlBulkCopy for schema and data migration between different instances.

Available options:
- SQL Server database -> Azure SQL database
- SQL Server database -> SQL Server database

## Requirements
.Net Framework v4.6.2 and SQL Server Management Objects (SMO) 2017 have to be installed on the client machine(also can edit .config file to redirect to a different SMO version if needed).

## Usage
```
SqlDatabaseCopy.exe "<source-connection-string>" "<target-connection-string>" [-DataOnly | -SchemaOnly] [-NoLog]
```
**Note**: empty database has to be created in the target before utility run.
