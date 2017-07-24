using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;

using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace SqlDatabaseCopy
{
    public class SqlDatabase : IDisposable
    {
        private SqlConnection _connection;
        private Server _server;
        private Database _database;
        private SqlScripterOptions _options;
        private SqlScripter _scripter;

        public static SqlDatabase Connect(string connectionString, SqlScripterOptions options)
        {
            var db = new SqlDatabase(connectionString, options);
            return db;
        }

        private static SqlConnection CreateConnection(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            builder.PersistSecurityInfo = true;
            builder.ApplicationName = "SqlDatabaseCopy";
            return new SqlConnection(builder.ToString());
        }

        public SqlDatabase(string connectionString, SqlScripterOptions options)
        {
            _options = options;
            _connection = CreateConnection(connectionString);
        }

        public SqlConnection Connection
        {
            get
            {
                if (_connection.State != ConnectionState.Open)
                {
                    _connection.Open();
                }
                return _connection;
            }
        }

        public Server Server
        {
            get
            {
                if (_server == null)
                {
                    var serverConnection = new ServerConnection(Connection);
                    _server = new Server(serverConnection);
                }
                return _server;
            }
        }

        public Database Database
        {
            get
            {
                if (_database == null)
                {
                    _database = Server.Databases[Connection.Database];
                }
                return _database;
            }
        }

        public SqlScripter Scripter
        {
            get
            {
                if (_scripter == null)
                {
                    _scripter = new SqlScripter(this, _options);
                }
                return _scripter;
            }
        }

        public SqlSmoObject GetSmoObject(SqlObject obj)
        {
            SqlSmoObject smoObj = null;

            switch (obj.Type)
            {
                case SqlObjectType.Table:
                    smoObj = Database.Tables[obj.Name, obj.Schema];
                    break;
                case SqlObjectType.View:
                    smoObj = Database.Views[obj.Name, obj.Schema];
                    break;
                case SqlObjectType.Function:
                    smoObj = Database.UserDefinedFunctions[obj.Name, obj.Schema];
                    break;
                case SqlObjectType.StoredProcedure:
                    smoObj = Database.StoredProcedures[obj.Name, obj.Schema];
                    break;
            }
            if (smoObj == null)
            {
                throw new InvalidOperationException($"{obj} can't be found");
            }
            return smoObj;
        }

        public SqlObject[] GetSqlObjects(bool tablesOnly)
        {
            var sql = @"select object_id, SCHEMA_NAME(schema_id), name, type from sys.objects where is_ms_shipped = 0" + (tablesOnly
                ? " and type = 'U'"
                : " and type in ('U', 'V', 'FN', 'TF', 'P')");

            return SqlHelper.ExecuteList(Connection, sql, r => new SqlObject
            {
                Id = r.GetInt32(0),
                Schema = r.GetString(1),
                Name = r.GetString(2),
                Type = SqlObject.GetSqlObjectType(r.GetString(3).TrimEnd())
            });
        }

        public SqlDependency[] GetSqlDependencies()
        {
            var sql = @"select distinct d.referencing_id , d.referenced_id from sys.sql_expression_dependencies d
                    join sys.objects s on s.object_id = d.referencing_id
                    join sys.objects t on t.object_id = d.referenced_id
                    -- exclude circular dependency just in case
                    where not exists (select 1 from sys.sql_expression_dependencies d2 where d2.referencing_id = d.referenced_id and d2.referenced_id = d.referencing_id)";

            return SqlHelper.ExecuteList(Connection, sql, r => new SqlDependency
            {
                Id = r.GetInt32(0),
                DependsOn = r.GetInt32(1)
            });
        }

        public bool IsTableEmpty(SqlObject o)
        {
            return SqlHelper.ExecuteScalar(Connection, $"select top 1 1 from {o.FullName}") == null;
        }

        public void ExecuteNoQuery(string sql, params SqlParameter[] parameters)
        {
            SqlHelper.ExecuteNoQuery(Connection, sql, parameters);
        }

        public SqlDataReader ExecuteReader(string sql, params SqlParameter[] parameters)
        {
            return SqlHelper.ExecuteReader(Connection, sql, parameters);
        }

        public T[] ExecuteList<T>(string sql, Func<IDataReader, T> map, params SqlParameter[] parameters)
        {
            return SqlHelper.ExecuteList<T>(Connection, sql, map, parameters);
        }

        public void BulkCopy(IDataReader dataSource, SqlObject table)
        {
            using (var bcp = new SqlBulkCopy(Connection,
                SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock, null))
            {
                bcp.DestinationTableName = table.FullName;
                bcp.BatchSize = 0;
                bcp.BulkCopyTimeout = 0;
                bcp.EnableStreaming = true;

                bcp.WriteToServer(dataSource);
            }
        }

        public void Dispose()
        {
            _server?.ConnectionContext.Disconnect();
            _connection?.Dispose();
        }
    }
}
