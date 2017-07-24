using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace SqlDatabaseCopy
{
    public static class SqlHelper
    {
        public const int CommandTimeout = 0;

        public static SqlCommand CreateCommand(SqlConnection connection, string sql, params SqlParameter[] parameters)
        {
            var cmd = new SqlCommand(sql, connection);
            cmd.CommandTimeout = CommandTimeout;

            if (parameters != null)
            {
                cmd.Parameters.AddRange(parameters);
            }

            return cmd;
        }

        public static void ExecuteNoQuery(SqlConnection connection, string sql, params SqlParameter[] parameters)
        {
            using (var cmd = CreateCommand(connection, sql, parameters))
            {
                cmd.ExecuteNonQuery();
            }
        }

        public static object ExecuteScalar(SqlConnection connection, string sql, params SqlParameter[] parameters)
        {
            using (var cmd = CreateCommand(connection, sql, parameters))
            {
                return cmd.ExecuteScalar();
            }
        }

        public static SqlDataReader ExecuteReader(SqlConnection connection, string sql, params SqlParameter[] parameters)
        {
            using (var cmd = CreateCommand(connection, sql, parameters))
            {
                return cmd.ExecuteReader();
            }
        }

        public static T[] ExecuteList<T>(SqlConnection connection, string sql, Func<IDataReader, T> map, params SqlParameter[] parameters)
        {
            var list = new List<T>();

            using (var reader = ExecuteReader(connection, sql, parameters))
            {
                while (reader.Read())
                {
                    list.Add(map(reader));
                }
            }

            return list.ToArray();
        }
    }
}
