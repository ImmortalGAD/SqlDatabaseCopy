using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDatabaseCopy
{
    public enum SqlObjectType
    {
        Table,
        View,
        StoredProcedure,
        Function
    }

    public static class SqlObjectExtensions
    {
        public static MigrationItem[] CreateMigrationItems(this IEnumerable<SqlObject> sqlItems)
        {
            if (sqlItems == null) return null;

            return sqlItems
                .Select(item => new MigrationItem { Object = item, Attempts = 0, Succeed = false, Status = MigrationItemStatus.NotStarted })
                .ToArray();
        }
    }

    public class SqlObject
    {
        public static SqlObjectType GetSqlObjectType(string type)
        {
            switch (type)
            {
                case "U":
                    return SqlObjectType.Table;
                case "V":
                    return SqlObjectType.View;
                case "P":
                    return SqlObjectType.StoredProcedure;
                case "FN":
                case "TF":
                    return SqlObjectType.Function;
                default:
                    throw new InvalidOperationException($"Unknown Sql Object type: {type}");
            }
        }

        public static string GetTypeName(SqlObjectType type)
        {
            switch (type)
            {
                case SqlObjectType.Function: return "function";
                case SqlObjectType.StoredProcedure: return "procedure";
                case SqlObjectType.Table: return "table";
                case SqlObjectType.View: return "view";
                default:
                    throw new InvalidOperationException($"Unknown Sql Object type: {type}");
            }
        }

        public int Id { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }
        public SqlObjectType Type { get; set; }

        public string FullName
        {
            get
            {
                return $"[{Schema}].[{Name}]";
            }
        }

        public override string ToString()
        {
            return $"{GetTypeName(Type)} {FullName}";
        }
    }
}
