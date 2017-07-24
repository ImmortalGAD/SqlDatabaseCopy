using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDatabaseCopy
{
    public enum MigrationItemStatus
    {
        NotStarted,
        SchemaMigrated,
        DataMigrated,
        MigrationCompleted
    }

    public static class MigrationItemExtensions
    {
        public static bool Succeed(this IEnumerable<MigrationItem> items)
        {
            return items.All(item => item.Succeed);
        }
    }

    public class MigrationItem
    {
        public SqlObject Object { get; set; }
        public MigrationItemStatus Status { get; set; }
        public int Attempts { get; set; }
        public bool Succeed { get; set; }
        public Exception LastError { get; set; }

        public int ObjectId
        {
            get
            {
                return Object.Id;
            }
        }

        public bool IsTable
        {
            get
            {
                return Object.Type == SqlObjectType.Table;
            }
        }

        public override string ToString()
        {
            return Object.ToString();
        }
    }
}
