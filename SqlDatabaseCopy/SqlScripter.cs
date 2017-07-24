using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace SqlDatabaseCopy
{
    public class SqlScripter
    {
        private static Regex cleanEncrypted = new Regex(@"COLLATE \w+ ENCRYPTED WITH \(.*?\)", RegexOptions.Multiline | RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private SqlDatabase _owner;
        private SqlScripterOptions _options;
        private Scripter _scripter;

        public SqlScripter(SqlDatabase owner, SqlScripterOptions options)
        {
            _owner = owner;
            _options = options;
        }

        private void CreateScripter()
        {
            _scripter = new Scripter(_owner.Server);

            _scripter.Options.SetTargetDatabaseEngineType((DatabaseEngineType)Enum.Parse(typeof(DatabaseEngineType), _options.TargetDatabaseEngineType));
            _scripter.Options.SetTargetServerVersion(new ServerVersion(_options.TargetServerVersion.Major, _options.TargetServerVersion.Minor));

            _scripter.Options.AllowSystemObjects = false;
            _scripter.Options.WithDependencies = false;
            _scripter.Options.SchemaQualify = true;
            _scripter.Options.SchemaQualifyForeignKeysReferences = true;
            _scripter.Options.DriIncludeSystemNames = true;
            _scripter.Options.IncludeIfNotExists = true;

            _scripter.Options.DriClustered = true;
            _scripter.Options.ClusteredIndexes = true;

            _scripter.Options.IncludeHeaders = _options.IncludeHeaders;
            _scripter.Options.AnsiPadding = _options.AnsiPadding;
            _scripter.Options.NoFileGroup = _options.NoFileGroup;
            _scripter.Options.NoCollation = _options.NoCollation;
            _scripter.Options.NoExecuteAs = _options.NoExecuteAs;
            _scripter.Options.NoFileStream = _options.NoFileStream;
            _scripter.Options.NoFileStreamColumn = _options.NoFileStreamColumn;
            _scripter.Options.NoIdentities = _options.NoIdentities;
            _scripter.Options.NoIndexPartitioningSchemes = _options.NoIndexPartitioningSchemes;
            _scripter.Options.NoTablePartitioningSchemes = _options.NoTablePartitioningSchemes;
            _scripter.Options.NoVardecimal = _options.NoVardecimal;
            _scripter.Options.NoViewColumns = _options.NoViewColumns;
            _scripter.Options.DriDefaults = _options.DriDefaults;
            _scripter.Options.DriChecks = _options.DriChecks;
            _scripter.Options.DriWithNoCheck = _options.DriWithNoCheck;
            _scripter.Options.ExtendedProperties = _options.ExtendedProperties;
            _scripter.Options.Triggers = _options.Triggers;
        }

        public Scripter Scripter
        {
            get
            {
                if (_scripter == null)
                {
                    CreateScripter();
                }
                return _scripter;
            }
        }

        public IEnumerable<string> ScriptSchemas()
        {
            Scripter.Options.ScriptDrops = false;
            return Scripter.EnumScript(_owner.Database.Schemas.Cast<Schema>().Where(s => !s.IsSystemObject).ToArray());
        }

        public IEnumerable<string> ScriptObject(SqlObject obj, bool scriptIndexes = true)
        {
            Scripter.Options.ScriptDrops = false;
            Scripter.Options.Indexes = scriptIndexes;
            Scripter.Options.DriIndexes = scriptIndexes;

            var sql = Scripter.EnumScript(new[] { _owner.GetSmoObject(obj) });

            if (obj.Type == SqlObjectType.Table)
            {
                sql = sql.Select(q => cleanEncrypted.Replace(q, String.Empty));
            }

            return sql;
        }

        public IEnumerable<string> ScriptForeignKeys(SqlObject obj)
        {
            var table = (Table)_owner.GetSmoObject(obj);

            Scripter.Options.ScriptDrops = false;
            return Scripter.EnumScript(table.ForeignKeys.Cast<ForeignKey>().ToArray());
        }

        public IEnumerable<string> ScriptDropForeignKeys(SqlObject obj)
        {
            var table = (Table)_owner.GetSmoObject(obj);

            Scripter.Options.ScriptDrops = true;
            return Scripter.EnumScript(table.ForeignKeys.Cast<ForeignKey>().ToArray());
        }

        public IEnumerable<string> ScriptDropIndexes(SqlObject obj)
        {
            var table = (Table)_owner.GetSmoObject(obj);

            Scripter.Options.ScriptDrops = true;
            return Scripter.EnumScript(table.Indexes.Cast<Index>().Where(i => !i.IsClustered).ToArray());
        }

        public IEnumerable<string> ScriptIndexes(SqlObject obj)
        {
            var table = (Table)_owner.GetSmoObject(obj);

            Scripter.Options.ScriptDrops = false;
            return Scripter.EnumScript(table.Indexes.Cast<Index>().Where(i => !i.IsClustered).ToArray());
        }

        public string ScriptSelect(SqlObject obj)
        {
            var sql = new StringBuilder();

            var query = $"select c.name, c.collation_name from sys.columns c"
                + $" where c.object_id = OBJECT_ID('{obj.FullName}')";

            var columns = _owner.ExecuteList(query, r => r.IsDBNull(1)
                ? $"[{r.GetString(0)}]"
                : $"[{r.GetString(0)}] COLLATE {r.GetString(1)} AS [{r.GetString(0)}]");

            if (!columns.Any())
            {
                throw new InvalidOperationException($"{obj} can't be found");
            }

            sql.Append($"SELECT " + String.Join(", ", columns) + $" FROM {obj.FullName}");

            query = $"select COL_NAME(c.object_id, c.column_id), c.is_descending_key from sys.index_columns c"
                + $" join sys.indexes i on i.index_id = c.index_id and i.object_id = c.object_id"
                + $" where c.object_id = OBJECT_ID('{obj.FullName}') and i.type = 1 and c.is_included_column = 0"
                + $" order by c.key_ordinal";

            columns = _owner.ExecuteList(query, r => $"[{r.GetString(0)}] " + (r.GetBoolean(1) ? "DESC" : "ASC"));

            if (columns.Any())
            {
                sql.Append(" ORDER BY " + String.Join(", ", columns));
            }

            return sql.ToString();
        }
    }
}
