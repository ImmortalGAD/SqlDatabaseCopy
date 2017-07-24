using System;
using System.Configuration;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace SqlDatabaseCopy
{
    public class MigrationOptions
    {
        public static MigrationOptions GetFromAppConfig()
        {
            var options = new MigrationOptions();
            options.ScripterOptions = SqlScripterOptions.GetFromAppConfig();
            options.MaxThreads = Convert.ToInt32(ConfigurationManager.AppSettings["MaxThreads"]);
            options.MaxErrors = Convert.ToInt32(ConfigurationManager.AppSettings["MaxErrors"]);
            options.MaxAttempts = Convert.ToInt32(ConfigurationManager.AppSettings["MaxAttempts"]);
            options.MaxAttempts = Convert.ToInt32(ConfigurationManager.AppSettings["MaxAttempts"]);

            return options;
        }

        public SqlScripterOptions ScripterOptions { get; set; }
        public string SourceConnectionString { get; set; }
        public string TargetConnectionString { get; set; }
        public bool DataOnly { get; set; }
        public bool SchemaOnly { get; set; }
        public int MaxAttempts { get; set; }
        public int MaxErrors { get; set; }
        public int MaxThreads { get; set; }
        public bool LogSql { get; set; }
        public TextWriter Log { get; set; }
    }
}
