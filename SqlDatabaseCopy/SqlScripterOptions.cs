using System;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;

namespace SqlDatabaseCopy
{
    public class SqlScripterOptions
    {
        public static SqlScripterOptions GetFromAppConfig()
        {
            var options = new SqlScripterOptions();

            options.TargetServerVersion = Version.Parse(ConfigurationManager.AppSettings["TargetServerVersion"]);
            options.TargetDatabaseEngineType = ConfigurationManager.AppSettings["TargetDatabaseEngineType"];

            options.IncludeHeaders = Convert.ToBoolean(ConfigurationManager.AppSettings["IncludeHeaders"]);
            options.AnsiPadding = Convert.ToBoolean(ConfigurationManager.AppSettings["AnsiPadding"]);
            options.NoFileGroup = Convert.ToBoolean(ConfigurationManager.AppSettings["NoFileGroup"]);
            options.NoCollation = Convert.ToBoolean(ConfigurationManager.AppSettings["NoCollation"]);
            options.NoExecuteAs = Convert.ToBoolean(ConfigurationManager.AppSettings["NoExecuteAs"]);
            options.NoFileStream = Convert.ToBoolean(ConfigurationManager.AppSettings["NoFileStream"]);
            options.NoFileStreamColumn = Convert.ToBoolean(ConfigurationManager.AppSettings["NoFileStreamColumn"]);
            options.NoIdentities = Convert.ToBoolean(ConfigurationManager.AppSettings["NoIdentities"]);
            options.NoIndexPartitioningSchemes = Convert.ToBoolean(ConfigurationManager.AppSettings["NoIndexPartitioningSchemes"]);
            options.NoTablePartitioningSchemes = Convert.ToBoolean(ConfigurationManager.AppSettings["NoTablePartitioningSchemes"]);
            options.NoVardecimal = Convert.ToBoolean(ConfigurationManager.AppSettings["NoVardecimal"]);
            options.NoViewColumns = Convert.ToBoolean(ConfigurationManager.AppSettings["NoViewColumns"]);
            options.DriDefaults = Convert.ToBoolean(ConfigurationManager.AppSettings["DriDefaults"]);
            options.DriChecks = Convert.ToBoolean(ConfigurationManager.AppSettings["DriChecks"]);
            options.DriWithNoCheck = Convert.ToBoolean(ConfigurationManager.AppSettings["DriWithNoCheck"]);
            options.ExtendedProperties = Convert.ToBoolean(ConfigurationManager.AppSettings["ExtendedProperties"]);
            options.Triggers = Convert.ToBoolean(ConfigurationManager.AppSettings["Triggers"]);
            return options;
        }

        public Version TargetServerVersion { get; set; }
        public string TargetDatabaseEngineType { get; set; }
        public bool IncludeHeaders { get; set; }
        public bool AnsiPadding { get; set; }
        public bool NoFileGroup { get; set; }
        public bool NoCollation { get; set; }
        public bool NoExecuteAs { get; set; }
        public bool NoFileStream { get; set; }
        public bool NoFileStreamColumn { get; set; }
        public bool NoIdentities { get; set; }
        public bool NoIndexPartitioningSchemes { get; set; }
        public bool NoTablePartitioningSchemes { get; set; }
        public bool NoVardecimal { get; set; }
        public bool NoViewColumns { get; set; }
        public bool DriDefaults { get; set; }
        public bool DriChecks { get; set; }
        public bool DriWithNoCheck { get; set; }
        public bool ExtendedProperties { get; set; }
        public bool Triggers { get; set; }
    }
}
