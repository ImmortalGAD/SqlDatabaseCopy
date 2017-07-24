using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace SqlDatabaseCopy
{
    class Program
    {
        static int Main(string[] args)
        {
            MigrationOptions options = null;
            try
            {
                if (args == null || args.Length < 2)
                {
                    Console.WriteLine("Usage:");
                    Console.WriteLine("SqlDatabaseCopy.exe <sourceConnectionString> <targetConnectionString> [-SchemaOnly | -DataOnly] [-NoLog]");
                    return -1;
                }

                options = GetOptions(args);
                var handler = new MigrationHandler(options);
                handler.Process();
                return HandleErrors(handler);
            }
            catch (Exception ex)
            {
                ConsoleHelper.WriteError(ex);
                options?.Log?.WriteLine(ex.ToString());
                return -1;
            }
            finally
            {
                options?.Log?.Dispose();
            }
        }

        static MigrationOptions GetOptions(string[] args)
        {
            bool noLog = false;
            var options = MigrationOptions.GetFromAppConfig();

            options.SourceConnectionString = args[0];
            options.TargetConnectionString = args[1];

            for (int i = 2; i < args.Length; i++)
            {
                switch (args[i].ToLower().Trim())
                {
                    case "-schemaonly":
                        options.SchemaOnly = true;
                        break;
                    case "-dataonly":
                        options.DataOnly = true;
                        break;
                    case "-nolog":
                        noLog = true;
                        break;
                    default:
                        throw new InvalidOperationException($"Unknown parameter: {args[i]}");
                }
            }

            if (options.SchemaOnly && options.DataOnly)
                throw new InvalidOperationException("It's not allowed to use SchemaOnly and DataOnly options same time");

            if (noLog)
            {
                options.Log = TextWriter.Null;
            }
            else
            {
                Directory.CreateDirectory("log");
                options.Log = TextWriter.Synchronized(new StreamWriter($"log\\SqlDatabaseCopy_{DateTime.Now.ToString("yyyyMMdd_HHmmsstt")}.log"));
            }

            return options;
        }

        static int HandleErrors(MigrationHandler handler)
        {
            int errorCount = 0;

            foreach (var item in handler.Items.Where(item => !item.Succeed))
            {
                errorCount++;
                if (item.LastError != null)
                {
                    ConsoleHelper.WriteError($"{item}: " + item.LastError.Message);
                }
            }

            return errorCount;
        }
    }
}
