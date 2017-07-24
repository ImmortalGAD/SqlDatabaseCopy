using System;
using System.IO;
using System.Diagnostics;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;

namespace SqlDatabaseCopy
{
    public class MigrationHandler
    {
        private static object syncRoot = new object();

        private MigrationOptions options;
        private MigrationItem[] items;

        private HashSet<int> itemsInProgress = null;
        private Dictionary<int, MigrationItem[]> dependenciesChild = null;
        private Dictionary<int, int[]> dependenciesParent = null;

        public MigrationItem[] Items
        {
            get
            {
                return items;
            }
        }

        public MigrationHandler(MigrationOptions options)
        {
            this.options = options;
        }

        public void Process()
        {
            Initialize();
            CollectDependencies();

            if (!options.DataOnly)
            {
                // schema only or full migration - generate schemas first
                GenerateSchemas();
            }
            else
            {
                // data only - drop FKs to be able drop non-clustered indexes before copying data
                DropRelationships();
            }

            ProcessItems();

            if (items.Where(t => t.IsTable).Succeed())
            {
                // in case all tables ok - restore \ generate FKs
                GenerateRelationships();
            }
        }

        private MigrationItem[] Initialize()
        {
            Console.WriteLine("Initialize...");

            using (var source = SqlDatabase.Connect(options.SourceConnectionString, options.ScripterOptions))
            using (var target = SqlDatabase.Connect(options.TargetConnectionString, options.ScripterOptions))
            {
                // in case when schema is generated ensure target database is empty
                if (!options.DataOnly)
                {
                    if (target.GetSqlObjects(tablesOnly: false).Any())
                        throw new InvalidOperationException("Target Database already has Schema migrated");
                }

                items = options.DataOnly
                    // fill with only tables have to be populated
                    ? target.GetSqlObjects(tablesOnly: true).CreateMigrationItems()
                    // or extract all objects from the source otherwise
                    : source.GetSqlObjects(tablesOnly: false).CreateMigrationItems();

                // in case when data is only copied - ensure tables are empty
                if (options.DataOnly)
                {
                    ValidateTargetDatabaseEmpty();
                }

                return items;
            }
        }

        private void ValidateTargetDatabaseEmpty()
        {
            Console.WriteLine("Validate Target Database...");

            Worker.Process(options, items.Where(item => item.IsTable), (source, target, item, log, queue) =>
            {
                var table = item.Object;
                if (!target.IsTableEmpty(table))
                    throw new InvalidOperationException($"{table} has data");

            }, noRetry: true);

            if (items.Any(item => item.IsTable && !item.Succeed))
                throw new InvalidOperationException("Target Database already has Data migrated");
        }

        private void CollectDependencies()
        {
            Console.WriteLine("Collect Dependencies...");

            // no schema dependencies
            if (options.DataOnly)
            {
                dependenciesChild = new Dictionary<int, MigrationItem[]>();
                dependenciesParent = new Dictionary<int, int[]>();
                return;
            }

            using (var source = SqlDatabase.Connect(options.SourceConnectionString, options.ScripterOptions))
            {
                var dependencies = source.GetSqlDependencies();
                var itemsMap = items.ToDictionary(i => i.Object.Id);

                // collect direct dependencies (id -> referenced id) 
                dependenciesParent = dependencies
                    .GroupBy(d => d.Id, d => d.DependsOn)
                    .ToDictionary(g => g.Key, g => g.ToArray());

                // collect inverse dependencies (id -> all items depended on id)
                dependenciesChild = dependencies
                    .GroupBy(d => d.DependsOn, d => itemsMap[d.Id])
                    .ToDictionary(g => g.Key, g => g.ToArray());

                // mark all items as pending
                itemsInProgress = new HashSet<int>(items.Select(i => i.Object.Id));
            }
        }

        private bool HasNoDependencies(MigrationItem item)
        {
            int[] parents;
            return !dependenciesParent.TryGetValue(item.ObjectId, out parents) || parents.All(id => !itemsInProgress.Contains(id));
        }

        private IEnumerable<MigrationItem> GetChildDependencies(MigrationItem item)
        {
            MigrationItem[] childs;
            return dependenciesChild.TryGetValue(item.ObjectId, out childs) ? childs : Enumerable.Empty<MigrationItem>();
        }

        private void GenerateSchemas()
        {
            Console.WriteLine("Generte Schemas...");

            using (var source = SqlDatabase.Connect(options.SourceConnectionString, options.ScripterOptions))
            using (var target = SqlDatabase.Connect(options.TargetConnectionString, options.ScripterOptions))
            {
                options.Log.WriteLine("Generate schemas.");
                Execute(target, source.Scripter.ScriptSchemas(), options.Log);
            }
        }

        private void DropRelationships()
        {
            Console.WriteLine("Drop Relationships...");

            Worker.Process(options, items.Where(t => t.IsTable), (source, target, item, log, queue) =>
            {
                log.WriteLine($"Drop foreign keys on {item}.");
                Execute(target, source.Scripter.ScriptDropForeignKeys(item.Object), log);
            });
        }

        private void ProcessItems()
        {
            Console.WriteLine("Process Items...");

            Worker.Process(options, items.Where(HasNoDependencies), items.Count(), (source, target, item, log, queue) =>
            {
                if (item.Status == MigrationItemStatus.NotStarted)
                {
                    bool doNotCopyData = options.SchemaOnly || !item.IsTable || source.IsTableEmpty(item.Object);

                    if (!options.DataOnly)
                    {
                        log.WriteLine($"Generate schema for {item}.");
                        // generate scheam                    
                        Execute(target, source.Scripter.ScriptObject(item.Object, scriptIndexes: doNotCopyData), log);

                        item.Status = doNotCopyData
                            ? MigrationItemStatus.MigrationCompleted
                            : MigrationItemStatus.SchemaMigrated;

                        EnqueueDependencies(item, queue, log);
                    }
                    else if (doNotCopyData)
                    {
                        log.WriteLine($"{item} is empty.");
                        item.Status = MigrationItemStatus.MigrationCompleted;
                    }
                    else
                    {
                        log.WriteLine($"Drop indexes on {item}.");

                        Execute(target, source.Scripter.ScriptDropIndexes(item.Object), log);
                        item.Status = MigrationItemStatus.SchemaMigrated;
                    }
                }

                if (item.Status == MigrationItemStatus.SchemaMigrated)
                {
                    log.WriteLine($"Copy data into {item}.");
                    // copy data
                    ExecuteBulkCopy(source, target, item.Object, log);
                    item.Status = MigrationItemStatus.DataMigrated;
                }

                if (item.Status == MigrationItemStatus.DataMigrated)
                {
                    log.WriteLine($"Generate indexes on {item}.");
                    // generate indexes
                    Execute(target, source.Scripter.ScriptIndexes(item.Object), log);
                    item.Status = MigrationItemStatus.MigrationCompleted;
                }
            });
        }

        private void EnqueueDependencies(MigrationItem completedItem, ConcurrentQueue<MigrationItem> queue, TextWriter log)
        {
            lock (syncRoot)
            {
                // mark item as processed
                itemsInProgress.Remove(completedItem.ObjectId);
                // and check all child items dependencies
                foreach (var childItem in GetChildDependencies(completedItem).Where(HasNoDependencies))
                {
                    log.WriteLine($"Add {childItem} into a queue.");
                    queue.Enqueue(childItem);
                }
            }
        }

        private void GenerateRelationships()
        {
            Console.WriteLine("Generate Relationships...");

            Worker.Process(options, items.Where(t => t.IsTable), (source, target, item, log, queue) =>
            {
                log.WriteLine($"Generate foreign kyes on {item}.");
                Execute(target, source.Scripter.ScriptForeignKeys(item.Object), log);
            });
        }

        #region Execute Sql

        private void Execute(SqlDatabase database, IEnumerable<string> script, TextWriter log)
        {
            foreach (var sql in script)
            {
                var timer = Stopwatch.StartNew();

                if (options.LogSql)
                {
                    log.WriteLine(sql.Trim());
                }

                database.ExecuteNoQuery(sql);
                timer.Stop();

                if (options.LogSql)
                {
                    log.WriteLine($"Elapsed: {timer.Elapsed}");
                    log.WriteLine();
                }
            }
        }

        private void ExecuteBulkCopy(SqlDatabase source, SqlDatabase target, SqlObject table, TextWriter log)
        {
            var timer = Stopwatch.StartNew();
            var sql = target.Scripter.ScriptSelect(table);

            if (options.LogSql)
            {
                log.WriteLine("Do Bulk Copy for the following query:");
                log.WriteLine(sql.Trim());
            }

            using (var reader = source.ExecuteReader(sql))
            {
                target.BulkCopy(reader, table);
            }

            timer.Stop();

            if (options.LogSql)
            {
                log.WriteLine($"Elapsed: {timer.Elapsed}");
                log.WriteLine();
            }
        }

        #endregion
    }
}
