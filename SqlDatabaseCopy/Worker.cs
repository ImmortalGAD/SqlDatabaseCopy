using System;
using System.IO;
using System.Diagnostics;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;

namespace SqlDatabaseCopy
{
    public class Worker
    {
        public delegate void WorkerAction(SqlDatabase source, SqlDatabase target, MigrationItem item, TextWriter log, ConcurrentQueue<MigrationItem> queue);

        public static void Process(MigrationOptions options, IEnumerable<MigrationItem> items, WorkerAction action, bool noRetry = false)
        {
            Process(options, items, items.Count(), action, noRetry);
        }

        public static void Process(MigrationOptions options, IEnumerable<MigrationItem> items, int totalCount, WorkerAction action, bool noRetry = false)
        {
            var worker = new Worker(options, action);
            worker.noRetry = noRetry;
            worker.Process(items, totalCount);
        }

        private MigrationOptions options;
        private WorkerAction action;
        private Thread[] threads;

        private ConcurrentQueue<MigrationItem> queue;

        private int totalCount;
        private int errorCount;
        private int completeCount;

        private bool noRetry = false;
        private Stopwatch timer;

        private Worker(MigrationOptions options, WorkerAction action)
        {
            this.threads = Enumerable.Range(0, options.MaxThreads).Select(_ => new Thread(DoWork) { IsBackground = true }).ToArray();
            this.action = action;
            this.options = options;
        }

        private void Process(IEnumerable<MigrationItem> items, int totalCount)
        {
            this.totalCount = totalCount;

            completeCount = 0;
            errorCount = 0;
            queue = new ConcurrentQueue<MigrationItem>(items);

            InitProgress();

            timer = Stopwatch.StartNew();

            foreach (var thread in threads)
            {
                thread.Start();
            }

            while (threads.Any(t => t.IsAlive))
            {
                UpdateProgress();
                Thread.Sleep(1000);
            }

            timer.Stop();
            UpdateProgress();
        }

        private void InitProgress()
        {
            Console.WriteLine($"{completeCount} of {totalCount} items are processed. Errors: {errorCount}");
        }

        private void UpdateProgress()
        {
            ConsoleHelper.WriteLineBefore($"{completeCount} of {totalCount} items are processed. Errors: {errorCount}. Elapsed: {timer.Elapsed}");
        }

        private void DoWork()
        {
            MigrationItem item;
            while (queue.TryDequeue(out item) && (errorCount < options.MaxErrors))
            {
                SqlDatabase source = null, target = null;
                var log = new StringWriter();

                try
                {
                    source = SqlDatabase.Connect(options.SourceConnectionString, options.ScripterOptions);
                    target = SqlDatabase.Connect(options.TargetConnectionString, options.ScripterOptions);

                    action(source, target, item, log, queue);

                    Interlocked.Increment(ref completeCount);
                    item.Succeed = true;
                    item.LastError = null;
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref errorCount);
                    item.Succeed = false;
                    item.LastError = ex;

                    log.WriteLine($"{ex}");

                    if (!noRetry && (++item.Attempts < options.MaxAttempts))
                    {
                        queue.Enqueue(item);
                    }
                }
                finally
                {
                    string logData = log.ToString();

                    if (!String.IsNullOrEmpty(logData))
                    {
                        options.Log.WriteLine(logData);
                    }

                    source?.Dispose();
                    target?.Dispose();
                }
            }
        }
    }
}
