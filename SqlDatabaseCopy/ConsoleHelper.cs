using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDatabaseCopy
{
    public static class ConsoleHelper
    {
        private static object syncRoot = new Object();

        public static void WriteError(Exception ex)
        {
            WriteError(ex.Message);
        }

        public static void WriteError(string msg)
        {
            lock (syncRoot)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(msg);
                Console.ResetColor();
            }
        }

        public static void WriteLineBefore(string msg)
        {
            lock (syncRoot)
            {
                Console.SetCursorPosition(0, Console.CursorTop - 1);
                Console.WriteLine(msg);
            }
        }
    }
}
