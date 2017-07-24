using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlDatabaseCopy
{
    public class SqlDependency
    {
        public int Id { get; set; }
        public int DependsOn { get; set; }
    }
}
