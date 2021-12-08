using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal class DeleteQuery : AbstractQuery
    {
        public DeleteQuery(Type entityType) : base(entityType)
        {
        }

        public WhereClause WhereClause { get; set; }
    }
}
