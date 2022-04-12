using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal class UpdateQuery : AbstractQuery
    {
        public UpdateQuery(string entityName, UpdateClause updateClause) : base(entityName)
        {
            UpdateClause = updateClause;
        }

        public UpdateClause UpdateClause { get; }

        public WhereClause? WhereClause { get; set; }
    }
}
