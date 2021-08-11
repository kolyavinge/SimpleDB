using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal class UpdateQuery
    {
        public UpdateQuery(UpdateClause updateClause)
        {
            UpdateClause = updateClause;
        }

        public UpdateClause UpdateClause { get; }

        public WhereClause WhereClause { get; set; }
    }
}
