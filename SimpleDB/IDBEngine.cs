using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB
{
    public interface IDBEngine
    {
        ICollection<TEntity> GetCollection<TEntity>();
    }
}
