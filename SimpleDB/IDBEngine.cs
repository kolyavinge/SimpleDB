using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB
{
    public interface IDBEngine : IDisposable
    {
        ICollection<TEntity> GetCollection<TEntity>();
    }
}
