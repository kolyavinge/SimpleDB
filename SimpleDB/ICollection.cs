using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB
{
    public interface ICollection<TEntity> : IDisposable
    {
        int Count();

        bool Exist(object id);

        TEntity Get(object id);

        void Insert(TEntity entity);

        void Update(TEntity entity);

        void InsertOrUpdate(TEntity entity);

        void Delete(object id);

        IQueryable<TEntity> Query();
    }
}
