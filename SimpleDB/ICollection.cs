using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB
{
    public interface ICollection<TEntity>
    {
        TEntity Get(object id);

        void Insert(TEntity entity);

        void Update(TEntity entity);

        void Delete(object id);

        IQueryable<TEntity> Query();
    }
}
