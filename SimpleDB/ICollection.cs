using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB
{
    public interface ICollection<TEntity>
    {
        TEntity GetById(object id);

        void Insert(TEntity entity);
    }
}
