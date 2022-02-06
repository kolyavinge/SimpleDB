using System.Collections.Generic;

namespace SimpleDB
{
    public interface ICollection<TEntity>
    {
        int Count();

        bool Exist(object id);

        TEntity Get(object id);

        IEnumerable<TEntity> Get(IEnumerable<object> idList);

        IEnumerable<TEntity> GetAll();

        void Insert(TEntity entity);

        void Insert(IEnumerable<TEntity> entities);

        void Update(TEntity entity);

        void Update(IEnumerable<TEntity> entities);

        void InsertOrUpdate(TEntity entity);

        void InsertOrUpdate(IEnumerable<TEntity> entities);

        void Delete(object id);

        void Delete(IEnumerable<object> idList);

        IQueryable<TEntity> Query();
    }
}
