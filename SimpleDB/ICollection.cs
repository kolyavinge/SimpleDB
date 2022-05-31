using System.Collections.Generic;

namespace SimpleDB;

public interface ICollection<TEntity>
{
    int Count();

    bool Exist(object id);

    TEntity? GetOrDefault(object id);

    IEnumerable<TEntity> GetOrDefault(IReadOnlyCollection<object> idList);

    IEnumerable<TEntity> GetAll();

    void Insert(TEntity entity);

    void Insert(IReadOnlyCollection<TEntity> entities);

    void Update(TEntity entity);

    void Update(IReadOnlyCollection<TEntity> entities);

    void InsertOrUpdate(TEntity entity);

    void InsertOrUpdate(IReadOnlyCollection<TEntity> entities);

    void Delete(object id);

    void Delete(IReadOnlyCollection<object> idList);

    IQueryable<TEntity> Query();
}
