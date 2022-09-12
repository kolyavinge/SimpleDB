using System.Collections.Generic;

namespace SimpleDB;

public interface ICollection<TEntity>
{
    int Count();

    bool Exist(object id);

    TEntity? Get(object id);

    IEnumerable<TEntity> GetRange(IReadOnlyCollection<object> idList);

    IEnumerable<TEntity> GetAll();

    void Insert(TEntity entity);

    void InsertRange(IReadOnlyCollection<TEntity> entities);

    void Update(TEntity entity);

    void UpdateRange(IReadOnlyCollection<TEntity> entities);

    void InsertOrUpdate(TEntity entity);

    void InsertOrUpdateRange(IReadOnlyCollection<TEntity> entities);

    void Delete(object id);

    void DeleteRange(IReadOnlyCollection<object> idList);

    IQueryable<TEntity> Query();
}
