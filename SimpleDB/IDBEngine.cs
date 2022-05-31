namespace SimpleDB;

public interface IDBEngine
{
    ICollection<TEntity> GetCollection<TEntity>();
}
