namespace SimpleDB.Queries;

internal abstract class AbstractQuery
{
    protected AbstractQuery(string entityName)
    {
        EntityName = entityName;
    }

    public string EntityName { get; }
}
