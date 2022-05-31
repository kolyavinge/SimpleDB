namespace SimpleDB.Queries;

internal class DeleteQuery : AbstractQuery
{
    public DeleteQuery(string entityName) : base(entityName) { }

    public WhereClause? WhereClause { get; set; }
}
