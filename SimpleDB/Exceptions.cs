using System;

namespace SimpleDB;

public class PrimaryKeyException : Exception
{
}

public class DBEngineException : Exception
{
    public DBEngineException(string message) : base(message)
    {
    }
}
