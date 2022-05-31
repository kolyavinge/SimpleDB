using System;

namespace SimpleDB.Linq;

internal class UnsupportedQueryException : Exception
{
    public UnsupportedQueryException()
    {
    }

    public UnsupportedQueryException(string message) : base(message)
    {
    }
}
