using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB
{
    public class PrimaryKeyException : Exception
    {
    }

    public class DBEngineException : Exception
    {
        public DBEngineException(string message) : base(message)
        {
        }
    }
}
