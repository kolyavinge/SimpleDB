using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Linq
{
    internal class UnsupportedQueryException : Exception
    {
        public UnsupportedQueryException()
        {
        }

        public UnsupportedQueryException(string message) : base(message)
        {
        }
    }
}
