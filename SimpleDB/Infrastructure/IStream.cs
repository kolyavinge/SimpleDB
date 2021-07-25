using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SimpleDB.Infrastructure
{
    internal interface IStream : IDisposable
    {
        bool EOF { get; }

        long Position { get; }

        void Seek(long offset, SeekOrigin origin);
    }
}
