using System;
using System.IO;

namespace SimpleDB.Infrastructure;

internal interface IStream : IDisposable
{
    long Position { get; }

    long Length { get; }

    long Seek(long offset, SeekOrigin origin);
}
