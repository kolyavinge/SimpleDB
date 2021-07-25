using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SimpleDB.Infrastructure
{
    internal interface IMemory
    {
        IMemoryBuffer GetBuffer();
    }

    internal class Memory : IMemory
    {
        public IMemoryBuffer GetBuffer()
        {
            return new MemoryBuffer();
        }
    }

    internal interface IMemoryBuffer : IWriteableStream
    {
        byte[] BufferArray { get; }
    }

    internal class MemoryBuffer : IMemoryBuffer
    {
        private readonly MemoryStream _memoryStream;
        private readonly BinaryWriter _write;

        public byte[] BufferArray { get; private set; }

        public bool EOF { get { return _memoryStream.Position >= _memoryStream.Length; } }

        public long Position { get { return _memoryStream.Position; } }

        public MemoryBuffer()
        {
            BufferArray = new byte[10 * 1024 * 1024];
            _memoryStream = new MemoryStream(BufferArray);
            _write = new BinaryWriter(_memoryStream);
        }

        public void Dispose()
        {
            _memoryStream.Dispose();
        }

        public void Seek(long offset, SeekOrigin origin)
        {
            _memoryStream.Seek(offset, origin);
        }

        public void Write(bool value)
        {
            _write.Write(value);
        }

        public void Write(sbyte value)
        {
            _write.Write(value);
        }

        public void Write(byte value)
        {
            _write.Write(value);
        }

        public void Write(char value)
        {
            _write.Write(value);
        }

        public void Write(short value)
        {
            _write.Write(value);
        }

        public void Write(ushort value)
        {
            _write.Write(value);
        }

        public void Write(int value)
        {
            _write.Write(value);
        }

        public void Write(uint value)
        {
            _write.Write(value);
        }

        public void Write(long value)
        {
            _write.Write(value);
        }

        public void Write(ulong value)
        {
            _write.Write(value);
        }

        public void Write(float value)
        {
            _write.Write(value);
        }

        public void Write(double value)
        {
            _write.Write(value);
        }

        public void Write(decimal value)
        {
            _write.Write(value);
        }

        public void Write(string value)
        {
            _write.Write(value);
        }

        public void Write(byte[] value, int index, int count)
        {
            _write.Write(value, index, count);
        }
    }
}
