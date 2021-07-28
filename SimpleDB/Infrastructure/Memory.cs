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

        public void WriteBool(bool value)
        {
            _write.Write(value);
        }

        public void WriteSByte(sbyte value)
        {
            _write.Write(value);
        }

        public void WriteByte(byte value)
        {
            _write.Write(value);
        }

        public void WriteChar(char value)
        {
            _write.Write(value);
        }

        public void WriteShort(short value)
        {
            _write.Write(value);
        }

        public void WriteUShort(ushort value)
        {
            _write.Write(value);
        }

        public void WriteInt(int value)
        {
            _write.Write(value);
        }

        public void WriteUInt(uint value)
        {
            _write.Write(value);
        }

        public void WriteLong(long value)
        {
            _write.Write(value);
        }

        public void WriteULong(ulong value)
        {
            _write.Write(value);
        }

        public void WriteFloat(float value)
        {
            _write.Write(value);
        }

        public void WriteDouble(double value)
        {
            _write.Write(value);
        }

        public void WriteDecimal(decimal value)
        {
            _write.Write(value);
        }

        public void WriteString(string value)
        {
            _write.Write(value);
        }

        public void WriteByteArray(byte[] value, int index, int count)
        {
            _write.Write(value, index, count);
        }
    }
}
