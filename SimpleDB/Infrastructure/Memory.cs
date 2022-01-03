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
        public static readonly Memory Instance = new Memory();

        private Memory() { }

        public IMemoryBuffer GetBuffer()
        {
            return new MemoryBuffer();
        }
    }

    internal interface IMemoryBuffer : IReadableStream, IWriteableStream
    {
        byte[] BufferArray { get; }
    }

    internal class MemoryBuffer : IMemoryBuffer
    {
        private readonly MemoryStream _memoryStream;
        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;

        public byte[] BufferArray { get; private set; }

        public long Position => _memoryStream.Position;

        public long Length => _memoryStream.Length;

        public MemoryBuffer()
        {
            BufferArray = new byte[10 * 1024 * 1024];
            _memoryStream = new MemoryStream(BufferArray, true);
            _reader = new BinaryReader(_memoryStream);
            _writer = new BinaryWriter(_memoryStream);
        }

        public void Dispose()
        {
            _memoryStream.Dispose();
        }

        public bool ReadBool()
        {
            return _reader.ReadBoolean();
        }

        public byte ReadByte()
        {
            return _reader.ReadByte();
        }

        public char ReadChar()
        {
            return _reader.ReadChar();
        }

        public decimal ReadDecimal()
        {
            return _reader.ReadDecimal();
        }

        public double ReadDouble()
        {
            return _reader.ReadDouble();
        }

        public float ReadFloat()
        {
            return _reader.ReadSingle();
        }

        public int ReadInt()
        {
            return _reader.ReadInt32();
        }

        public long ReadLong()
        {
            return _reader.ReadInt64();
        }

        public sbyte ReadSByte()
        {
            return _reader.ReadSByte();
        }

        public short ReadShort()
        {
            return _reader.ReadInt16();
        }

        public string ReadString()
        {
            return _reader.ReadString();
        }

        public uint ReadUInt()
        {
            return _reader.ReadUInt32();
        }

        public ulong ReadULong()
        {
            return _reader.ReadUInt64();
        }

        public ushort ReadUShort()
        {
            return _reader.ReadUInt16();
        }

        public byte[] ReadByteArray(int count)
        {
            return _reader.ReadBytes(count);
        }

        public void ReadByteArray(byte[] buffer, int index, int count)
        {
            _reader.Read(buffer, index, count);
        }

        public long Seek(long offset, SeekOrigin origin)
        {
            return _memoryStream.Seek(offset, origin);
        }

        public void WriteBool(bool value)
        {
            _writer.Write(value);
        }

        public void WriteSByte(sbyte value)
        {
            _writer.Write(value);
        }

        public void WriteByte(byte value)
        {
            _writer.Write(value);
        }

        public void WriteChar(char value)
        {
            _writer.Write(value);
        }

        public void WriteShort(short value)
        {
            _writer.Write(value);
        }

        public void WriteUShort(ushort value)
        {
            _writer.Write(value);
        }

        public void WriteInt(int value)
        {
            _writer.Write(value);
        }

        public void WriteUInt(uint value)
        {
            _writer.Write(value);
        }

        public void WriteLong(long value)
        {
            _writer.Write(value);
        }

        public void WriteULong(ulong value)
        {
            _writer.Write(value);
        }

        public void WriteFloat(float value)
        {
            _writer.Write(value);
        }

        public void WriteDouble(double value)
        {
            _writer.Write(value);
        }

        public void WriteDecimal(decimal value)
        {
            _writer.Write(value);
        }

        public void WriteString(string value)
        {
            _writer.Write(value);
        }

        public void WriteByteArray(byte[] value, int index, int count)
        {
            _writer.Write(value, index, count);
        }
    }
}
