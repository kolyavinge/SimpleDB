using System;
using System.IO;

namespace SimpleDB.Infrastructure
{
    internal interface IFileStream : IReadableStream, IWriteableStream, IDisposable
    {
        void Flush();
    }

    internal class FileStream : IFileStream
    {
        private readonly System.IO.FileStream _fileStream;
        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;

        public bool EOF { get { return _fileStream.Position >= _fileStream.Length; } }

        public long Position { get { return _fileStream.Position; } }

        public FileStream(System.IO.FileStream fileStream)
        {
            _fileStream = fileStream;
            _reader = new BinaryReader(_fileStream);
            _writer = new BinaryWriter(_fileStream);
        }

        public void Dispose()
        {
            _fileStream.Dispose();
        }

        public void Flush()
        {
            _fileStream.Flush();
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

        public void Seek(long offset, SeekOrigin origin)
        {
            _fileStream.Seek(offset, origin);
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
