using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace SimpleDB.Infrastructure
{
    internal interface IFileStream : IDisposable
    {
        bool EOF { get; }

        long Position { get; }

        void Seek(long offset, SeekOrigin origin);

        bool ReadBool();
        sbyte ReadSByte();
        byte ReadByte();
        char ReadChar();
        short ReadShort();
        ushort ReadUShort();
        int ReadInt();
        uint ReadUInt();
        long ReadLong();
        ulong ReadULong();
        float ReadFloat();
        double ReadDouble();
        decimal ReadDecimal();
        string ReadString();

        void Write(bool value);
        void Write(sbyte value);
        void Write(byte value);
        void Write(char value);
        void Write(short value);
        void Write(ushort value);
        void Write(int value);
        void Write(uint value);
        void Write(long value);
        void Write(ulong value);
        void Write(float value);
        void Write(double value);
        void Write(decimal value);
        void Write(string value);
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

        public void Write(bool value)
        {
            _writer.Write(value);
        }

        public void Write(sbyte value)
        {
            _writer.Write(value);
        }

        public void Write(byte value)
        {
            _writer.Write(value);
        }

        public void Write(char value)
        {
            _writer.Write(value);
        }

        public void Write(short value)
        {
            _writer.Write(value);
        }

        public void Write(ushort value)
        {
            _writer.Write(value);
        }

        public void Write(int value)
        {
            _writer.Write(value);
        }

        public void Write(uint value)
        {
            _writer.Write(value);
        }

        public void Write(long value)
        {
            _writer.Write(value);
        }

        public void Write(ulong value)
        {
            _writer.Write(value);
        }

        public void Write(float value)
        {
            _writer.Write(value);
        }

        public void Write(double value)
        {
            _writer.Write(value);
        }

        public void Write(decimal value)
        {
            _writer.Write(value);
        }

        public void Write(string value)
        {
            _writer.Write(value);
        }
    }
}
