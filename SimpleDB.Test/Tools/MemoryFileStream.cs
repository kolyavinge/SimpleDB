using System.IO;
using SimpleDB.Infrastructure;

namespace SimpleDB.Test.Tools
{
    public class MemoryFileStream : IFileStream
    {
        private readonly MemoryStream _stream;
        private readonly BinaryReader _reader;
        private readonly BinaryWriter _writer;

        public MemoryFileStream()
        {
            _stream = new MemoryStream();
            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_stream);
        }

        public MemoryFileStream(byte[] bytes)
        {
            _stream = new MemoryStream(bytes);
            _reader = new BinaryReader(_stream);
            _writer = new BinaryWriter(_stream);
        }

        public bool DidRead { get; set; }

        public bool DidWrite { get; set; }

        public string FileName { get; set; }

        public long Position { get { return _stream.Position; } }

        public long Length { get { return _stream.Length; } }

        public void Flush() { }

        public void Dispose() { }

        public bool ReadBool()
        {
            DidRead = true;
            return _reader.ReadBoolean();
        }

        public byte ReadByte()
        {
            DidRead = true;
            return _reader.ReadByte();
        }

        public char ReadChar()
        {
            DidRead = true;
            return _reader.ReadChar();
        }

        public decimal ReadDecimal()
        {
            DidRead = true;
            return _reader.ReadDecimal();
        }

        public double ReadDouble()
        {
            DidRead = true;
            return _reader.ReadDouble();
        }

        public float ReadFloat()
        {
            DidRead = true;
            return _reader.ReadSingle();
        }

        public int ReadInt()
        {
            DidRead = true;
            return _reader.ReadInt32();
        }

        public long ReadLong()
        {
            DidRead = true;
            return _reader.ReadInt64();
        }

        public sbyte ReadSByte()
        {
            DidRead = true;
            return _reader.ReadSByte();
        }

        public short ReadShort()
        {
            DidRead = true;
            return _reader.ReadInt16();
        }

        public uint ReadUInt()
        {
            DidRead = true;
            return _reader.ReadUInt32();
        }

        public ulong ReadULong()
        {
            DidRead = true;
            return _reader.ReadUInt64();
        }

        public ushort ReadUShort()
        {
            DidRead = true;
            return _reader.ReadUInt16();
        }

        public string ReadString()
        {
            DidRead = true;
            return _reader.ReadString();
        }

        public byte[] ReadByteArray(int count)
        {
            DidRead = true;
            return _reader.ReadBytes(count);
        }

        public void ReadByteArray(byte[] buffer, int index, int count)
        {
            DidRead = true;
            _reader.Read(buffer, index, count);
        }

        public long Seek(long offset, SeekOrigin origin)
        {
            return _stream.Seek(offset, origin);
        }

        public void SetLength(long length)
        {
            _stream.SetLength(length);
        }

        public void WriteBool(bool value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteSByte(sbyte value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteByte(byte value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteChar(char value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteShort(short value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteUShort(ushort value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteInt(int value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteUInt(uint value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteLong(long value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteULong(ulong value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteFloat(float value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteDouble(double value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteDecimal(decimal value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteString(string value)
        {
            DidWrite = true;
            _writer.Write(value);
        }

        public void WriteByteArray(byte[] value, int index, int count)
        {
            DidWrite = true;
            _writer.Write(value, index, count);
        }
    }
}
