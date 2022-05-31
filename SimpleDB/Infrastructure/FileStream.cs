using System;
using System.IO;

namespace SimpleDB.Infrastructure;

internal interface IFileStream : IReadableStream, IWriteableStream, IDisposable
{
    string Name { get; }
    void SetLength(long length);
}

internal class FileStream : IFileStream
{
    private readonly Stream _stream;
    private readonly Action<IFileStream> _disposeFunc;
    private readonly BinaryReader? _reader;
    private readonly BinaryWriter? _writer;

    public string Name { get; }

    public long Position => _stream.Position;

    public long Length => _stream.Length;

    public FileStream(string fileName, Stream fileStream, Action<IFileStream> disposeFunc)
    {
        Name = fileName;
        _stream = fileStream;
        _disposeFunc = disposeFunc;
        if (_stream.CanRead)
        {
            _reader = new BinaryReader(_stream);
        }
        if (_stream.CanWrite)
        {
            _writer = new BinaryWriter(_stream);
        }
    }

    public void Dispose()
    {
        _stream.Dispose();
        _disposeFunc(this);
    }

    public bool ReadBool()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadBoolean();
    }

    public byte ReadByte()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadByte();
    }

    public char ReadChar()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadChar();
    }

    public decimal ReadDecimal()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadDecimal();
    }

    public double ReadDouble()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadDouble();
    }

    public float ReadFloat()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadSingle();
    }

    public int ReadInt()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadInt32();
    }

    public long ReadLong()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadInt64();
    }

    public sbyte ReadSByte()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadSByte();
    }

    public short ReadShort()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadInt16();
    }

    public string ReadString()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadString();
    }

    public uint ReadUInt()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadUInt32();
    }

    public ulong ReadULong()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadUInt64();
    }

    public ushort ReadUShort()
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadUInt16();
    }

    public byte[] ReadByteArray(int count)
    {
        if (_reader == null) throw new IOException("Read is not allowed");
        return _reader.ReadBytes(count);
    }

    public void ReadByteArray(byte[] buffer, int index, int count)
    {
        if (_reader == null) throw new IOException("Read is not allowed");
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
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteSByte(sbyte value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteByte(byte value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteChar(char value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteShort(short value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteUShort(ushort value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteInt(int value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteUInt(uint value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteLong(long value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteULong(ulong value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteFloat(float value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteDouble(double value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteDecimal(decimal value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteString(string value)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value);
    }

    public void WriteByteArray(byte[] value, int index, int count)
    {
        if (_writer == null) throw new IOException("Write is not allowed");
        _writer.Write(value, index, count);
    }
}
