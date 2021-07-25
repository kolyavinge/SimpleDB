using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Infrastructure
{
    internal interface IReadableStream : IStream
    {
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
    }
}
