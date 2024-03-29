﻿namespace SimpleDB.Infrastructure;

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
    byte[] ReadByteArray(int count);
    void ReadByteArray(byte[] buffer, int index, int count);
}
