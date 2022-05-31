namespace SimpleDB.Infrastructure;

internal interface IWriteableStream : IStream
{
    void WriteBool(bool value);
    void WriteSByte(sbyte value);
    void WriteByte(byte value);
    void WriteChar(char value);
    void WriteShort(short value);
    void WriteUShort(ushort value);
    void WriteInt(int value);
    void WriteUInt(uint value);
    void WriteLong(long value);
    void WriteULong(ulong value);
    void WriteFloat(float value);
    void WriteDouble(double value);
    void WriteDecimal(decimal value);
    void WriteString(string value);
    void WriteByteArray(byte[] value, int index, int count);
}
