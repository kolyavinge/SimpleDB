using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Infrastructure
{
    internal interface IWriteableStream : IStream
    {
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
        void Write(byte[] value, int index, int count);
    }
}
