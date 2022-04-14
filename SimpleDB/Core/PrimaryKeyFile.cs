using System;
using System.Collections.Generic;
using System.Text;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class PrimaryKeyFile
    {
        private readonly Type _primaryKeyType;
        private readonly IFileSystem _fileSystem;
        private readonly IMemoryBuffer _memoryBuffer;
        private IFileStream? _fileStream;

        public string FileName { get; }

        public PrimaryKeyFile(string fileName, Type primaryKeyType, IFileSystem fileSystem, IMemory memory)
        {
            FileName = fileName;
            _primaryKeyType = primaryKeyType;
            _fileSystem = fileSystem;
            _memoryBuffer = memory.GetBuffer();
        }

        public long SizeInBytes => _fileStream!.Length;

        public void BeginRead()
        {
            _fileStream = _fileSystem.OpenFileRead(FileName);
        }

        public void BeginReadWrite()
        {
            _fileStream = _fileSystem.OpenFileReadWrite(FileName);
        }

        public void EndReadWrite()
        {
            _fileStream!.Dispose();
        }

        public IEnumerable<PrimaryKey> GetAllPrimaryKeys()
        {
            var currentPosition = _fileStream!.Seek(0, System.IO.SeekOrigin.Begin);
            var fileStreamLength = _fileStream.Length;
            while (currentPosition < fileStreamLength)
            {
                var primaryKeyFileOffset = currentPosition;
                var primaryKeyFlags = _fileStream.ReadByte();
                var startDataFileOffset = _fileStream.ReadLong();
                var endDataFileOffset = _fileStream.ReadLong();
                currentPosition += sizeof(byte) + 2 * sizeof(long);
                object primaryKeyValue;
                if (_primaryKeyType == typeof(sbyte))
                {
                    primaryKeyValue = _fileStream.ReadSByte();
                    currentPosition += sizeof(sbyte);
                }
                else if (_primaryKeyType == typeof(byte))
                {
                    primaryKeyValue = _fileStream.ReadByte();
                    currentPosition += sizeof(byte);
                }
                else if (_primaryKeyType == typeof(char))
                {
                    primaryKeyValue = _fileStream.ReadChar();
                    currentPosition += sizeof(char);
                }
                else if (_primaryKeyType == typeof(short))
                {
                    primaryKeyValue = _fileStream.ReadShort();
                    currentPosition += sizeof(short);
                }
                else if (_primaryKeyType == typeof(ushort))
                {
                    primaryKeyValue = _fileStream.ReadUShort();
                    currentPosition += sizeof(ushort);
                }
                else if (_primaryKeyType == typeof(int))
                {
                    primaryKeyValue = _fileStream.ReadInt();
                    currentPosition += sizeof(int);
                }
                else if (_primaryKeyType == typeof(uint))
                {
                    primaryKeyValue = _fileStream.ReadUInt();
                    currentPosition += sizeof(uint);
                }
                else if (_primaryKeyType == typeof(long))
                {
                    primaryKeyValue = _fileStream.ReadLong();
                    currentPosition += sizeof(long);
                }
                else if (_primaryKeyType == typeof(ulong))
                {
                    primaryKeyValue = _fileStream.ReadULong();
                    currentPosition += sizeof(ulong);
                }
                else if (_primaryKeyType == typeof(float))
                {
                    primaryKeyValue = _fileStream.ReadFloat();
                    currentPosition += sizeof(float);
                }
                else if (_primaryKeyType == typeof(double))
                {
                    primaryKeyValue = _fileStream.ReadDouble();
                    currentPosition += sizeof(double);
                }
                else if (_primaryKeyType == typeof(decimal))
                {
                    primaryKeyValue = _fileStream.ReadDecimal();
                    currentPosition += sizeof(decimal);
                }
                else if (_primaryKeyType == typeof(string))
                {
                    var length = _fileStream.ReadInt();
                    currentPosition += sizeof(int) + length;
                    var bytes = _fileStream.ReadByteArray(length);
                    primaryKeyValue = Encoding.UTF8.GetString(bytes);
                }
                else if (_primaryKeyType == typeof(byte[]))
                {
                    var length = _fileStream.ReadInt();
                    currentPosition += sizeof(int) + length;
                    primaryKeyValue = _fileStream.ReadByteArray(length);
                }
                else
                {
                    var length = _fileStream.ReadInt();
                    currentPosition += sizeof(int) + length;
                    var bytes = _fileStream.ReadByteArray(length);
                    var fieldValueJson = Encoding.UTF8.GetString(bytes);
                    primaryKeyValue = JsonSerialization.FromJson(_primaryKeyType, fieldValueJson) ?? throw new PrimaryKeyException();
                }
                yield return new PrimaryKey(primaryKeyValue, startDataFileOffset, endDataFileOffset, primaryKeyFileOffset, primaryKeyFlags);
            }
        }

        public PrimaryKey Insert(object value, long startDataFileOffset, long endDataFileOffset)
        {
            var primaryKeyFileOffset = _fileStream!.Seek(0, System.IO.SeekOrigin.End);
            return Insert(_fileStream, value, primaryKeyFileOffset, startDataFileOffset, endDataFileOffset);
        }

        private PrimaryKey Insert(IWriteableStream stream, object? value, long primaryKeyFileOffset, long startDataFileOffset, long endDataFileOffset)
        {
            byte primaryKeyFlags = 0;
            stream.WriteByte(primaryKeyFlags);
            stream.WriteLong(startDataFileOffset);
            stream.WriteLong(endDataFileOffset);
            if (_primaryKeyType == typeof(sbyte))
            {
                stream.WriteSByte((sbyte)value!);
            }
            else if (_primaryKeyType == typeof(byte))
            {
                stream.WriteByte((byte)value!);
            }
            else if (_primaryKeyType == typeof(char))
            {
                stream.WriteChar((char)value!);
            }
            else if (_primaryKeyType == typeof(short))
            {
                stream.WriteShort((short)value!);
            }
            else if (_primaryKeyType == typeof(ushort))
            {
                stream.WriteUShort((ushort)value!);
            }
            else if (_primaryKeyType == typeof(int))
            {
                stream.WriteInt((int)value!);
            }
            else if (_primaryKeyType == typeof(uint))
            {
                stream.WriteUInt((uint)value!);
            }
            else if (_primaryKeyType == typeof(long))
            {
                stream.WriteLong((long)value!);
            }
            else if (_primaryKeyType == typeof(ulong))
            {
                stream.WriteULong((ulong)value!);
            }
            else if (_primaryKeyType == typeof(float))
            {
                stream.WriteFloat((float)value!);
            }
            else if (_primaryKeyType == typeof(double))
            {
                stream.WriteDouble((double)value!);
            }
            else if (_primaryKeyType == typeof(decimal))
            {
                stream.WriteDecimal((decimal)value!);
            }
            else if (_primaryKeyType == typeof(string))
            {
                if (value is string str)
                {
                    var bytes = Encoding.UTF8.GetBytes(str);
                    stream.WriteInt(bytes.Length);
                    stream.WriteByteArray(bytes, 0, bytes.Length);
                }
                else
                {
                    throw new PrimaryKeyException();
                }
            }
            else if (_primaryKeyType == typeof(byte[]))
            {
                if (value is byte[] bytes)
                {
                    stream.WriteInt(bytes.Length);
                    stream.WriteByteArray(bytes, 0, bytes.Length);
                }
                else
                {
                    throw new PrimaryKeyException();
                }
            }
            else
            {
                var primaryKeyValueJson = JsonSerialization.ToJson(value!);
                var strBytes = Encoding.UTF8.GetBytes(primaryKeyValueJson);
                stream.WriteInt(strBytes.Length);
                stream.WriteByteArray(strBytes, 0, strBytes.Length);
            }

            if (value == null) throw new PrimaryKeyException();

            return new PrimaryKey(value, startDataFileOffset, endDataFileOffset, primaryKeyFileOffset, primaryKeyFlags);
        }

        public void UpdatePrimaryKey(PrimaryKey primaryKey, long newStartDataFileOffset, long newEndDataFileOffset)
        {
            if (primaryKey.StartDataFileOffset != newStartDataFileOffset)
            {
                UpdateStartEndDataFileOffset(primaryKey.PrimaryKeyFileOffset, newStartDataFileOffset, newEndDataFileOffset);
                primaryKey.StartDataFileOffset = newStartDataFileOffset;
                primaryKey.EndDataFileOffset = newEndDataFileOffset;
            }
            else if (primaryKey.EndDataFileOffset != newEndDataFileOffset)
            {
                UpdateEndDataFileOffset(primaryKey.PrimaryKeyFileOffset, newEndDataFileOffset);
                primaryKey.EndDataFileOffset = newEndDataFileOffset;
            }
        }

        public void UpdateStartEndDataFileOffset(long primaryKeyFileOffset, long newStartDataFileOffset, long newEndDataFileOffset)
        {
            _fileStream!.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            _fileStream.Seek(sizeof(byte), System.IO.SeekOrigin.Current); // skip 'primaryKeyFlags'
            _fileStream.WriteLong(newStartDataFileOffset);
            _fileStream.WriteLong(newEndDataFileOffset);
        }

        public void UpdateEndDataFileOffset(long primaryKeyFileOffset, long newEndDataFileOffset)
        {
            _fileStream!.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            _fileStream.Seek(sizeof(byte) + sizeof(long), System.IO.SeekOrigin.Current); // skip 'primaryKeyFlags' and 'startDataFileOffset'
            _fileStream.WriteLong(newEndDataFileOffset);
        }

        public void Delete(long primaryKeyFileOffset)
        {
            _fileStream!.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            var primaryKeyFlags = PrimaryKey.SetDeleted(0);
            _fileStream.WriteByte(primaryKeyFlags);
        }

        public int CalculateSize(PrimaryKey primaryKey)
        {
            _memoryBuffer.Seek(0, System.IO.SeekOrigin.Begin);
            Insert(_memoryBuffer, primaryKey.Value, 0, primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset);
            var size = _memoryBuffer.Position;

            return (int)size;
        }
    }

    internal interface IPrimaryKeyFileFactory
    {
        PrimaryKeyFile MakeFromFileName(string fileName, Type primaryKeyType);
        PrimaryKeyFile MakeFromEntityName(string entityName, Type primaryKeyType);
    }

    internal class PrimaryKeyFileFactory : IPrimaryKeyFileFactory
    {
        private readonly IFileSystem _fileSystem;
        private readonly IMemory _memory;

        public PrimaryKeyFileFactory(IFileSystem fileSystem, IMemory? memory = null)
        {
            _fileSystem = fileSystem;
            _memory = memory ?? Memory.Instance;
        }

        public PrimaryKeyFile MakeFromFileName(string fileName, Type primaryKeyType)
        {
            return new PrimaryKeyFile(fileName, primaryKeyType, _fileSystem, _memory);
        }

        public PrimaryKeyFile MakeFromEntityName(string entityName, Type primaryKeyType)
        {
            return MakeFromFileName(PrimaryKeyFileName.FromEntityName(entityName), primaryKeyType);
        }
    }

    internal static class PrimaryKeyFileName
    {
        public const string Extension = ".primary";

        public static string FromEntityName(string entityName)
        {
            return $"{entityName}{Extension}";
        }
    }
}
