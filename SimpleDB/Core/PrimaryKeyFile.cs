using System;
using System.Collections.Generic;
using System.Text;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class PrimaryKeyFile
    {
        private readonly string _fileFullPath;
        private readonly Type _primaryKeyType;
        private IFileStream _fileStream;

        public PrimaryKeyFile(string fileFullPath, Type primaryKeyType)
        {
            _fileFullPath = fileFullPath;
            _primaryKeyType = primaryKeyType;
            IOC.Get<IFileSystem>().CreateFileIfNeeded(_fileFullPath);
        }

        public void BeginRead()
        {
            _fileStream = IOC.Get<IFileSystem>().OpenFileRead(_fileFullPath);
        }

        public void BeginWrite()
        {
            _fileStream = IOC.Get<IFileSystem>().OpenFileWrite(_fileFullPath);
        }

        public void EndReadWrite()
        {
            _fileStream.Dispose();
        }

        public IEnumerable<PrimaryKey> GetAllPrimaryKeys()
        {
            var currentPosition = _fileStream.Seek(0, System.IO.SeekOrigin.Begin);
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
                else
                {
                    var length = _fileStream.ReadInt();
                    currentPosition += sizeof(int) + length;
                    var bytes = _fileStream.ReadByteArray(length);
                    var fieldValueJson = Encoding.UTF8.GetString(bytes);
                    primaryKeyValue = JsonSerialization.FromJson(_primaryKeyType, fieldValueJson);
                }
                yield return new PrimaryKey(primaryKeyValue, startDataFileOffset, endDataFileOffset, primaryKeyFileOffset, primaryKeyFlags);
            }
        }

        public PrimaryKey Insert(object value, long startDataFileOffset, long endDataFileOffset)
        {
            var primaryKeyFileOffset = _fileStream.Seek(0, System.IO.SeekOrigin.End);
            byte primaryKeyFlags = 0;
            _fileStream.WriteByte(primaryKeyFlags);
            _fileStream.WriteLong(startDataFileOffset);
            _fileStream.WriteLong(endDataFileOffset);
            if (_primaryKeyType == typeof(sbyte))
            {
                _fileStream.WriteSByte((sbyte)value);
            }
            else if (_primaryKeyType == typeof(byte))
            {
                _fileStream.WriteByte((byte)value);
            }
            else if (_primaryKeyType == typeof(char))
            {
                _fileStream.WriteChar((char)value);
            }
            else if (_primaryKeyType == typeof(short))
            {
                _fileStream.WriteShort((short)value);
            }
            else if (_primaryKeyType == typeof(ushort))
            {
                _fileStream.WriteUShort((ushort)value);
            }
            else if (_primaryKeyType == typeof(int))
            {
                _fileStream.WriteInt((int)value);
            }
            else if (_primaryKeyType == typeof(uint))
            {
                _fileStream.WriteUInt((uint)value);
            }
            else if (_primaryKeyType == typeof(long))
            {
                _fileStream.WriteLong((long)value);
            }
            else if (_primaryKeyType == typeof(ulong))
            {
                _fileStream.WriteULong((ulong)value);
            }
            else if (_primaryKeyType == typeof(float))
            {
                _fileStream.WriteFloat((float)value);
            }
            else if (_primaryKeyType == typeof(double))
            {
                _fileStream.WriteDouble((double)value);
            }
            else if (_primaryKeyType == typeof(decimal))
            {
                _fileStream.WriteDecimal((decimal)value);
            }
            else if (_primaryKeyType == typeof(string))
            {
                var str = (string)value;
                if (str == null)
                {
                    throw new PrimaryKeyException();
                }
                else
                {
                    var bytes = Encoding.UTF8.GetBytes(str);
                    _fileStream.WriteInt(bytes.Length);
                    _fileStream.WriteByteArray(bytes, 0, bytes.Length);
                }
            }
            else
            {
                var primaryKeyValueJson = JsonSerialization.ToJson(value);
                var strBytes = Encoding.UTF8.GetBytes(primaryKeyValueJson);
                _fileStream.WriteInt(strBytes.Length);
                _fileStream.WriteByteArray(strBytes, 0, strBytes.Length);
            }

            return new PrimaryKey(value, startDataFileOffset, endDataFileOffset, primaryKeyFileOffset, primaryKeyFlags);
        }

        public void UpdateStartEndDataFileOffset(long primaryKeyFileOffset, long newStartDataFileOffset, long newEndDataFileOffset)
        {
            _fileStream.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            _fileStream.Seek(sizeof(byte), System.IO.SeekOrigin.Current); // skip 'primaryKeyFlags'
            _fileStream.WriteLong(newStartDataFileOffset);
            _fileStream.WriteLong(newEndDataFileOffset);
        }

        public void UpdateEndDataFileOffset(long primaryKeyFileOffset, long newEndDataFileOffset)
        {
            _fileStream.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            _fileStream.Seek(sizeof(byte) + sizeof(long), System.IO.SeekOrigin.Current); // skip 'primaryKeyFlags' and 'startDataFileOffset'
            _fileStream.WriteLong(newEndDataFileOffset);
        }

        public void Delete(long primaryKeyFileOffset)
        {
            _fileStream.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            var primaryKeyFlags = PrimaryKey.SetDeleted(0);
            _fileStream.WriteByte(primaryKeyFlags);
        }
    }

    internal static class PrimaryKeyFileName
    {
        public static string FromCollectionName(string collectionName)
        {
            return String.Format("{0}.primary", collectionName);
        }
    }
}
