using System;
using System.Collections.Generic;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class PrimaryKeyFile : IDisposable
    {
        private readonly string _fileFullPath;
        private readonly Type _primaryKeyType;
        private readonly IFileStream _fileStream;

        public PrimaryKeyFile(string fileFullPath, Type primaryKeyType)
        {
            _fileFullPath = fileFullPath;
            _primaryKeyType = primaryKeyType;
            IOC.Get<IFileSystem>().CreateFileIfNeeded(_fileFullPath);
            _fileStream = IOC.Get<IFileSystem>().OpenFile(_fileFullPath);
        }

        public void Dispose()
        {
            _fileStream.Flush();
            _fileStream.Dispose();
        }

        public IEnumerable<PrimaryKey> GetAllPrimaryKeys()
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.Begin);
            while (_fileStream.EOF == false)
            {
                var primaryKeyFileOffset = _fileStream.Position;
                var primaryKeyFlags = _fileStream.ReadByte();
                var startDataFileOffset = _fileStream.ReadLong();
                var endDataFileOffset = _fileStream.ReadLong();
                object primaryKeyValue;
                if (_primaryKeyType == typeof(sbyte))
                {
                    primaryKeyValue = _fileStream.ReadSByte();
                }
                else if (_primaryKeyType == typeof(byte))
                {
                    primaryKeyValue = _fileStream.ReadByte();
                }
                else if (_primaryKeyType == typeof(char))
                {
                    primaryKeyValue = _fileStream.ReadChar();
                }
                else if (_primaryKeyType == typeof(short))
                {
                    primaryKeyValue = _fileStream.ReadShort();
                }
                else if (_primaryKeyType == typeof(ushort))
                {
                    primaryKeyValue = _fileStream.ReadUShort();
                }
                else if (_primaryKeyType == typeof(int))
                {
                    primaryKeyValue = _fileStream.ReadInt();
                }
                else if (_primaryKeyType == typeof(uint))
                {
                    primaryKeyValue = _fileStream.ReadUInt();
                }
                else if (_primaryKeyType == typeof(long))
                {
                    primaryKeyValue = _fileStream.ReadLong();
                }
                else if (_primaryKeyType == typeof(ulong))
                {
                    primaryKeyValue = _fileStream.ReadULong();
                }
                else if (_primaryKeyType == typeof(float))
                {
                    primaryKeyValue = _fileStream.ReadFloat();
                }
                else if (_primaryKeyType == typeof(double))
                {
                    primaryKeyValue = _fileStream.ReadDouble();
                }
                else if (_primaryKeyType == typeof(decimal))
                {
                    primaryKeyValue = _fileStream.ReadDecimal();
                }
                else if (_primaryKeyType == typeof(string))
                {
                    primaryKeyValue = _fileStream.ReadString();
                }
                else
                {
                    var primaryKeyValueJson = _fileStream.ReadString();
                    primaryKeyValue = JsonSerialization.FromJson(_primaryKeyType, primaryKeyValueJson);
                }
                yield return new PrimaryKey(primaryKeyValue, startDataFileOffset, endDataFileOffset, primaryKeyFileOffset, primaryKeyFlags);
            }
        }

        public PrimaryKey Insert(object value, long startDataFileOffset, long endDataFileOffset)
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.End);
            var primaryKeyFileOffset = _fileStream.Position;
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
                _fileStream.WriteString((string)value);
            }
            else
            {
                var primaryKeyValueJson = JsonSerialization.ToJson(value);
                _fileStream.WriteString(primaryKeyValueJson);
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
            var primaryKeyFlags = _fileStream.ReadByte();
            primaryKeyFlags = PrimaryKey.SetDeleted(primaryKeyFlags);
            _fileStream.Seek(-sizeof(byte), System.IO.SeekOrigin.Current);
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
