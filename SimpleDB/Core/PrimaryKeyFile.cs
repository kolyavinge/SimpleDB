using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using SimpleDB.Infrastructure;

[assembly: InternalsVisibleTo("SimpleDB.Test")]

namespace SimpleDB.Core
{
    internal class PrimaryKeyFile
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

        public PrimaryKey Insert(object value, long startDataFileOffset, long endDataFileOffset)
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.End);
            var primaryKeyFileOffset = _fileStream.Position;
            _fileStream.Write(startDataFileOffset);
            _fileStream.Write(endDataFileOffset);
            if (_primaryKeyType == typeof(sbyte))
            {
                _fileStream.Write((sbyte)value);
            }
            else if (_primaryKeyType == typeof(byte))
            {
                _fileStream.Write((byte)value);
            }
            else if (_primaryKeyType == typeof(char))
            {
                _fileStream.Write((char)value);
            }
            else if (_primaryKeyType == typeof(short))
            {
                _fileStream.Write((short)value);
            }
            else if (_primaryKeyType == typeof(ushort))
            {
                _fileStream.Write((ushort)value);
            }
            else if (_primaryKeyType == typeof(int))
            {
                _fileStream.Write((int)value);
            }
            else if (_primaryKeyType == typeof(uint))
            {
                _fileStream.Write((uint)value);
            }
            else if (_primaryKeyType == typeof(long))
            {
                _fileStream.Write((long)value);
            }
            else if (_primaryKeyType == typeof(ulong))
            {
                _fileStream.Write((ulong)value);
            }
            else if (_primaryKeyType == typeof(float))
            {
                _fileStream.Write((float)value);
            }
            else if (_primaryKeyType == typeof(double))
            {
                _fileStream.Write((double)value);
            }
            else if (_primaryKeyType == typeof(decimal))
            {
                _fileStream.Write((decimal)value);
            }
            else if (_primaryKeyType == typeof(string))
            {
                _fileStream.Write((string)value);
            }
            else
            {
                var primaryKeyValueJson = JsonSerialization.ToJson(value);
                _fileStream.Write(primaryKeyValueJson);
            }

            return new PrimaryKey(value, startDataFileOffset, endDataFileOffset, primaryKeyFileOffset);
        }

        public void UpdateStartEndDataFileOffset(long primaryKeyFileOffset, long newStartDataFileOffset, long newEndDataFileOffset)
        {
            _fileStream.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            _fileStream.Write(newStartDataFileOffset);
            _fileStream.Write(newEndDataFileOffset);
        }

        public void UpdateEndDataFileOffset(long primaryKeyFileOffset, long newEndDataFileOffset)
        {
            _fileStream.Seek(primaryKeyFileOffset, System.IO.SeekOrigin.Begin);
            _fileStream.Seek(8, System.IO.SeekOrigin.Current); // skip 'startDataFileOffset'
            _fileStream.Write(newEndDataFileOffset);
        }

        public IEnumerable<PrimaryKey> GetAllPrimaryKeys()
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.Begin);
            while (_fileStream.EOF == false)
            {
                var primaryKeyFileOffset = _fileStream.Position;
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
                yield return new PrimaryKey(primaryKeyValue, startDataFileOffset, endDataFileOffset, primaryKeyFileOffset);
            }
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
