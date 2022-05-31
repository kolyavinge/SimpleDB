namespace SimpleDB.Core;

internal class PrimaryKey
{
    internal static PrimaryKey Dummy = new(0, 0, 0, 0, 0);

    public const byte FieldNumber = 0;

    public object Value { get; }

    public long StartDataFileOffset { get; set; }

    public long EndDataFileOffset { get; set; }

    public long PrimaryKeyFileOffset { get; set; }

    public byte Flags { get; set; }

    public PrimaryKey(object value, long startDataFileOffset, long endDataFileOffset, long primaryKeyFileOffset, byte flags)
    {
        Value = value;
        StartDataFileOffset = startDataFileOffset;
        EndDataFileOffset = endDataFileOffset;
        PrimaryKeyFileOffset = primaryKeyFileOffset;
        Flags = flags;
    }

    public bool IsDeleted => (Flags & 0x00000001) == 1;

    public static byte SetDeleted(byte primaryKeyFlags)
    {
        return (byte)(primaryKeyFlags | 0x00000001);
    }
}
