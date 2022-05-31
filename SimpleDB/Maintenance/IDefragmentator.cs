namespace SimpleDB.Maintenance;

public interface IDefragmentator
{
    void DefragmentDataFile(string dataFileName);
}
