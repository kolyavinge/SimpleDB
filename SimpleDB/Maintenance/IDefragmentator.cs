using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Maintenance
{
    public interface IDefragmentator
    {
        void DefragmentDataFile(string dataFileName);
    }
}
