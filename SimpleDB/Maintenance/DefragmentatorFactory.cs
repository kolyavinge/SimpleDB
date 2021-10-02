using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Maintenance
{
    public static class DefragmentatorFactory
    {
        public static IDefragmentator MakeDefragmentator(string workingDirectory)
        {
            GlobalSettings.WorkingDirectory = workingDirectory;
            return new Defragmentator();
        }
    }
}
