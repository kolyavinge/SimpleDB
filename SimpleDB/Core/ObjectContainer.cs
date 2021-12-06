using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class ObjectContainer
    {
        private readonly string _fieldValueJson;

        public ObjectContainer(string fieldValueJson)
        {
            _fieldValueJson = fieldValueJson;
        }

        public override string ToString()
        {
            return _fieldValueJson;
        }
    }
}
