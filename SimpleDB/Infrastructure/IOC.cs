using System;
using System.Collections.Generic;
using System.Text;
using Ninject;

namespace SimpleDB.Infrastructure
{
    internal static class IOC
    {
        private static IKernel _kernel;

        static IOC()
        {
            _kernel = new StandardKernel();
        }

        public static T Get<T>()
        {
            return _kernel.Get<T>();
        }

        public static void Set<T>(T obj)
        {
            if (_kernel.TryGet<T>() == null)
            {
                _kernel.Bind<T>().ToMethod(_ => obj);
            }
        }

        public static void Reset()
        {
            _kernel = new StandardKernel();
        }
    }
}
