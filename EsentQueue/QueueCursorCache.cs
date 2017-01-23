using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;

namespace EsentQueue
{
    internal class QueueCursorCache
    {
        private readonly ConcurrentBag<QueueCursor> _cursors = new ConcurrentBag<QueueCursor>();
        private readonly Instance _instance;
        private readonly string _databaseName;

        public QueueCursorCache(Instance instance, string databaseName)
        {
            _instance = instance;
            _databaseName = databaseName;
        }

        public QueueCursor GetCursor()
        {
            QueueCursor cursor;
            if (!_cursors.TryTake(out cursor))
            {
                cursor = new QueueCursor(_instance, _databaseName);
            }

            return cursor;
        }

        public void Return(QueueCursor cursor)
        {
            _cursors.Add(cursor);
        }

        public void FreeAll()
        {
            QueueCursor cursor;
            while (_cursors.TryTake(out cursor))
            {
                cursor.Dispose();
            }
        }
    }
}
