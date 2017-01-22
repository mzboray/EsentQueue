using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EsentQueue
{
    public struct QueueItem<T>
    {
        public QueueItem(T item, int attemptCount)
        {
            Item = item;
            AttemptCount = attemptCount;
        }

        public T Item { get; }

        public int AttemptCount { get; }
    }
}
