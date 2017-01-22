﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EsentQueue;

namespace QueueDemo
{
    class Program
    {
        private static Random random = new Random();

        static void Main(string[] args)
        {
            const int ItemCount = 50000;
            SingleThreadTest(ItemCount);
            int workers = Environment.ProcessorCount;
            Console.WriteLine($"Using {workers} workers...");
            MultiThreadReadTest(ItemCount, workers);
        }

        private static void MultiThreadReadTest(int itemCount, int workers)
        {
            using (var queue = new PersistentQueue<Event>("test", StartOption.CreateNew))
            {
                var s = new Stopwatch();
                s.Start();
                for (int i = 0; i < itemCount; i++)
                {
                    var evt = CreateEvent();
                    queue.Enqueue(evt);
                }
                s.Stop();
                Console.WriteLine($"Added {itemCount} in {s.Elapsed}");
                Console.WriteLine($"Count: {queue.Count}");

                s.Restart();
                long count = 0;
                QueueItem<Event> item;
                var workerTasks = new Task[workers];
                for (int i = 0; i < workers; i++)
                {
                    workerTasks[i] = Task.Run(() =>
                    {
                        try
                        {
                            while (queue.TryDequeue(out item))
                            {
                                Interlocked.Increment(ref count);
                            }
                        }
                        catch (Exception)
                        {
                            long c = Interlocked.Read(ref count);
                            Console.WriteLine($"Failed at {c}");
                            throw;
                        }
                    });
                }
                Task.WaitAll(workerTasks);
                s.Stop();
                Console.WriteLine($"Removed {itemCount} in {s.Elapsed}");
                Console.WriteLine($"Count: {queue.Count}");
            }
        }

        private static void SingleThreadTest(int itemCount)
        {
            using (var queue = new PersistentQueue<Event>("test", StartOption.CreateNew))
            {
                var s = new Stopwatch();
                s.Start();
                for (int i = 0; i < itemCount; i++)
                {
                    var evt = CreateEvent();
                    queue.Enqueue(evt);
                }
                s.Stop();
                Console.WriteLine($"Added {itemCount} in {s.Elapsed}");
                Console.WriteLine($"Count: {queue.Count}");

                s.Restart();
                QueueItem<Event> item;
                while (queue.TryDequeue(out item))
                {
                    // intentionally blank
                }
                s.Stop();
                Console.WriteLine($"Removed {itemCount} in {s.Elapsed}");
                Console.WriteLine($"Count: {queue.Count}");
            }
        }

        private static Event CreateEvent()
        {
            return new Event()
            {
                MessageId = Guid.NewGuid(),
                Action = random.Next(0, 16),
                Type = random.Next(0, 16),
                ObjectId = Guid.NewGuid(),
                Timestamp = new DateTime(1000000000L + random.Next() * 100L)
            };
        }
    }

    public struct Event
    {
        public Guid MessageId;

        public int Action;

        public int Type;

        public Guid ObjectId;

        public DateTime Timestamp;
    }
}