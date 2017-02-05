using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;

namespace EsentQueue.Test
{
    [Parallelizable]
    class PersistentQueueTest
    {
        PersistentQueue<int> Queue { get; set; }

        [SetUp]
        public void BeforeEachTest()
        {
            string path = Path.Combine(TestContext.CurrentContext.WorkDirectory, TestContext.CurrentContext.WorkerId, "test.edb");
            TestContext.WriteLine($"Path: {path}");
            Queue = new PersistentQueue<int>(path, StartOption.CreateNew);
        }

        [TearDown]
        public void AfterEachTest()
        {
            Queue?.Dispose();
        }

        [Test]
        public void EnqueueAndCount()
        {
            Queue.Enqueue(1);
            Queue.Enqueue(2);
            Queue.Enqueue(3);
            Assert.AreEqual(3, Queue.Count);
        }

        [Test]
        public void EnqueueAndDequeue()
        {
            Queue.Enqueue(6);
            Queue.Enqueue(5);
            Queue.Enqueue(4);

            Queue.Dequeue().Should().Be(6);
            Queue.Dequeue().Should().Be(5);
            Queue.Dequeue().Should().Be(4);
            Assert.AreEqual(0, Queue.Count);
        }

        [Test]
        public void PeekItem()
        {
            Queue.Enqueue(2);
            Queue.Enqueue(100);
            Queue.Enqueue(11);

            Queue.Peek().Should().Be(2);
            Queue.Peek().Should().Be(2);
            Assert.AreEqual(3, Queue.Count);
        }

        [Test]
        public void EmptyPeekThrowsInvalid()
        {
            Queue.Invoking(q => q.Peek())
                .ShouldThrowExactly<InvalidOperationException>();
        }

        [Test]
        public void EmptyDequeueThrowsInvalid()
        {
            Queue.Invoking(q => q.Dequeue())
                .ShouldThrowExactly<InvalidOperationException>();
        }

        [Test]
        public void ValuesAreAlwayFIFO()
        {
            const int size = 1000;
            int ticks = Environment.TickCount;
            TestContext.WriteLine($"Random input: {ticks}");
            var random = new Random(ticks);

            var values = new List<int>();
            for (int i = 0; i < size; i++)
            {
                int x = random.Next(0, 100 * size);
                values.Add(x);
                Queue.Enqueue(x);
            }

            for (int i = 0; i < size; i++)
            {
                var item = Queue.Dequeue();
                Assert.AreEqual(values[i], item, "index: {0}", i);
            }
        }
    }
}
