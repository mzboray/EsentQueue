using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace EsentQueue.Test
{
    class LeaseQueueTest
    {
        LeaseQueue<int> Queue { get; set; }

        [SetUp]
        public void BeforeEachTest()
        {
            string path = Path.Combine(TestContext.CurrentContext.WorkDirectory, TestContext.CurrentContext.WorkerId, "test.edb");
            TestContext.WriteLine($"Path: {path}");
            Queue = new LeaseQueue<int>(path);
        }

        [TearDown]
        public void AfterEachTest()
        {
            Queue?.Dispose();
        }

        [Test]
        public void EnqueueCountTest()
        {
            Queue.Enqueue(1);
            Queue.Enqueue(2);
            Queue.Enqueue(3);
            Assert.AreEqual(3, Queue.Count);
        }

        [Test]
        public void TakeLease()
        {
            Queue.Enqueue(1);
            QueueItemLease<int> lease;
            Assert.IsTrue(Queue.TryTakeLease(out lease));
            Assert.AreEqual(1, lease.Item);
        }

        [Test]
        public void EmptyQueueHasNothingToLease()
        {
            QueueItemLease<int> lease;
            Assert.IsFalse(Queue.TryTakeLease(out lease));
        }

        [Test]
        public void TakeLeaseMarkComplete()
        {
            Queue.Enqueue(1);
            QueueItemLease<int> lease;
            Assert.IsTrue(Queue.TryTakeLease(out lease));
            lease.MarkCompleted();
            Assert.AreEqual(0, Queue.Count);
            Assert.IsFalse(Queue.TryTakeLease(out lease));
        }

        [Test]
        public void CannotLeaseItemTwice()
        {
            Queue.Enqueue(1);
            QueueItemLease<int> lease;
            Assert.IsTrue(Queue.TryTakeLease(out lease));
            Assert.IsFalse(Queue.TryTakeLease(out lease));
        }

        [Test]
        public void SecondLeaseGetSecondItem()
        {
            Queue.Enqueue(1);
            Queue.Enqueue(2);
            QueueItemLease<int> lease1, lease2;
            Assert.IsTrue(Queue.TryTakeLease(out lease1));
            Assert.AreEqual(1, lease1.Item);
            Assert.IsTrue(Queue.TryTakeLease(out lease2));
            Assert.AreEqual(2, lease2.Item);
        }

        [Test]
        public void MarkCompletedRemovesItems()
        {
            Queue.Enqueue(1);
            Queue.Enqueue(2);
            QueueItemLease<int> lease;
            Assert.IsTrue(Queue.TryTakeLease(out lease));
            Assert.AreEqual(2, Queue.Count);
            lease.MarkCompleted();
            Assert.AreEqual(1, Queue.Count);
        }

        [Test]
        public void RollbackPutsItemBack()
        {
            Queue.Enqueue(1);
            Queue.Enqueue(2);
            QueueItemLease<int> lease1, lease2;
            Assert.IsTrue(Queue.TryTakeLease(out lease1));
            lease1.Rollback();
            Assert.AreEqual(2, Queue.Count);
            Assert.IsTrue(Queue.TryTakeLease(out lease2));
            Assert.AreEqual(1, lease2.Item);
        }
    }
}
