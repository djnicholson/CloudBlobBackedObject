using CloudBlobBackedObject;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;

namespace CloudBlobBackedObjectTests
{
    [TestClass]
    public class MarshalledExceptionsThreadTests
    {
        [TestCleanup]
        public void ForceGarbageCollectionAfterEachTest()
        {
            try
            {
                GC.Collect();
                GC.Collect();
            }
            catch (AggregateException e)
            {
                Assert.Fail("Unexpected AggregateException during post-test garbage collection: " + e.ToString());
            }
        }

        [TestMethod]
        public void ThreadThatWasNeverStartedNeverThrows()
        {
            using (MarshalledExceptionsThread target = NewExceptionThrowerThread())
            {
            }
        }

        [TestMethod]
        public void SuppliedCodeIsInvokedWhenThreadIsStarted()
        {
            bool threadHasRun = false;
            MarshalledExceptionsThread target = new MarshalledExceptionsThread(() => { threadHasRun = true; });

            Assert.IsFalse(threadHasRun);

            target.Start();

            SleepToAllowActivityInThread();

            Assert.IsTrue(threadHasRun);
        }

        [TestMethod]
        public void InvocationOfCodeIsNonBlocking()
        {
            ManualResetEvent reset = new ManualResetEvent(false);
            bool threadHasExited = false;
            MarshalledExceptionsThread target = new MarshalledExceptionsThread(
                () => 
                {
                    reset.WaitOne();
                    threadHasExited = true;
                });
            target.Start();

            Assert.IsFalse(threadHasExited);

            reset.Set();

            SleepToAllowActivityInThread();

            Assert.IsTrue(threadHasExited);
        }

        [TestMethod]
        public void ThreadCanBeJoined()
        {
            bool threadHasExited = false;
            MarshalledExceptionsThread target = new MarshalledExceptionsThread(
                () =>
                {
                    Thread.Sleep(TimeSpan.FromSeconds(10.0));
                    threadHasExited = true;
                });
            target.Start();

            Assert.IsFalse(threadHasExited);

            target.Join();

            Assert.IsTrue(threadHasExited);
        }

        [TestMethod]
        public void JoinThrowsPendingException()
        {
            MarshalledExceptionsThread target = NewExceptionThrowerThread();
            target.Start();

            try
            {
                target.Join();
                Assert.Fail("target.Join(); should have thrown an exception");
            }
            catch (AggregateException e)
            {
                ExampleException innerException = e.InnerException as ExampleException;
                Assert.IsNotNull(innerException, "Inner exception not correctly wrapped");
            }
        }

        [TestMethod]
        public void JoinOnlyThrowsExceptionOnce()
        {
            MarshalledExceptionsThread target = NewExceptionThrowerThread();
            target.Start();

            try
            {
                target.Join();
            }
            catch (AggregateException)
            {
            }

            target.Join();
        }

        [TestMethod]
        public void DisposeJoinsThread()
        {
            bool threadHasExited = false;
            using (MarshalledExceptionsThread target = new MarshalledExceptionsThread(
                () =>
                {
                    Thread.Sleep(TimeSpan.FromSeconds(10.0));
                    threadHasExited = true;
                }))
            {
                target.Start();
                Assert.IsFalse(threadHasExited);
            }

            Assert.IsTrue(threadHasExited);
        }

        [TestMethod]
        public void FinalizerNeverThrows()
        {
            MarshalledExceptionsThread target = NewExceptionThrowerThread(); // never disposed
            target.Start();
            target = null;
            GC.Collect();
            GC.Collect();
        }

        [TestMethod]
        public void DisposeDoesNotThrowIfJoinCalledFirst()
        {
            using (MarshalledExceptionsThread target = NewExceptionThrowerThread())
            {
                target.Start();

                try
                {
                    target.Join();
                    Assert.Fail("target.Join(); should have thrown an exception");
                }
                catch (AggregateException)
                {
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(AggregateException))]
        public void DisposeMightThrowIfJoinNotCalled()
        {
            using (MarshalledExceptionsThread target = NewExceptionThrowerThread())
            {
                target.Start();
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void CannotJoinAfterDispose()
        {
            MarshalledExceptionsThread targetCopy;
            using (MarshalledExceptionsThread target = new MarshalledExceptionsThread(() => { }))
            {
                targetCopy = target;
                target.Start();
            }
            targetCopy.Join();
        }

        [TestMethod]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void CannotStartAfterDispose()
        {
            MarshalledExceptionsThread targetCopy;
            using (MarshalledExceptionsThread target = new MarshalledExceptionsThread(() => { }))
            {
                targetCopy = target;
            }
            targetCopy.Start();
        }

        private static void SleepToAllowActivityInThread()
        {
            Thread.Sleep(TimeSpan.FromSeconds(2.0));
        }

        private MarshalledExceptionsThread NewExceptionThrowerThread()
        {
            return new MarshalledExceptionsThread(() => { ThrowException(); });
        }

        private class ExampleException : Exception
        {
        }

        private void ThrowException()
        {
            throw new ExampleException();
        }
    }
}
