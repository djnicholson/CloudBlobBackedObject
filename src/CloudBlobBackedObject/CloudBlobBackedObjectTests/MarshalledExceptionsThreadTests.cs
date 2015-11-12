﻿using CloudBlobBackedObject;
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
            }
            catch (AggregateException e)
            {
                Assert.Fail("Unexpected AggregateException during post-test garbage collection: " + e.ToString());
            }
        }

        [TestMethod]
        public void ThreadThatWasNeverStartedNeverThrows()
        {
            MarshalledExceptionsThread target = NewExceptionThrowerThread();
            target = null;
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
