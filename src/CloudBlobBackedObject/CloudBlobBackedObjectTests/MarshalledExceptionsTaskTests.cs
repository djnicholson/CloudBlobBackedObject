using System;
using System.Threading;

using CloudBlobBackedObject;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CloudBlobBackedObjectTests
{
    [TestClass]
    public class MarshalledExceptionsTaskTests
    {
        [TestMethod]
        public void SuppliedCodeIsInvokedWhenWrapperIsConstructed()
        {
            bool taskHasRun = false;
            using (MarshalledExceptionsTask target = new MarshalledExceptionsTask(() => { taskHasRun = true; }))
            {
                SleepToAllowActivityInTask();

                Assert.IsTrue(taskHasRun);
            }
        }

        [TestMethod]
        public void InvocationOfCodeIsNonBlocking()
        {
            ManualResetEventSlim reset = new ManualResetEventSlim(false);
            bool taskHasExited = false;
            using (MarshalledExceptionsTask target = new MarshalledExceptionsTask(
                () =>
                {
                    reset.Wait();
                    taskHasExited = true;
                }))
            {
                Assert.IsFalse(taskHasExited);

                reset.Set();

                SleepToAllowActivityInTask();

                Assert.IsTrue(taskHasExited);
            }
        }

        [TestMethod]
        public void TaskCanBeAwaited()
        {
            bool taskHasExited = false;
            using (MarshalledExceptionsTask target = new MarshalledExceptionsTask(
                () =>
                {
                    Thread.Sleep(TimeSpan.FromSeconds(10.0));
                    taskHasExited = true;
                }))
            {
                Assert.IsFalse(taskHasExited);

                target.Wait();

                Assert.IsTrue(taskHasExited);
            }
        }

        [TestMethod]
        public void WaitThrowsPendingException()
        {
            using (MarshalledExceptionsTask target = NewExceptionThrowerTask())
            {
                try
                {
                    target.Wait();
                    Assert.Fail("target.Wait(); should have thrown an exception");
                }
                catch (AggregateException e)
                {
                    ExampleException innerException = e.InnerException as ExampleException;
                    Assert.IsNotNull(innerException, "Inner exception not correctly wrapped");
                }
            }
        }

        [TestMethod]
        public void WaitOnlyThrowsExceptionOnce()
        {
            using (MarshalledExceptionsTask target = NewExceptionThrowerTask())
            {
                try
                {
                    target.Wait();
                }
                catch (AggregateException)
                {
                }

                target.Wait();
            }
        }

        [TestMethod]
        public void DisposeAwaitsTask()
        {
            bool taskHasExited = false;
            using (MarshalledExceptionsTask target = new MarshalledExceptionsTask(
                () =>
                {
                    Thread.Sleep(TimeSpan.FromSeconds(10.0));
                    taskHasExited = true;
                }))
            {
                Assert.IsFalse(taskHasExited);
            }

            Assert.IsTrue(taskHasExited);
        }

        [TestMethod]
        public void FinalizerNeverThrows()
        {
            MarshalledExceptionsTask target = NewExceptionThrowerTask(); // never disposed
            target = null; // eligible for GC
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            SleepToAllowActivityInTask(); // give all finalizers a chance to finish
        }

        [TestMethod]
        public void DisposeDoesNotThrowIfWaitCalledFirst()
        {
            using (MarshalledExceptionsTask target = NewExceptionThrowerTask())
            {
                try
                {
                    target.Wait();
                    Assert.Fail("target.Wait(); should have thrown an exception");
                }
                catch (AggregateException)
                {
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(AggregateException))]
        public void DisposeMightThrowIfWaitNotCalled()
        {
            using (MarshalledExceptionsTask target = NewExceptionThrowerTask())
            {
            }
        }

        [TestMethod]
        [ExpectedException(typeof(ObjectDisposedException))]
        public void CannotAwaitAfterDispose()
        {
            MarshalledExceptionsTask targetCopy;
            using (MarshalledExceptionsTask target = new MarshalledExceptionsTask(() => { }))
            {
                targetCopy = target;
            }
            targetCopy.Wait();
        }

        private static void SleepToAllowActivityInTask()
        {
            Thread.Sleep(TimeSpan.FromSeconds(2.0));
        }

        private MarshalledExceptionsTask NewExceptionThrowerTask()
        {
            return new MarshalledExceptionsTask(() => { ThrowException(); });
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
