using System;
using System.Threading;

namespace CloudBlobBackedObject
{
    internal class MarshalledExceptionsThread : IDisposable
    {
        public MarshalledExceptionsThread(Action code)
        {
            this.exception = null;
            
            this.thread = new Thread(() =>
            {
                lock (this.exceptionExclusiveLock)
                {
                    try
                    {
                        code();
                    }
                    catch (Exception e)
                    {
                        this.exception = e;
                    }
                }
            });
        }

        /// <summary>
        /// Calls <see cref="Thread.Start()"/> Start on the underlying thread.
        /// </summary>
        public void Start()
        {
            this.OperateExclusivelyOnThread(t => t.Start());
        }

        /// <summary>
        /// If the thread is running, this call will block until the thread is no longer running.
        /// This method potentially throws (on the caller thread) an exception that was previously
        /// thrown and unhandled by the inner thread.  The method can be called multiple times, but
        /// an individual exception will only be re-thrown once.
        /// </summary>
        public void Join()
        {
            this.OperateExclusivelyOnThread(t => t.Join());
            this.ThrowBackgroundException();
        }

        ~MarshalledExceptionsThread()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// If this object has been disposed, throws an ObjectDisposedException; otherwise returns
        /// a reference to the underlying thread.
        /// </summary>
        private void OperateExclusivelyOnThread(Action<Thread> action)
        {
            lock (this.threadExclusiveLock)
            {
                if (this.thread == null)
                {
                    throw new ObjectDisposedException("MarshalledExceptionsThread");
                }

                action(this.thread);
            }
        }

        private void ThrowBackgroundException()
        {
            lock (this.exceptionExclusiveLock)
            {
                // If we obtain this lock, we can infer that the thread never started, or
                // has finished.

                if (this.exception == null)
                {
                    return;
                }

                Exception innerException = this.exception;

                this.exception = null;

                throw new AggregateException(
                    "An exception was thrown by a background thread",
                    innerException);
            }
        }

        /// <summary>
        /// If the thread is running, this call will block until the thread is no longer running.
        /// This method potentially throws (on the caller thread) an exception that was previously
        /// thrown and unhandled by the inner thread.  The method can be called multiple times, but
        /// an individual exception will only be re-thrown once.
        /// 
        /// If and only if <see cref="Join"/> is called before Dispose, then it is guaranteed that 
        /// Dispose will not throw an exception.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (this.threadExclusiveLock)
                {
                    this.thread = null;
                }

                // Per https://msdn.microsoft.com/en-us/library/b1yfkh5e(v=vs.110).aspx:
                //   AVOID throwing an exception from within Dispose(bool) except under critical 
                //   situations where the containing process has been corrupted (leaks, inconsistent 
                //   shared state, etc.)
                // Clients are advised to not Dispose instances of this class until after joining
                // the inner thread from within their client code; if they follow this advice, they
                // are guaranteed that exceptions are not thrown as a result of calling Dispose().
                // If they ignore this advice, Dispose may throw and clients are responsible for
                // catching and handling this in client code.  The finalizer will never throw.
                this.ThrowBackgroundException();
            }
        }

        private object threadExclusiveLock = new object();
        private object exceptionExclusiveLock = new object();
        private Thread thread; // Class invariant: thread == null => disposed
        private Exception exception;
    }
}
