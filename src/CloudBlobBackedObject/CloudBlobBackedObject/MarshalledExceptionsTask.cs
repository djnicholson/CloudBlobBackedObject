using System;
using System.Threading;
using System.Threading.Tasks;

namespace CloudBlobBackedObject
{
    internal class MarshalledExceptionsTask : IDisposable
    {
        public MarshalledExceptionsTask(Action code)
        {
            this.exception = null;
            
            this.task = Task.Run(() =>
            {
                try
                {
                    code();
                }
                catch (Exception e)
                {
                    this.exception = e;
                }
            });
        }

        /// <summary>
        /// If the task is running, this call will block until the task is no longer running.
        /// This method potentially throws (on the caller thread) an exception that was previously
        /// thrown and unhandled by the inner task.  The method can be called multiple times, but
        /// an individual exception will only be re-thrown once.
        /// </summary>
        public void Wait()
        {
            Task t = this.task;

            if (t == null)
            {
                throw new ObjectDisposedException("MarshalledExceptionsTask");    
            }

            t.Wait();

            this.ThrowBackgroundException();
        }

        ~MarshalledExceptionsTask()
        {
            this.Dispose(false);
        }

        private void ThrowBackgroundException()
        {
            Exception backgroundException = Interlocked.Exchange<Exception>(ref this.exception, null);

            if (backgroundException != null)
            {
                throw new AggregateException(
                    "An exception was thrown by a task",
                    backgroundException);
            }
        }

        /// <summary>
        /// If the task is running, this call will block until the task is no longer running.
        /// This method potentially throws (on the caller thread) an exception that was previously
        /// thrown and unhandled by the inner task.  The method can be called multiple times, but
        /// an individual exception will only be re-thrown once.
        /// 
        /// If and only if <see cref="Wait"/> is called before Dispose, then it is guaranteed that 
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
                Task newTask = null;
                Task previousTask = Interlocked.Exchange<Task>(ref this.task, newTask);

                if (previousTask != null)
                {
                    previousTask.Wait();

                    // Per https://msdn.microsoft.com/en-us/library/b1yfkh5e(v=vs.110).aspx:
                    //   AVOID throwing an exception from within Dispose(bool) except under critical 
                    //   situations where the containing process has been corrupted (leaks, inconsistent 
                    //   shared state, etc.)
                    // Clients are advised to not Dispose instances of this class until after awaiting
                    // the inner task from within their client code; if they follow this advice, they
                    // are guaranteed that exceptions are not thrown as a result of calling Dispose().
                    // If they ignore this advice, Dispose may throw and clients are responsible for
                    // catching and handling this in client code.  The finalizer will never throw.
                    this.ThrowBackgroundException();
                }
            }
        }

        private Task task; // Assigned in constructor. Class invariant: task == null => disposed
        private Exception exception;
    }
}
