using System;
using System.Threading;

namespace CloudBlobBackedObject
{
    internal class MarshalledExceptionsThread
    {
        public MarshalledExceptionsThread(Action code)
        {
            this.exception = null;
            this.thread = new Thread(() =>
            {
                ThrowBackgroundException();

                try
                {
                    code();
                }
                catch (Exception e)
                {
                    lock (this)
                    {
                        this.exception = e;
                    }
                }
            });
        }

        public void Start()
        {
            this.thread.Start();
        }

        public void Join()
        {
            this.thread.Join();

            ThrowBackgroundException();
        }

        ~MarshalledExceptionsThread()
        {
            ThrowBackgroundException();
        }

        private void ThrowBackgroundException()
        {
            if (this.exception == null)
            {
                return;
            }

            lock (this)
            {
                if (this.exception == null)
                {
                    return;
                }

                this.exception = null;

                throw new AggregateException(
                    "An exception was thrown by a background thread",
                    this.exception);
            }
        }

        private Thread thread;
        private Exception exception;
    }
}
