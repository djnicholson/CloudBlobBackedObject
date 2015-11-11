using System;
using System.IO;

namespace CloudBlobBackedObject
{
    /// <summary>
    /// Helper class that facilitates simultaneous identical manipulation of two streams.
    ///
    /// It is assumed that the streams passed to the constructor are in an identical state
    /// at the time of construction, and no code outside of this class modifies them in a way
    /// that makes them no-longer identical.
    /// 
    /// If any of the methods in this class throw an exception, it is no longer guaranteed that
    /// both streams are in a consistent state.
    /// </summary>
    internal class StreamTee : Stream
    {
        public StreamTee(Stream stream1, Stream stream2)
        {
            if (stream1 == null)
            {
                throw new ArgumentNullException("stream1");
            }

            if (stream2 == null)
            {
                throw new ArgumentNullException("stream1");
            }

            this.stream1 = stream1;
            this.stream2 = stream2;
        }

        public override bool CanRead
        {
            get
            {
                return false;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return this.stream1.CanSeek && this.stream2.CanSeek;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return this.stream1.CanWrite && this.stream2.CanWrite;
            }
        }

        public override long Length
        {
            get
            {
                return this.stream1.Length;
            }
        }

        public override long Position
        {
            get
            {
                return this.stream1.Position;
            }

            set
            {
                this.stream1.Position = value;
                this.stream2.Position = value;
            }
        }

        public override void Flush()
        {
            this.stream1.Flush();
            this.stream2.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            this.stream1.Seek(offset, origin);
            return this.stream2.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            this.stream1.SetLength(value);
            this.stream2.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.stream1.Write(buffer, offset, count);
            this.stream2.Write(buffer, offset, count);
        }

        private Stream stream1;
        private Stream stream2;
    }
}
