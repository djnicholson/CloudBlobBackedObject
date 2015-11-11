using System;
using System.IO;
using System.Security.Cryptography;

namespace CloudBlobBackedObject
{
    /// <summary>
    /// Helper class that does a man-in-the-middle on an arbitrary read-only or write-only stream
    /// and facilitates computing the SHA256 hash of the data that has been read/written.
    /// </summary>
    internal class SHA256StreamHasher : Stream
    {
        /// <summary>
        /// Constructs a writeable stream
        /// </summary>
        public SHA256StreamHasher()
        {
        }

        /// <summary>
        /// Constructs a readable stream
        /// </summary>
        public SHA256StreamHasher(Stream sourceStream)
        {
            this.sourceStream = sourceStream;
        }

        public override bool CanRead
        {
            get
            {
                return !this.finalized && (this.sourceStream != null);
            }
        }

        public override bool CanSeek
        {
            get
            {
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return !this.finalized && (this.sourceStream == null);
            }
        }

        public override long Length
        {
            get
            {
                return position;
            }
        }

        public override long Position
        {
            get
            {
                return this.position;
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (!this.CanRead)
            {
                throw new InvalidOperationException("This SHA256StreamHasher is not readable");
            }

            int bytesRead = this.sourceStream.Read(buffer, offset, count);

            AddDataToHash(buffer, offset, bytesRead);

            return bytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!this.CanWrite)
            {
                throw new InvalidOperationException("This SHA256StreamHasher is not writeable");
            }

            AddDataToHash(buffer, offset, count);
        }

        private void AddDataToHash(byte[] buffer, int offset, int count)
        {
            int bytesHashed = 0;
            while (bytesHashed < count)
            {
                bytesHashed += hasher.TransformBlock(
                    buffer,
                    offset + bytesHashed,
                    count - bytesHashed,
                    buffer,
                    offset + bytesHashed);
            }
            position += count;
        }

        public byte[] ComputeHash()
        {
            if (!finalized)
            {
                lock (this)
                {
                    if (!finalized)
                    {
                        hasher.TransformFinalBlock(new byte[0], 0, 0);
                        finalized = true;
                    }
                }
            }
                
            return hasher.Hash;
        }

        private SHA256 hasher = SHA256Managed.Create();
        long position = 0;
        private bool finalized = false;
        private Stream sourceStream = null;
    }
}
