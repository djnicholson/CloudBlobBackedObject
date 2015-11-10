using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;

namespace CloudBlobBackedObject
{
    /// <summary>
    /// Simplifies persisting state of arbitrary objects across program invocations;
    /// provides a mechanism to give one application near-realtime visibility into
    /// another application's state.
    /// 
    /// State is mastered in Azure Blob Storage.  Different program invocations periodically
    /// push and/or pull to/from Azure to update local state.  It is possible to take a
    /// lease on the backing blob (thus preventing other invocations from also writing to
    /// the backing data in the blob).
    /// </summary>
    /// <typeparam name="T">Any serializable class</typeparam>
    public class CloudBlobBacked<T> where T : class
    {
        /// <summary>
        /// The minimum lease duration that may be passed to the constructor.
        /// </summary>
        public const int MinimumLeaseInSeconds = 20;

        /// <summary>
        /// How often the lease is refreshed.  The lower this value is, the less likely we are
        /// to lose the lease due to slowdown of the renewal thread, but the more "chatty" we are
        /// to the storage service.
        /// </summary>
        public const int LeaseRenewalIntervalInSeconds = 5;

        /// <summary>
        /// Create a wrapper around an object that is backed in Azure Blob Storage.
        /// </summary>
        /// <param name="backingBlob">
        /// A reference to the blob that the object will be backed by.  This blob
        /// does not have to exist.  If it does not exist it will be immeadiately
        /// created with contents representing a null object.
        /// </param>
        /// <param name="leaseDuration">
        /// If set, a lease of the specified duration will be taken on the backing
        /// blob; no other writable instances of this class may be constructed from 
        /// this blob while this object is active (and possibly for up to leaseDuration
        /// afterwards in the event of an unclean shutdown).  If set, this value must
        /// be at least MinimumLeaseInSeconds.  Set to null if you only intend to read
        /// from the object, or are more resilient to writes being over-written by
        /// other instances of this class referring to the same blob.
        /// </param>
        /// <param name="writeToCloudFrequency">
        /// If set, the contents of the object will be periodically written to the 
        /// backing blob.  A smaller value results in better data resiliency but may
        /// result in higher Azure Storage costs and slightly degraded client-side 
        /// performance.  Network requests to Azure will only be made when the local
        /// state has actually changed (regardless of the frequency specified here).
        /// Set this to null if you only require read-only visibility into an object 
        /// being maintained by another application.
        /// </param>
        /// <param name="readFromCloudFrequency">
        /// If set, the contents of the object will be periodically refreshed based on 
        /// the backing blob.  A smaller value results in better data freshness but 
        /// higher Azure Storage costs and higher chance of loss of local changes made 
        /// to this instance of the object.  Requests will always be made according to
        /// this frequency but network utilization will be low in the cases where the
        /// data in the backing blob has not been updated since the last retrieval.
        /// </param>
        public CloudBlobBacked(
            ICloudBlob backingBlob,
            TimeSpan? leaseDuration = null,
            TimeSpan? writeToCloudFrequency = null,
            TimeSpan? readFromCloudFrequency = null)
        {
            if (leaseDuration.HasValue && (leaseDuration < TimeSpan.FromSeconds(MinimumLeaseInSeconds)))
            {
                throw new ArgumentException("Lease duration too short", "leaseDuration");
            }

            this.backingBlob = backingBlob;

            if (!TryRefreshDataFromCloudBlob())
            {
                WriteLocalDataToCloudIfNeeded();
            }

            if (leaseDuration.HasValue)
            {
                TryAquireLeaseAndRefresh(leaseDuration.Value);
                TryRefreshDataFromCloudBlob();
            }

            if (writeToCloudFrequency.HasValue)
            {
                StartBlobWriter(writeToCloudFrequency.Value);
            }

            if (readFromCloudFrequency.HasValue)
            {
                StartBlobReader(readFromCloudFrequency.Value);
            }
        }

        /// <summary>
        /// A reference to a current snapshot of the object.  Any changes to this object may later
        /// be written back to blob storage.
        /// 
        /// Do not create a copy of this reference in your code, use it directly (to avoid inadvertent 
        /// use of stale data).
        /// 
        /// If you plan on placing the object in an interstitial state not suitable for replicating to
        /// the backing blob (e.g. calling an instance method that modifies internal state, or making
        /// a sequence of dependent field/property changes) you should first take a lock on SyncRoot to
        /// prevent a synchronization during your modification.
        /// </summary>
        public T Object
        {
            get
            {
                T temp = null;
                Interlocked.Exchange(ref temp, this.localObject);
                return temp;
            }

            set
            {
                Interlocked.Exchange(ref this.localObject, value);
            }
        }

        /// <summary>
        /// Take a lock on this to temporarily prevent the object from being refreshed from the backing 
        /// blob, or being replicated into the backing blob.
        /// </summary>
        public object SyncRoot
        {
            get
            {
                return this.syncRoot;
            }
        }

        /// <summary>
        /// URL of this object in blob storage
        /// </summary>
        public StorageUri BackingBlobUri
        {
            get
            {
                return this.backingBlob.StorageUri;
            }
        }

        /// <summary>
        /// Triggers whenever local state is modified based on changes to the backing blob.
        /// </summary>
        public event EventHandler OnUpdate;

        /// <summary>
        /// Aborts the various worker threads.  The writer thread is allowed to fully terminate before the
        /// lease renewer thread is aborted (to allow a final write to succeed before giving up the lease).
        /// </summary>
        ~CloudBlobBacked()
        {
            Shutdown();
        }

        /// <summary>
        /// Aborts the various worker threads.  The writer thread is allowed to fully terminate before the
        /// lease renewer thread is aborted (to allow a final write to succeed before giving up the lease).
        /// The object can still be used after calling Shutdown, but it will no longer be periodically
        /// synchonrized with the data in the cloud and the lease (if taken) will be given up.
        /// </summary>
        public void Shutdown()
        {
            if (this.blobReader != null)
            {
                Thread t = this.blobReader;
                this.blobReader = null;
                this.stopBlobReader.Set();
                t.Join();
            }

            if (this.blobWriter != null)
            {
                Thread t = this.blobWriter;
                this.blobWriter = null;
                this.stopBlobWriter.Set();
                t.Join();
            }

            if (this.leaseRenewer != null)
            {
                Thread t = this.leaseRenewer;
                this.leaseRenewer = null;
                this.stopLeaseRenewer.Set();
                t.Join();
            }
        }

        /// <summary>
        /// Takes out a lease on a blob and starts a thread to keep it renewed.  Throws if the lease
        /// cannot be obtained.
        /// </summary>
        private void TryAquireLeaseAndRefresh(TimeSpan leaseDuration)
        {
            AcquireNewLease(leaseDuration);
            StartLeaseRenewer(leaseDuration);
        }

        private void AcquireNewLease(TimeSpan leaseDuration)
        {
            StorageOperation.Try(
                () =>
                {
                    this.writeAccessCondition.LeaseId = this.backingBlob.AcquireLease(leaseDuration, proposedLeaseId: null);
                },
                catchHttp409: e => 
                {
                    throw new InvalidOperationException("The lease for this blob has been taken by another client", e);
                });
        }

        /// <summary>
        /// Starts a thread that triggers a request to renew the lease on the backing blob MinimumLeaseInSeconds
        /// before it expires, and releases the lease whent he thread is aborted.
        /// </summary>
        private void StartLeaseRenewer(TimeSpan leaseDuration)
        {
            this.leaseRenewer = (new Thread(() =>
            {
                bool stop = false;
                while (!stop)
                {
                    // Renew lease MinimumLeaseInSeconds before it expires:
                    stop = stopLeaseRenewer.WaitOne(TimeSpan.FromSeconds(LeaseRenewalIntervalInSeconds));

                    lock (this.writeAccessCondition)
                    {
                        StorageOperation.Try(
                            () => 
                            {
                                this.backingBlob.RenewLease(this.writeAccessCondition);
                            },
                            catchHttp409: e =>
                            {
                                // Lost our original lease (maybe due to this thread sleeping for an extremely long time)
                                AcquireNewLease(leaseDuration);
                            });
                    }
                }

                StorageOperation.Try(
                    () =>
                    {
                        this.backingBlob.ReleaseLease(this.writeAccessCondition);
                    },
                    catchHttp409: e =>
                    {
                        // Maybe we didn't have the lease anyway? Ooh well, we're shutting down anyway (absorb this error)
                    });
            }));
            this.leaseRenewer.Start();
        }

        /// <summary>
        /// Start a thread that writes the local state to the backing blob (if needed) at the given frequency,
        /// and writes the local state one final time when the thread is aborted.
        /// </summary>
        private void StartBlobWriter(TimeSpan writeToCloudFrequency)
        {
            this.blobWriter = new Thread(() =>
            {
                bool stop = false;
                while (!stop)
                {
                    stopBlobWriter.WaitOne(writeToCloudFrequency);
                    WriteLocalDataToCloudIfNeeded();
                    stop = stopBlobWriter.WaitOne(0);
                }
            });
            this.blobWriter.Start();
        }

        /// <summary>
        /// Starts a thread that updates the local state from the backing blob at the given
        /// frequency.
        /// </summary>
        private void StartBlobReader(TimeSpan readFromCloudFrequency)
        {
            this.blobReader = new Thread(() =>
            {
                bool stop = false;
                while (!stop)
                {
                    TryRefreshDataFromCloudBlob();
                    stop = stopBlobReader.WaitOne(readFromCloudFrequency);
                }
            });
            this.blobReader.Start();
        }

        private bool TryRefreshDataFromCloudBlob()
        {
            bool exists = true;

            StorageOperation.Try(
                () =>
                {
                    T temp = default(T);
                    MemoryStream currentBlobContents = new MemoryStream();
                    OperationContext context = new OperationContext();

                    lock (this.syncRoot)
                    {
                        StorageOperation.Try(
                            () =>
                            {
                                this.backingBlob.DownloadToStream(currentBlobContents, accessCondition: this.readAccessCondition, operationContext: context);
                                this.readAccessCondition.IfNoneMatchETag = context.LastResult.Etag;
                                exists = Serialization.DeserializeInto<T>(ref temp, currentBlobContents);
                            },
                            catchHttp404: e =>
                            {
                                exists = false;
                            });

                        this.localObject = temp;

                        if (exists)
                        {
                            this.lastKnownBlobContentsHash = Hash(currentBlobContents.ToArray());

                            if (this.OnUpdate != null)
                            {
                                this.OnUpdate(this, EventArgs.Empty);
                            }
                        }
                    }
                },
                catchHttp304: e => { },
                catchHttp412: e => { });

            return exists;
        }

        private void WriteLocalDataToCloudIfNeeded()
        {
            lock (this.syncRoot)
            {
                byte[] buffer = Serialization.Serialize(this.localObject);

                if (ArrayEquals(Hash(buffer), this.lastKnownBlobContentsHash))
                {
                    return;
                }

                OperationContext context = new OperationContext();                
                this.backingBlob.UploadFromByteArray(
                    buffer,
                    0,
                    buffer.Length,
                    accessCondition: writeAccessCondition,
                    operationContext: context);
                this.readAccessCondition.IfNoneMatchETag = context.LastResult.Etag;
                this.lastKnownBlobContentsHash = Hash(buffer);
            }
        }

        private byte[] Hash(byte[] buffer)
        {
            using (SHA256 hasher = SHA256Managed.Create())
            {
                return hasher.TransformFinalBlock(buffer, 0, buffer.Length);
            }
        }

        private static bool ArrayEquals(byte[] xs, byte[] ys)
        {
            if (xs == null)
            {
                return ys == null;
            }

            if (ys == null)
            {
                return xs == null;
            }

            if (xs.Length != ys.Length)
            {
                return false;
            }

            for (int i = 0; i < xs.Length; i++)
            {
                if (xs[i] != ys[i])
                {
                    return false;
                }
            }

            return true;
        }

        private Thread leaseRenewer;
        private Thread blobReader;
        private Thread blobWriter;

        private ManualResetEvent stopLeaseRenewer = new ManualResetEvent(false);
        private ManualResetEvent stopBlobReader = new ManualResetEvent(false);
        private ManualResetEvent stopBlobWriter = new ManualResetEvent(false);

        private AccessCondition writeAccessCondition = new AccessCondition();
        private AccessCondition readAccessCondition = new AccessCondition();

        private ICloudBlob backingBlob;
        private T localObject;
        private byte[] lastKnownBlobContentsHash;
        private Object syncRoot = new Object();
    }
}