using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

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
    public class CloudBlobBacked<T> : IDisposable where T : class
    {
        /// <summary>
        /// The minimum lease duration that may be passed to the constructor.
        /// </summary>
        public const int MinimumLeaseInSeconds = 20;

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
            CloudBlockBlob backingBlob,
            TimeSpan? leaseDuration = null,
            TimeSpan? writeToCloudFrequency = null,
            TimeSpan? readFromCloudFrequency = null)
        {
            this.backingBlob = backingBlob;

            bool leaseAcquired = false;
            if (leaseDuration.HasValue)
            {
                if (leaseDuration < TimeSpan.FromSeconds(MinimumLeaseInSeconds))
                {
                    throw new ArgumentException("Lease duration too short", "leaseDuration");
                }

                leaseAcquired = AcquireLeaseIfBlobExists(leaseDuration.Value);
            }

            bool exists = TryRefreshDataFromCloudBlob();

            if (!exists)
            {
                WriteLocalDataToCloudIfNeeded();
            }

            if (leaseDuration.HasValue)
            {
                if (!leaseAcquired)
                {
                    AcquireLeaseForExistingBlob(leaseDuration.Value);
                }

                this.leaseRenewer = StartLeaseRenewer(leaseDuration.Value);
            }

            if (writeToCloudFrequency.HasValue)
            {
                this.blobWriter = StartBlobWriter(writeToCloudFrequency.Value);
            }

            if (readFromCloudFrequency.HasValue)
            {
                this.blobReader = StartBlobReader(readFromCloudFrequency.Value);
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
                ThrowIfInErrorState();

                T temp = null;
                Interlocked.Exchange(ref temp, this.localObject);
                return temp;
            }

            set
            {
                ThrowIfInErrorState();

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
                ThrowIfInErrorState();

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
                ThrowIfInErrorState();

                return this.backingBlob.StorageUri;
            }
        }

        /// <summary>
        /// Triggers whenever local state is modified based on changes to the backing blob.
        /// </summary>
        public event EventHandler OnUpdate;

        /// <summary>
        /// Aborts the various worker tasks.  The writer task is allowed to fully terminate before the
        /// lease renewer task is aborted (to allow a final write to succeed before giving up the lease).
        /// The object can still be used after calling Shutdown, but it will no longer be periodically
        /// synchonrized with the data in the cloud and the lease (if taken) will be given up.
        /// </summary>
        public void Shutdown()
        {
            TryAwaitStop(taskToAwait: ref this.blobReader, stopSignal: this.stopBlobReader);

            TryAwaitStop(taskToAwait: ref this.blobWriter, stopSignal: this.stopBlobWriter);

            TryAwaitStop(taskToAwait: ref this.leaseRenewer, stopSignal: this.stopLeaseRenewer);
        }

        /// <summary>
        /// Calls <see cref="Shutdown"/>.
        /// </summary>
        public void Dispose()
        {
            Shutdown();
        }

        private static void TryAwaitStop(ref Task taskToAwait, ManualResetEventSlim stopSignal)
        {
            stopSignal.Set();

            if (taskToAwait != null)
            {
                taskToAwait.Wait();

                if (taskToAwait.Exception != null)
                {
                    throw taskToAwait.Exception;
                }

                taskToAwait = null;
            }
        }

        private bool AcquireLeaseIfBlobExists(TimeSpan leaseDuration)
        {
            bool exists = true;
            StorageOperation.Try(
                () => { AcquireLeaseForExistingBlob(leaseDuration); },
                catchHttp404: e => { exists = false; }
                );
            return exists;
        }

        private void AcquireLeaseForExistingBlob(TimeSpan leaseDuration)
        {
            StorageOperation.Try(
                () =>
                {
                    lock (this.writeAccessCondition)
                    {
                        this.writeAccessCondition.LeaseId = this.backingBlob.AcquireLease(leaseDuration, proposedLeaseId: null);
                    }
                },
                catchHttp409: e => 
                {
                    throw new InvalidOperationException("The lease for this blob has been taken by another client", e);
                });
        }

        /// <summary>
        /// Starts a task that triggers a request to renew the lease on the backing blob MinimumLeaseInSeconds
        /// before it expires, and releases the lease when the task is aborted.
        /// </summary>
        private Task StartLeaseRenewer(TimeSpan leaseDuration)
        {
            return Task.Run(
                () =>
                {
                    Stopwatch timer = Stopwatch.StartNew();
                    bool stop = false;
                    while (!stop)
                    {
                        stop = stopLeaseRenewer.Wait(TimeSpan.FromTicks(timer.ElapsedTicks % (leaseDuration.Ticks / 2)));

                        if (!stop)
                        {
                            StorageOperation.Try(
                                () =>
                                {
                                    lock (this.writeAccessCondition)
                                    {
                                        this.backingBlob.RenewLease(this.writeAccessCondition);
                                    }
                                },
                                catchHttp409: e =>
                                {
                                    // Lost our original lease (maybe due to this task sleeping for an extremely long time)
                                    AcquireLeaseForExistingBlob(leaseDuration);
                                });
                        }
                    }

                    StorageOperation.Try(
                        () =>
                        {
                            lock (this.writeAccessCondition)
                            {
                                this.backingBlob.ReleaseLease(this.writeAccessCondition);
                            }
                        },
                        catchHttp409: e =>
                        {
                            // Maybe we didn't have the lease anyway? Ooh well, we're shutting down anyway (absorb this error)
                        });
                });
        }

        /// <summary>
        /// Start a task that writes the local state to the backing blob (if needed) at the given frequency,
        /// and writes the local state one final time when the task is aborted.
        /// </summary>
        private Task StartBlobWriter(TimeSpan writeToCloudFrequency)
        {
            return Task.Run(
                () =>
                {
                    Stopwatch timer = Stopwatch.StartNew();
                    bool stop = false;
                    while (!stop)
                    {
                        stop = stopBlobWriter.Wait(TimeSpan.FromTicks(timer.ElapsedTicks % writeToCloudFrequency.Ticks));
                        WriteLocalDataToCloudIfNeeded();
                    }
                });
        }

        /// <summary>
        /// Starts a task that updates the local state from the backing blob at the given
        /// frequency.
        /// </summary>
        private Task StartBlobReader(TimeSpan readFromCloudFrequency)
        {
            return Task.Run(
                () =>
                {
                    Stopwatch timer = Stopwatch.StartNew();
                    bool stop = false;
                    while (!stop)
                    {
                        TryRefreshDataFromCloudBlob();
                        stop = stopBlobReader.Wait(TimeSpan.FromTicks(timer.ElapsedTicks % readFromCloudFrequency.Ticks));
                    }
                });
        }

        private bool TryRefreshDataFromCloudBlob()
        {
            bool exists = true;

            StorageOperation.Try(
                () =>
                {
                    T temp = default(T);
                    OperationContext context = new OperationContext();

                    lock (this.syncRoot)
                    {
                        StorageOperation.Try(
                            () =>
                            {
                                Stream blobContentsStream = this.backingBlob.OpenRead(
                                    accessCondition: this.readAccessCondition, 
                                    operationContext: context);

                                exists = Serialization.DeserializeInto<T>(
                                    ref temp, 
                                    blobContentsStream, 
                                    out this.lastKnownBlobContentsHash);

                                this.readAccessCondition.IfNoneMatchETag = context.LastResult.Etag;
                            },
                            catchHttp404: e =>
                            {
                                exists = false;
                            });

                        this.localObject = temp;

                        if (exists)
                        {
                            if (this.OnUpdate != null)
                            {
                                this.OnUpdate(this, EventArgs.Empty);
                            }
                        }
                    }
                },
                catchHttp304: e => { }, // TODO: Perf improvement; prevent throwing of exceptions in expected-case paths?
                catchHttp412: e => { });

            return exists;
        }

        private void WriteLocalDataToCloudIfNeeded()
        {
            lock (this.syncRoot)
            {
                byte[] localContentHash;

                if (!Serialization.ModifiedSince(
                    this.localObject, 
                    this.lastKnownBlobContentsHash, 
                    out localContentHash))
                { 
                    return;
                }

                AccessCondition writeAccessConditionAtStartOfUpload = new AccessCondition();
                lock (this.writeAccessCondition)
                {
                    writeAccessConditionAtStartOfUpload.LeaseId = this.writeAccessCondition.LeaseId;
                }

                StorageOperation.Try(
                    () =>
                    {
                        OperationContext context = new OperationContext();

                        CloudBlobStream uploadStream = this.backingBlob.OpenWrite(
                            accessCondition: writeAccessConditionAtStartOfUpload,
                            operationContext: context);
                        Serialization.SerializeIntoStream(this.localObject, uploadStream);
                        uploadStream.Close();

                        this.readAccessCondition.IfNoneMatchETag = context.LastResult.Etag;
                        this.lastKnownBlobContentsHash = localContentHash;
                    },
                    catchHttp400: e =>
                    {
                        // Can happen when two clients race to perform an upload to the same blob; one
                        // client will get a HTTP 400 when committing their list of uploaded blocks.
                        OnWriteToCloudFailure(e, writeAccessConditionAtStartOfUpload);
                    },
                    catchHttp412: e =>
                    {
                        // There is a lease in place that prevents this write.
                        OnWriteToCloudFailure(e, writeAccessConditionAtStartOfUpload);
                    });
            }
        }

        private void OnWriteToCloudFailure(StorageException e, AccessCondition writeAccessConditionAtStartOfUpload)
        {
            if (!string.IsNullOrEmpty(writeAccessConditionAtStartOfUpload.LeaseId))
            {
                // Someone else has the lease, but we are supposed to have it. That is bad.
                throw new TimeoutException("Lease was lost and updates cannot be saved", e);
            }

            // User optimistically tried to update an object that they opted not to take a lease on.
            // There update has not been persisted.  Ensure that:
            // A. The local data is shortly rolled back to reflect the state of the object in the cloud 
            //    (if the user opted to poll for updates).
            // B. This write is retried (if still relevent) next time the writer task wakes up (whoever
            //    has the lease may have gone away by then).
            this.readAccessCondition.IfNoneMatchETag = null; // [A]
            this.lastKnownBlobContentsHash = null; // [B]
        }

        private void ThrowIfInErrorState()
        {
            ThrowIfFaulted(this.leaseRenewer);
            ThrowIfFaulted(this.blobWriter);
            ThrowIfFaulted(this.blobReader);
        }

        private void ThrowIfFaulted(Task task)
        {
            if (task == null)
            {
                return;
            }

            if (!task.IsFaulted)
            {
                return;
            }

            lock (task)
            {
                Exception e = task.Exception;
                if (e != null)
                {
                    throw e;
                }
            }
        }

        private Task leaseRenewer;
        private Task blobReader;
        private Task blobWriter;

        private ManualResetEventSlim stopLeaseRenewer = new ManualResetEventSlim(false);
        private ManualResetEventSlim stopBlobReader = new ManualResetEventSlim(false);
        private ManualResetEventSlim stopBlobWriter = new ManualResetEventSlim(false);

        private AccessCondition writeAccessCondition = new AccessCondition();
        private AccessCondition readAccessCondition = new AccessCondition();

        private CloudBlockBlob backingBlob;
        private T localObject;
        private byte[] lastKnownBlobContentsHash;
        private Object syncRoot = new Object();
        private Queue<Exception> backgroundExceptions = new Queue<Exception>();
    }
}