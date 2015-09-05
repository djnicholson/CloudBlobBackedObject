using CloudBlobBackedObject;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Win32;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Diagnostics;
using System.Threading;

namespace CloudBlobBackedObjectTests
{
    [TestClass]
    public class IntegrationTests
    {
        [TestInitialize]
        public void TestSetup()
        {
            string RegistryKeyLocation = @"HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject";
            string RegistryKeyName = @"StorageKey";
            string RegistryKey = RegistryKeyLocation + "\\" + RegistryKeyName;

            string storageAccountAndKey = Registry.GetValue(RegistryKeyLocation, RegistryKeyName, null) as string;

            // If this fails, run the following command:
            //   reg add HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject /v StorageKey /t REG_SZ /f /d "<storageAccountName> <storage key>"
            // (where foo is an Azure Storage access key)
            Assert.IsNotNull(storageAccountAndKey, RegistryKey + " not found");

            string[] parts = storageAccountAndKey.Split(' ');

            Assert.AreEqual(2, parts.Length, RegistryKey + " malformed");

            this.blobClient = new CloudBlobClient(
                new Uri("https://" + parts[0] + ".blob.core.windows.net"),
                new StorageCredentials(parts[0], parts[1]));

            this.blobPrefix = Guid.NewGuid() + "_";

            CloudBlobContainer root = this.blobClient.GetRootContainerReference();
            root.CreateIfNotExists();         
        }

        private ICloudBlob NewBlob()
        {
            CloudBlobContainer root = this.blobClient.GetRootContainerReference();
            return root.GetBlockBlobReference(this.blobPrefix + Guid.NewGuid());
        }

        [TestCleanup]
        public void TestCleanup()
        {
            bool success = false;
            while (!success)
            {
                success = true;
                foreach (IListBlobItem blob in this.blobClient.GetRootContainerReference().ListBlobs())
                {
                    if (blob.Uri.ToString().Contains(this.blobPrefix))
                    {
                        try
                        {
                            this.blobClient.GetBlobReferenceFromServer(blob.StorageUri).DeleteIfExists();
                        }
                        catch (StorageException)
                        {
                            success = false;
                        }
                    }
                }

                if (!success)
                {
                    GC.Collect();
                    Thread.Sleep(TimeSpan.FromSeconds(1.0));
                }
            }
        }

        private CloudBlobClient blobClient;

        private string blobPrefix;

        private void AllVariations<T>(Action<CloudBlobBacked<T>> test) where T : class
        {
            Trace.TraceInformation("readOnce");
            var readOnce =
                new CloudBlobBacked<T>(NewBlob());
            test(readOnce);
            readOnce.Shutdown();

            Trace.TraceInformation("readOnceUnderLease");
            var readOnceUnderLease =
                new CloudBlobBacked<T>(NewBlob(), leaseDuration: TimeSpan.FromMinutes(1.0));
            test(readOnceUnderLease);
            readOnceUnderLease.Shutdown();

            Trace.TraceInformation("readContinually");
            var readContinually =
                new CloudBlobBacked<T>(NewBlob(), readFromCloudFrequency: TimeSpan.FromSeconds(1.0));
            test(readContinually);
            readContinually.Shutdown();

            Trace.TraceInformation("readContinuallyUnderLease");
            var readContinuallyUnderLease = new CloudBlobBacked<T>(
                NewBlob(), 
                readFromCloudFrequency: TimeSpan.FromSeconds(1.0),
                leaseDuration: TimeSpan.FromMinutes(1.0));
            test(readContinuallyUnderLease);
            readContinuallyUnderLease.Shutdown();

            Trace.TraceInformation("writeContinually");
            var writeContinually =
                new CloudBlobBacked<T>(NewBlob(), writeToCloudFrequency: TimeSpan.FromSeconds(1.0));
            test(writeContinually);
            writeContinually.Shutdown();

            Trace.TraceInformation("writeContinuallyUnderLease");
            var writeContinuallyUnderLease = new CloudBlobBacked<T>(
                NewBlob(), 
                writeToCloudFrequency: TimeSpan.FromSeconds(1.0),
                leaseDuration: TimeSpan.FromMinutes(1.0));
            test(writeContinuallyUnderLease);
            writeContinuallyUnderLease.Shutdown();

            Trace.TraceInformation("readWriteContinually");
            var readWriteContinually = new CloudBlobBacked<T>(
                NewBlob(), 
                readFromCloudFrequency: TimeSpan.FromSeconds(1.0),
                writeToCloudFrequency: TimeSpan.FromSeconds(1.0));
            test(readWriteContinually);
            readWriteContinually.Shutdown();

            Trace.TraceInformation("readWriteContinuallyUnderLease");
            var readWriteContinuallyUnderLease = new CloudBlobBacked<T>(
                NewBlob(),
                readFromCloudFrequency: TimeSpan.FromSeconds(1.0),
                writeToCloudFrequency: TimeSpan.FromSeconds(1.0),
                leaseDuration: TimeSpan.FromMinutes(1.0));
            test(readWriteContinuallyUnderLease);
            readWriteContinuallyUnderLease.Shutdown();
        }

        [TestMethod]
        public void NonExistingObject()
        {
            AllVariations<string>(target => 
            {
                Assert.IsNull(target.Object);
                target.Object = "Hello world!";
                Assert.AreEqual("Hello world!", target.Object);
                Thread.Sleep(TimeSpan.FromSeconds(3.0));
                Assert.AreEqual("Hello world!", target.Object);
            });
        }

        [TestMethod]
        public void Lease()
        {
            var blob = NewBlob();

            // Take a lease out on a new blob
            var exclusiveWriter = new CloudBlobBacked<string>(
                blob,
                writeToCloudFrequency: TimeSpan.FromSeconds(0.5), 
                leaseDuration: TimeSpan.FromMinutes(1.0));
            exclusiveWriter.Object = "Hello";

            // Verify that no one else can take the lease
            try
            {
                new CloudBlobBacked<string>(blob, leaseDuration: TimeSpan.FromMinutes(0.5));
            }
            catch(InvalidOperationException)
            {
            }

            // A reader is ok though
            Thread.Sleep(TimeSpan.FromSeconds(2.0));
            var reader = new CloudBlobBacked<string>(blob, readFromCloudFrequency: TimeSpan.FromSeconds(0.5));
            Assert.AreEqual("Hello", reader.Object);

            // Someone else can try and update it, but updates will not be made while the lease is active...
            var otherWriter = new CloudBlobBacked<string>(blob, writeToCloudFrequency: TimeSpan.FromSeconds(0.5));
            otherWriter.Object = "I changed";
            Thread.Sleep(TimeSpan.FromSeconds(2.0));
            Assert.AreNotEqual("I changed", reader.Object);
            Assert.AreEqual("I changed", otherWriter.Object);

            // The write will eventually be made after the lease is given up though
            exclusiveWriter.Shutdown();
            Thread.Sleep(TimeSpan.FromSeconds(2.0));
            Assert.AreEqual("I changed", reader.Object);
            Assert.AreEqual("I changed", otherWriter.Object);

            reader.Shutdown();
            otherWriter.Shutdown();
        }

        public void ReaderWriterImpl(TimeSpan? lease = null)
        {
            var blob = NewBlob();

            var writer = new CloudBlobBacked<string>(
                blob,
                leaseDuration: lease,
                writeToCloudFrequency: TimeSpan.FromSeconds(1.0));

            var reader = new CloudBlobBacked<string>(
                blob,
                readFromCloudFrequency: TimeSpan.FromSeconds(1.0));

            writer.Object = "Hello world!";

            while (!writer.Object.Equals(reader.Object))
            {
                Trace.TraceInformation("waiting...");
                Thread.Sleep(TimeSpan.FromSeconds(0.25));
            }

            reader.Shutdown();
            writer.Shutdown();
        }

        [TestMethod]
        public void ReaderWriterWithoutLease()
        {
            ReaderWriterImpl();
        }

        [TestMethod]
        public void ReaderWriterWithLease()
        {
            ReaderWriterImpl(lease: TimeSpan.FromSeconds(30.0));
        }

        [TestMethod]
        public void MultipleWriters()
        {
            var blob = NewBlob();

            // Two instances fighting over the value of an object and a witness
            // that ensures they each win at least once.

            var witness = new CloudBlobBacked<string>(
                blob,
                readFromCloudFrequency: TimeSpan.FromSeconds(0.1));

            var writer1 = new CloudBlobBacked<string>(
                blob,
                readFromCloudFrequency: TimeSpan.FromSeconds(0.1),
                writeToCloudFrequency: TimeSpan.FromSeconds(0.3));

            var writer2 = new CloudBlobBacked<string>(
                blob,
                readFromCloudFrequency: TimeSpan.FromSeconds(0.1),
                writeToCloudFrequency: TimeSpan.FromSeconds(0.7));

            bool writer1Success = false;
            bool writer2Success = false;

            witness.OnUpdate += (s, e) =>
            {
                writer1Success |= "1".Equals(witness.Object);
                writer2Success |= "2".Equals(witness.Object);
            };
                
            while (!writer1Success || !writer2Success)
            {
                writer1.Object = "1";
                writer2.Object = "2";
                Thread.Yield();
            }

            witness.Shutdown();
            writer1.Shutdown();
            writer2.Shutdown();
        }

        [TestMethod]
        public void ReadOnlySnapshot()
        {
            var blob = NewBlob();

            var writer = new CloudBlobBacked<string>(
                blob,
                writeToCloudFrequency: TimeSpan.FromSeconds(0.5));
            writer.Object = "hello";
            Thread.Sleep(TimeSpan.FromSeconds(1.0));

            var snapshot = new CloudBlobBacked<string>(blob);
            Assert.AreEqual("hello", snapshot.Object);

            writer.Object = "changed";
            Thread.Sleep(TimeSpan.FromSeconds(1.0));

            Assert.AreEqual("hello", snapshot.Object);
            
            writer.Shutdown();
        }

        [TestMethod]
        public void LockBlocksReads()
        {
            var blob = NewBlob();

            var writer = new CloudBlobBacked<string>(
                blob,
                writeToCloudFrequency: TimeSpan.FromSeconds(0.1));
            writer.Object = "hello";
            Thread.Sleep(TimeSpan.FromSeconds(1.0));

            var reader1 = new CloudBlobBacked<string>(
                blob,
                readFromCloudFrequency: TimeSpan.FromSeconds(0.1));

            var reader2 = new CloudBlobBacked<string>(
                blob,
                readFromCloudFrequency: TimeSpan.FromSeconds(0.1));

            lock (reader1.SyncRoot)
            {
                Assert.AreEqual("hello", reader1.Object);
                Assert.AreEqual("hello", reader2.Object);

                writer.Object = "new";
                Thread.Sleep(TimeSpan.FromSeconds(1.0));

                Assert.AreEqual("hello", reader1.Object);
                Assert.AreEqual("new", reader2.Object);
            }

            Thread.Sleep(TimeSpan.FromSeconds(1.0));

            Assert.AreEqual("new", reader1.Object);
            Assert.AreEqual("new", reader2.Object);

            reader1.Shutdown();
            reader2.Shutdown();
            writer.Shutdown();
        }

        [TestMethod]
        public void LockBlocksWrites()
        {
            var blob = NewBlob();

            var writer1 = new CloudBlobBacked<string>(
                blob,
                writeToCloudFrequency: TimeSpan.FromSeconds(0.1));

            var writer2 = new CloudBlobBacked<string>(
                blob,
                writeToCloudFrequency: TimeSpan.FromSeconds(0.1));

            writer1.Object = "hello";
            Thread.Sleep(TimeSpan.FromSeconds(1.0));

            var reader = new CloudBlobBacked<string>(
                blob,
                readFromCloudFrequency: TimeSpan.FromSeconds(0.1));
            Assert.AreEqual("hello", reader.Object);

            lock (writer1.SyncRoot)
            {
                Assert.AreEqual("hello", reader.Object);

                writer1.Object = "1";
                writer2.Object = "2";
                Thread.Sleep(TimeSpan.FromSeconds(1.0));

                Assert.AreEqual("2", reader.Object);

                writer2.Shutdown();
            }

            Thread.Sleep(TimeSpan.FromSeconds(1.0));

            Assert.AreEqual("1", reader.Object);
            
            reader.Shutdown();
            writer1.Shutdown();
            writer2.Shutdown();
        }
    }
}
