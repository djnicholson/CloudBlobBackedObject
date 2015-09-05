using CloudBlobBackedObject;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Win32;
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
            foreach (IListBlobItem blob in this.blobClient.GetRootContainerReference().ListBlobs())
            {
                if (blob.Uri.ToString().Contains(this.blobPrefix))
                {
                    this.blobClient.GetBlobReferenceFromServer(blob.StorageUri).DeleteIfExists();
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
                Thread.Sleep(TimeSpan.FromSeconds(5.0));
                Assert.AreEqual("Hello world!", target.Object);
            });
        }
    }
}
