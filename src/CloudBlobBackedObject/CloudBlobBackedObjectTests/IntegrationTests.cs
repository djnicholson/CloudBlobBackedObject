using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Win32;
using Microsoft.WindowsAzure.Storage.Blob;

namespace CloudBlobBackedObjectTests
{
    [TestClass]
    public class IntegrationTests
    {
        [TestInitialize]
        public void TestSetup()
        {
            string storageAccountAndKey = Registry.GetValue(
                @"HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject",
                "StorageKey",
                null) as string;

            // If this fails, run the following command:
            //   reg add HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject /v StorageKey /t REG_SZ /f /d "<storageAccountName> <storage key>"
            // (where foo is an Azure Storage access key)
            if (storageAccountAndKey == null)
            {
                Assert.Fail(
                    @"HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject\StorageKey not found");
            }

            
            
        }

        [TestCleanup]
        public void TestCleanup()
        {

        }

        [TestMethod]
        public void NewObject()
        {
            Assert.Inconclusive("No tests yet!");

            ICloudBlob blob;
        }
    }
}
