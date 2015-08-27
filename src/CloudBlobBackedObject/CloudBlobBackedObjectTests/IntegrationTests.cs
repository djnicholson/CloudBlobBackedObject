﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Win32;

namespace CloudBlobBackedObjectTests
{
    [TestClass]
    public class IntegrationTests
    {
        [TestInitialize]
        public void TestSetup()
        {
            string storageKey = Registry.GetValue(
                @"HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject",
                "StorageKey",
                null) as string;

            // If this fails, run the following command:
            //   reg add HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject /v StorageKey /t REG_SZ /f /d foo
            // (where foo is a Azure Storage access key)
            if (storageKey == null)
            {
                Assert.Fail(
                    @"HKEY_CURRENT_USER\SOFTWARE\djnicholson\CloudBlobBackedObject\StorageKey not found");
            }
        }

        [TestMethod]
        public void HelloWorld()
        {
            Assert.Inconclusive("No tests yet!");
        }
    }
}
