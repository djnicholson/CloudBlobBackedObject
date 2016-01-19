using Microsoft.WindowsAzure.Storage;

using System;

namespace CloudBlobBackedObject
{
    internal static class StorageOperation
    {
        public static void Try(
            Action operation,
            Action<StorageException> catchHttp304 = null,
            Action<StorageException> catchHttp400 = null,
            Action<StorageException> catchHttp404 = null,
            Action<StorageException> catchHttp409 = null,
            Action<StorageException> catchHttp412 = null)
        {
            try
            {
                operation();
            }
            catch (StorageException e)
            {
                int httpStatus = HttpStatusCode(e);
                if (catchHttp304 != null && httpStatus == 304)
                {
                    catchHttp304(e);
                }
                else if (catchHttp400 != null && httpStatus == 400)
                {
                    catchHttp400(e);
                }
                else if (catchHttp404 != null && httpStatus == 404)
                {
                    catchHttp404(e);
                }
                else if (catchHttp409 != null && httpStatus == 409)
                {
                    catchHttp409(e);
                }
                else if (catchHttp412 != null && httpStatus == 412)
                {
                    catchHttp412(e);
                }
                else
                {
                    throw;
                }
            }
        }

        private static int HttpStatusCode(StorageException e)
        {
            if (e == null || e.RequestInformation == null)
            {
                return 0;
            }
            return e.RequestInformation.HttpStatusCode;
        }

    }
}
