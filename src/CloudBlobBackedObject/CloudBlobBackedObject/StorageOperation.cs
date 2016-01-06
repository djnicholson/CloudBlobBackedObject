using System;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;

namespace CloudBlobBackedObject
{
    internal static class StorageOperation
    {
        internal static void Try(
            Action operation,
            Action<StorageException> catchHttp304 = null,
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

        internal static async Task<T> TryAsync<T>(
            Task<T> operation,
            Func<StorageException, T> handleHttp304 = null,
            Func<StorageException, T> handleHttp404 = null,
            Func<StorageException, T> handleHttp409 = null,
            Func<StorageException, T> handleHttp412 = null)
        {
            try
            {
                await operation;
            }
            catch (StorageException e)
            {
                int httpStatus = HttpStatusCode(e);
                if (handleHttp304 != null && httpStatus == 304)
                {
                    return handleHttp304(e);
                }
                else if (handleHttp404 != null && httpStatus == 404)
                {
                    return handleHttp404(e);
                }
                else if (handleHttp409 != null && httpStatus == 409)
                {
                    return handleHttp409(e);
                }
                else if (handleHttp412 != null && httpStatus == 412)
                {
                    return handleHttp412(e);
                }
                else
                {
                    throw;
                }
            }

            return operation.Result;
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
