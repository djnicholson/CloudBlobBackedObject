using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;

namespace CloudBlobBackedObject
{
    internal static class Serialization
    {
        public static void SerializeIntoStream(Object obj, Stream outputStream)
        {
            if (obj == null)
            {
                // A null object is represented by serializing zero bytes
            }
            else
            {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(outputStream, obj);
            }
        }

        public static bool DeserializeInto<T>(ref T target, Stream serializedObjectReader)
        {
            if (serializedObjectReader.Length != 0)
            {
                serializedObjectReader.Seek(0, SeekOrigin.Begin);
                BinaryFormatter formatter = new BinaryFormatter();
                try
                {
                    target = (T)formatter.Deserialize(serializedObjectReader);
                }
                catch (SerializationException)
                {
                    return false;
                }
            }

            return true;
        }

        public static byte[] Hash(byte[] buffer)
        {
            using (SHA256 hasher = SHA256Managed.Create())
            {
                return hasher.TransformFinalBlock(buffer, 0, buffer.Length);
            }
        }

        public static bool ModifiedSince(byte[] currentBuffer, byte[] lastKnownHash)
        {
            byte[] currentHash = Hash(currentBuffer);

            if (currentHash.Length != lastKnownHash.Length)
            {
                return true;
            }

            for (int i = 0; i < currentHash.Length; i++)
            {
                if (currentHash[i] != lastKnownHash[i])
                {
                    return true;
                }
            }

            return false;
        }
    }
}
