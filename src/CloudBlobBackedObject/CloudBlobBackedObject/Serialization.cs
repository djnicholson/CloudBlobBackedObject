using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;

namespace CloudBlobBackedObject
{
    internal static class Serialization
    {
        public static byte[] Serialize(Object obj)
        {
            if (obj == null)
            {
                return new byte[0];
            }
            else
            {
                BinaryFormatter formatter = new BinaryFormatter();
                MemoryStream memory = new MemoryStream();
                formatter.Serialize(memory, obj);
                return memory.GetBuffer();
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

        public static bool HashesAreEqual(byte[] xs, byte[] ys)
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
    }
}
