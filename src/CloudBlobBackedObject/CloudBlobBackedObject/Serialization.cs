using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace CloudBlobBackedObject
{
    internal static class Serialization
    {
        private static readonly byte[] NullHash = new byte[0];

        /// <summary>
        /// Write a serialized representation of obj into serializedObjectWriter. 
        /// </summary>
        public static void SerializeIntoStream<T>(T obj, Stream serializedObjectWriter)
        {
            if (obj == null)
            {
                return;
            }

            BinaryFormatter formatter = new BinaryFormatter();
            formatter.Serialize(serializedObjectWriter, obj);
        }

        /// <summary>
        /// Returns true if the serialized representation of obj has changed since the lastKnownHash
        /// was produced.
        /// </summary>
        public static bool ModifiedSince<T>(T obj, byte[] lastKnownHash, out byte[] newHash)
        {
            if (obj == null)
            {
                newHash = NullHash;
            }
            else
            {
                SHA256StreamHasher hasher = new SHA256StreamHasher();
                SerializeIntoStream(obj, hasher);
                newHash = hasher.ComputeHash();
            }

            return (lastKnownHash == null) || !HashEquals(newHash, lastKnownHash);
        }

        public static bool DeserializeInto<T>(ref T target, Stream serializedObjectReader, out byte[] lastKnownHash)
        {
            lastKnownHash = NullHash;

            if (serializedObjectReader.Length != 0)
            {
                serializedObjectReader.Seek(0, SeekOrigin.Begin);
                SHA256StreamHasher readAndHash = new SHA256StreamHasher(serializedObjectReader);
                BinaryFormatter formatter = new BinaryFormatter();
                try
                {
                    target = (T)formatter.Deserialize(readAndHash);
                }
                catch (SerializationException)
                {
                    return false;
                }
                lastKnownHash = readAndHash.ComputeHash();
            }

            return true;
        }

        private static bool HashEquals(byte[] currentHash, byte[] lastKnownHash)
        {
            if (currentHash.Length != lastKnownHash.Length)
            {
                return false;
            }

            for (int i = 0; i < currentHash.Length; i++)
            {
                if (currentHash[i] != lastKnownHash[i])
                {
                    return false;
                }
            }

            return true;
        }
    }
}
