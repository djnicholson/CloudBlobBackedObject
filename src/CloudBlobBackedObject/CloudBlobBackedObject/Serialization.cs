using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace CloudBlobBackedObject
{
    internal class Serialization
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
    }
}
