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

        public static bool DeserializeInto<T>(ref T target, Stream serializedObject)
        {
            if (serializedObject.Length != 0)
            {
                serializedObject.Seek(0, SeekOrigin.Begin);
                BinaryFormatter formatter = new BinaryFormatter();
                try
                {
                    target = (T)formatter.Deserialize(serializedObject);
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
