package net.thenobody.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * Created by antonvanco on 27/07/2016.
  */
object SerialisationUtil {

  def deserialise(bytes: Array[Byte]): Any = {
    val stream = new ObjectInputStream(new ByteArrayInputStream(bytes))
    stream.readObject
  }

  def serialise(instance: Any): Array[Byte] = {
    val bytes = new ByteArrayOutputStream
    val outputStream = new ObjectOutputStream(bytes)

    outputStream.writeObject(instance)
    outputStream.close()

    bytes.toByteArray
  }
}
