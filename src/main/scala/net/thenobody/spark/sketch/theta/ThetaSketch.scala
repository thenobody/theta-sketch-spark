package net.thenobody.spark.sketch.theta

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import com.yahoo.sketches.memory._
import com.yahoo.sketches.theta._

/**
  * Created by antonvanco on 27/07/2016.
  */
sealed trait ThetaSketch extends Serializable {
  def sketch: Sketch
  def estimate: Double = sketch.getEstimate
  def update(value: Any): ThetaSketch
  def merge(other: ThetaSketch): ThetaSketch
  def finalise: ThetaSketch
}

class CompactedThetaSketch(private var _sketch: Sketch) extends ThetaSketch {
  import ThetaSketch._

  def sketch: Sketch = _sketch

  def update(value: Any): ThetaSketch = throw new UnsupportedOperationException("Cannot updated a compacted theta sketch")

  def merge(other: ThetaSketch): ThetaSketch = throw new UnsupportedOperationException("Cannot perform merge on a compacted theta sketch")

  def finalise: ThetaSketch = this

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(serialiseSketch(_sketch))
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    val bytes = in.readObject().asInstanceOf[Array[Byte]]
    _sketch = deserialiseSketch(bytes)
  }
}

class SimpleThetaSketch(private var k: Int) extends ThetaSketch {
  import ThetaSketch._

  var sketch: UpdateSketch = Sketches.updateSketchBuilder().build(k)

  def merge(other: ThetaSketch): ThetaSketch = new MergedThetaSketch(k).merge(this).merge(other)

  def update(value: Any): ThetaSketch = {
    Option(value).foreach {
      case v: String => sketch.update(v)
      case v: Double => sketch.update(v)
      case v: Long => sketch.update(v)
      case v: Int => sketch.update(v)
      case v: Array[Byte] => sketch.update(v)
      case v: Array[Long] => sketch.update(v)
      case v: Array[Int] => sketch.update(v)
    }
    this
  }

  def finalise: ThetaSketch = new CompactedThetaSketch(sketch.compact)

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(k)
    out.writeObject(serialiseSketch(sketch))
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    k = in.readInt
    val bytes = in.readObject().asInstanceOf[Array[Byte]]
    sketch = deserialiseSketch(bytes).asInstanceOf[UpdateSketch]
  }
}

class MergedThetaSketch(private var k: Int) extends ThetaSketch {
  import ThetaSketch._

  private var union = Sketches.setOperationBuilder().buildUnion(k)

  def sketch: Sketch = union.getResult

  def merge(other: ThetaSketch): ThetaSketch = {
    union.update(other.sketch)
    this
  }

  def update(value: Any): ThetaSketch = {
    Option(value).foreach {
      case v: String => union.update(v)
      case v: Double => union.update(v)
      case v: Long => union.update(v)
      case v: Int => union.update(v)
      case v: Array[Byte] => union.update(v)
      case v: Array[Long] => union.update(v)
      case v: Array[Int] => union.update(v)
      case v: Memory => union.update(v)
      case v: Sketch => union.update(v)
    }
    this
  }

  def finalise: ThetaSketch = new CompactedThetaSketch(sketch)

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(k)
    out.writeObject(serialiseSetOperation(union.asInstanceOf[SetOperation]))
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    k = in.readInt
    val bytes = in.readObject().asInstanceOf[Array[Byte]]
    union = deserialiseSetOperation(bytes).asInstanceOf[Union]
  }
}

object ThetaSketch {
  val DefaultK = 512

  def apply(): ThetaSketch = ThetaSketch(DefaultK)

  def apply(k: Int): ThetaSketch = new SimpleThetaSketch(k)

  def serialiseSketch(sketch: Sketch): Array[Byte] = sketch.toByteArray

  def deserialiseSketch(bytes: Array[Byte]): Sketch = Sketches.heapifySketch(new NativeMemory(bytes))

  def serialiseSetOperation(setOperation: SetOperation): Array[Byte] = setOperation match {
    case union: Union => union.toByteArray
    case intersection: Intersection => intersection.toByteArray
  }

  def deserialiseSetOperation(bytes: Array[Byte]): SetOperation = Sketches.heapifySetOperation(new NativeMemory(bytes))
}