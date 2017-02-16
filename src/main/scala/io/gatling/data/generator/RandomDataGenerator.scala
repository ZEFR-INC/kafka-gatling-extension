package io.gatling.data.generator

import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericEnumSymbol, GenericRecord}

import scala.collection.JavaConverters._

class RandomDataGenerator[K: Manifest, V: Manifest] {
  private final val ByteManifest = manifest[Byte]
  private final val ShortManifest = manifest[Short]
  private final val IntManifest = manifest[Int]
  private final val LongManifest = manifest[Long]
  private final val FloatManifest = manifest[Float]
  private final val DoubleManifest = manifest[Double]
  private final val CharManifest = manifest[Char]
  private final val StringManifest = manifest[String]
  private final val BooleanManifest = manifest[Boolean]
  private final val ByteArrayManifest = manifest[Array[Byte]]
  private final val AnyManifest = manifest[Any]
  private final val AnyRefManifest = manifest[AnyRef]
  private final val GenericRecordManifest = manifest[GenericRecord]
  private final val genericData = new GenericData();
  def generateKey(schema: Option[Schema] = None): K = {
    manifest[K] match {
      case ByteManifest => 42.toByte.asInstanceOf[K]
      case ShortManifest => 42.toShort.asInstanceOf[K]
      case IntManifest => 42.asInstanceOf[K]
      case LongManifest => 42.toLong.asInstanceOf[K]
      case FloatManifest => 42.11.toFloat.asInstanceOf[K]
      case DoubleManifest => 42.11.asInstanceOf[K]
      case CharManifest => 'C'.asInstanceOf[K]
      case StringManifest => "Str".asInstanceOf[K]
      case BooleanManifest => true.asInstanceOf[K]
      case ByteArrayManifest => "Str".getBytes.asInstanceOf[K]
      case AnyManifest => 'C'.asInstanceOf[K]
      case AnyRefManifest => "Str".asInstanceOf[K]
      case GenericRecordManifest => generateDataForAvroSchema(schema).asInstanceOf[K]
      case x if x.runtimeClass.isArray =>
        Array(1, 2, 3, 4, 5).asInstanceOf[K]
    }
  }

  def generateValue(schema: Option[Schema] = None): V = {
    manifest[V] match {
      case ByteManifest => 42.toByte.asInstanceOf[V]
      case ShortManifest => 42.toShort.asInstanceOf[V]
      case IntManifest => 42.asInstanceOf[V]
      case LongManifest => 42.toLong.asInstanceOf[V]
      case FloatManifest => 42.11.toFloat.asInstanceOf[V]
      case DoubleManifest => 42.11.asInstanceOf[V]
      case CharManifest => 'C'.asInstanceOf[V]
      case StringManifest => "Str".asInstanceOf[V]
      case BooleanManifest => true.asInstanceOf[V]
      case ByteArrayManifest => "Str".getBytes.asInstanceOf[V]
      case AnyManifest => 'C'.asInstanceOf[V]
      case AnyRefManifest => "Str".asInstanceOf[V]
      case GenericRecordManifest => generateDataForAvroSchema(schema).asInstanceOf[V]
      case x if x.runtimeClass.isArray =>
        Array(1, 2, 3, 4, 5).asInstanceOf[V]
    }
  }

  protected def generateDataForAvroSchema(schemaOpt: Option[Schema]): Any = {
    schemaOpt match {
      case None => throw new RuntimeException("schema is empty. Cannot generate record")
      case Some(schema) =>
        val fieldType = schema.getType
        fieldType match {
          case Type.NULL => null
          case Type.BOOLEAN => true
          case Type.ENUM => genericData.createEnum(schema.getEnumSymbols.get(0), schema)
          case Type.INT => 42
          case Type.LONG => 42.toLong
          case Type.FLOAT => 42.11.toFloat
          case Type.DOUBLE => 42.11
          case Type.FIXED => new GenericData.Fixed(schema, Array.range(0, schema.getFixedSize).map(_.toByte) )
          case Type.STRING => "42"
          case Type.BYTES => ByteBuffer.wrap("42".getBytes)
          case Type.RECORD => {
            val avroRecord = new Record(schema)
            schema.getFields.asScala.foreach(
              field => {
                avroRecord.put(field.name(), generateDataForAvroSchema(Some(field.schema())))
              })
            avroRecord
          }
          case Type.ARRAY => Array.range(0,3).map(_ => generateDataForAvroSchema(Some(schema.getElementType))).toList.asJava
          case Type.MAP => Array.range(0,3).zipWithIndex.map(tuple => (tuple._1.toString(), generateDataForAvroSchema(Some(schema.getValueType())))).toMap.asJava
          case Type.UNION => generateDataForAvroSchema(schema.getTypes.asScala.filter(s => s.getType() != Type.NULL).headOption) // return first non null type
        }
    }
  }
}