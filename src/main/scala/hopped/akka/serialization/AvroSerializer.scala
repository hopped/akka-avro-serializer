/*!
 * Copyright (c) 2013 Dennis Hoppe
 * www.dennis-hoppe.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package hopped.akka.serialization

import akka.serialization._

import java.io.ByteArrayOutputStream

import org.apache.avro.generic.{ GenericDatumReader, IndexedRecord }
import org.apache.avro.io.{
    BinaryEncoder,
    BinaryDecoder,
    DecoderFactory,
    EncoderFactory
}
import org.apache.avro.Schema
import org.apache.avro.specific.{
    SpecificDatumReader,
    SpecificDatumWriter,
    SpecificRecordBase
}

/**
  * This proxy is used to encapsulate the schema definition for a given Avro
  * record. Please ensure, that the schema is statically available when
  * instancing a new class using the default constructor.
  *
  * Please refer to [[hopped.akka.serialization.GenericRecordProxies]] for
  * an example (cf. test suite).
  */
trait GenericRecordProxy {
    def getSchema(): Schema
}


/**
  * This Serializer serializes and deserializes Apache Avro records. This
  * includes primarily pre-compiled records that implement the
  * [[org.apache.avro.specific.SpecificRecordBase]] interface. In order to
  * support generic Avro records, i.e. you want to serialized/deserialize Avro
  * records without code generation, please use [[GenericRecordProxy]] as
  * base class.
  */
class AvroSerializer extends Serializer {

    def includeManifest: Boolean = true
    def identifier = 14041983

    /** in-memory output stream used to hold Avro records as bytes */
    private var out = new ByteArrayOutputStream()

    /** binary Avro encoder (re-used) */
    private var encoder: BinaryEncoder =
        EncoderFactory.get().binaryEncoder(out, null)

    /** binary Avro decoder */
    private var decoder: BinaryDecoder = null

    def toBinary(obj: AnyRef): Array[Byte] = obj match {
        case record: IndexedRecord => {
            out.reset()
            encoder = EncoderFactory.get().binaryEncoder(out, encoder)
            new SpecificDatumWriter(record.getSchema).write(record, encoder)
            encoder.flush()
            out.toByteArray()
        }
        case _ => {
            val errorMsg = "Can't serialize a non-Avro message using " +
                "AvroSerializer [" + obj + "]"
            throw new IllegalArgumentException(errorMsg)
        }
    }

    def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
        case Some(clazz) if classOf[SpecificRecordBase].isAssignableFrom(clazz) => {
            val ctor = clazz.getConstructor()
            val avroObject = ctor.newInstance().asInstanceOf[SpecificRecordBase]
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder)
            new SpecificDatumReader(avroObject.getSchema).read(null, decoder)
        }
        case Some(clazz) if classOf[GenericRecordProxy].isAssignableFrom(clazz) => {
            val ctor = clazz.getConstructor()
            val avroObject = ctor.newInstance().asInstanceOf[GenericRecordProxy]
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder)
            new GenericDatumReader(avroObject.getSchema).read(null, decoder)
        }
        case Some(clazz) => {
            val errorMsg = "Need either a subclass of SpecificRecordBase or " +
                "GenericRecordProxy in order to deserialize bytes into an " +
                "Avro record [" + clazz.getClass.getSimpleName + "]"
            throw new IllegalArgumentException(errorMsg)
        }
        case None => {
            val errorMsg = "Need a given manifest in order to " +
                "deserialize bytes into an Avro record"
            throw new IllegalArgumentException(errorMsg)
        }
    }

}
