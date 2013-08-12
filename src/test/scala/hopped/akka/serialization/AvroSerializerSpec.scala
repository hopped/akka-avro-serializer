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

import akka.actor.{ ActorRef, ActorSystem }
import akka.serialization._

import com.typesafe.config.ConfigFactory

import hopped.akka.serialization.avro.{ SearchRequest, SearchResponse }

import java.io.File
import java.util.ArrayList
import java.util.HashMap
import java.util.List

import org.apache.avro.generic.{
    GenericData,
    GenericRecord,
    GenericDatumReader,
    GenericDatumWriter,
    IndexedRecord
}
import org.apache.avro.specific.{
    SpecificDatumReader,
    SpecificDatumWriter,
    SpecificRecordBase
}
import org.apache.avro.io.{ DecoderFactory, EncoderFactory }
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.{ FlatSpec, WordSpec, BeforeAndAfterAll, Tag }
import org.scalatest.matchers._
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


trait AvroRecordBehaviour { this: FlatSpec with Matchers =>

    def AvroRecord(avroSystem: => Serialization, avroRecord: => AnyRef) {

        val ser = avroSystem.findSerializerFor(avroRecord)

        case class TestClass(hello: String)
        val testObj = new TestClass("world")

        it must "serialize given IndexedRecord into Array[Byte]" in {
            val clazz = ser.toBinary(avroRecord).getClass
            clazz should equal (Array[Byte]().getClass)
        }

        it must "throw IllegalArgumentException when no object (toBinary)" in {
            intercept[IllegalArgumentException] {
                ser.toBinary()
            }
        }

        it must "throw IllegalArgumentException when wrong object (toBinary)" in {
            intercept[IllegalArgumentException] {
                ser.toBinary(testObj)
            }
        }

        it must "throw IllegalArgumentException when no manifest (fromBinary)" in {
            intercept[IllegalArgumentException] {
                ser.fromBinary(ser.toBinary(avroRecord))
            }
        }

        it must "throw IllegalArgumentException when wrong manifest (fromBinary)" in {
            intercept[IllegalArgumentException] {
                ser.fromBinary(ser.toBinary(avroRecord), classOf[TestClass])
            }
        }

    }

}

@RunWith(classOf[JUnitRunner])
class AvroSerializerSpec extends FlatSpec with Matchers with AvroRecordBehaviour {

    val reqSchema = getSchemaParser("../../resources/test/avro/request.avsc")
    val resSchema = getSchemaParser("../../resources/test/avro/response.avsc")
    val targets: ListBuffer[CharSequence] = ListBuffer("en")
    val translations: Map[CharSequence, CharSequence] = Map(("de", "Schluesselwort"))

	def serializerSystem = {
		val config = ConfigFactory.parseString("""
		    akka {
		      actor {
		        serializers {
		          avro = "hopped.akka.serialization.AvroSerializer"
		        }

		        serialization-bindings {
		          "java.io.Serializable" = none
		          "org.apache.avro.generic.GenericContainer" = avro
		        }
		      }
		    }
		""")
		val system = ActorSystem("AvroSystem", ConfigFactory.load(config))
		SerializationExtension(system)
	}

    def getSchemaParser(path: String): Schema = {
        val resource = getClass.getClassLoader.getResource(path)
        new Parser().parse(new File(resource.toURI))
    }

	def sRecordRequest = {
		new SearchRequest("keyword", "de", targets.asJava)
	}

	def sRecordResponse = {
		new SearchResponse("keyword", translations.asJava)
	}

    def gRecordRequest = {
        val request = new GenericData.Record(reqSchema);
        request.put("Query", "keyword")
        request.put("Source", "de")
        request.put("Targets", targets.asJava)
        request
    }

    def gRecordResponse = {
        val response = new GenericData.Record(resSchema);
        response.put("Query", "keyword")
        response.put("Translations", translations.asJava)
        response
    }

	"The defined SearchRequest" must "return 'keyword' as query" in {
		sRecordRequest.getQuery should equal ("keyword")
	}

	it must	"return 'de' as source language" in {
		sRecordRequest.getSource should equal ("de")
	}

    it must	"return 'en' as the only target language" in {
		sRecordRequest.getTargets.size should equal (1)
		sRecordRequest.getTargets.get(0) should equal ("en")
	}

    "The ActorSystem" must "return an AvroSerializer for a given specific Avro record" in {
        val avroSerializer = serializerSystem.findSerializerFor(sRecordRequest)
        avroSerializer.getClass should equal (classOf[AvroSerializer])
    }

    it must "return an AvroSerializer for a given generic Avro record" in {
        val avroSerializer = serializerSystem.findSerializerFor(gRecordRequest)
        avroSerializer.getClass should equal (classOf[AvroSerializer])
    }

    "AvroSerializer" must "have a specific identifier" in {
        new AvroSerializer().identifier should equal (14041983)
    }

    it must "need a manifest" in {
        new AvroSerializer().includeManifest should equal (true)
    }

	"SearchRequest" should behave like
        AvroRecord(serializerSystem, sRecordRequest)

    it must "serialize and deserialize properly" in {
        val avroSerializer = serializerSystem.findSerializerFor(sRecordRequest)
        val serialized = avroSerializer.toBinary(sRecordRequest)
        val obj = avroSerializer
            .fromBinary(serialized, sRecordRequest.getClass)
            .asInstanceOf[SearchRequest]
        obj.getQuery.toString should equal ("keyword")

    }

	"SearchResponse" should behave like
        AvroRecord(serializerSystem, sRecordResponse)

    it must "serialize and deserialize properly" in {
        val avroSerializer = serializerSystem.findSerializerFor(sRecordResponse)
        val serialized = avroSerializer.toBinary(sRecordResponse)
        var obj = avroSerializer
            .fromBinary(serialized, sRecordResponse.getClass)
            .asInstanceOf[SearchResponse]
        obj.getQuery.toString should equal ("keyword")
    }

    "SearchRequestProxy" should behave like
        AvroRecord(serializerSystem, gRecordRequest)

    it must "serialize and deserialize properly" in {
        val avroSerializer = serializerSystem.findSerializerFor(gRecordRequest)
        val serialized = avroSerializer.toBinary(gRecordRequest)
        var obj = avroSerializer
            .fromBinary(serialized, classOf[SearchRequestProxy])
            .asInstanceOf[GenericData.Record]
        obj.get("Query").toString should equal ("keyword")
    }

    "SearchResponseProxy" should behave like
        AvroRecord(serializerSystem, gRecordResponse)

    it must "serialize and deserialize properly" in {
        val avroSerializer = serializerSystem.findSerializerFor(gRecordResponse)
        val serialized = avroSerializer.toBinary(gRecordResponse)
        var obj = avroSerializer
            .fromBinary(serialized, classOf[SearchResponseProxy])
            .asInstanceOf[GenericData.Record]
        obj.get("Query").toString should equal ("keyword")
    }

}
