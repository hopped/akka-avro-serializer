# Akka Serializer for Apache Avro

## Description
This project provides classes for serializing and deserializing **Apache Avro**
records using the **Akka** serialization interface. It uses Akka's built-in
Serializer interface to ensure full compatibility with Akka.

If your are not familiar with **Apache Avro** or **Akka**, please read on here:

  1. The [Apache Avro](http://avro.apache.org) data serialization system
  2. The [Akka](http://www.akka.io) toolkit for building event-driven apps on the JVM

The following **tutorial** is based on Scala 2.10.2, Apache Avro 1.7.4, and
Akka 2.10.2.

This is just demonstration code, and not optimized for production.

If you find bugs, come up with suggestions, or would like to
contribute improvements, just let me know!


## Project details

Since this is a [Gradle](http://www.gradle.org) project, you can

  1. Build a JAR using: **gradle jar**
  2. Run the test suite: **gradle test**
  3. Generate the Scala documentation: **gradle scalaDoc**
  4. ...

A Gradle Avro plugin is applied in order to compile Avro definition files
automatically while building the project.


## Usage
This section discusses both the **serialization** and **deserialization** of
Avro records using Akka in combination with the **AvroSerializer**.

### Configuration of the ActorSystem
In order to make use of Avro serialization, you need to tell the
**akka.actor.ActorSystem** about the Avro serializer to be used,
**hopped.akka.serialization.AvroSerializer**, and which objects are to be
serialized, as follows:

    import akka.actor.ActorSystem
    import akka.serialization._
    import com.typesafe.config.ConfigFactory

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
    val system = ActorSystem("example", ConfigFactory.load(config))
    val serialization = SerializationExtension(system)

Alternatively, you can add the information shown above to one of your
configuration files (cf. Akka's documentation on [configuration of
serializers](http://doc.akka.io/docs/akka/snapshot/scala/serialization.html)).

### A simple Avro Record

Assuming **pre-compiled classes** (package **hopped.akka.serialization.avro**)
exist for the following **Avro definition**:

    {
      "namespace" : "hopped.akka.serialization.avro",
      "name" : "SearchRequest",
      "type" : "record",
      "fields" : [
        {
          "name" : "Query",
          "type" : "string"
        },
        {
          "name" : "Source",
          "type" : "string"
        },
        {
          "name" : "Targets",
          "type" : {
            "type"  : "array",
            "items" : "string"
          }
        }
      ]
    }

### Serialization and Deserialization of Avro records

Once the **ActorSystem** is set up, Avro record objects can be serialized and
deserialized as follows (using auto-compiled version of a **SearchRequest**
definition):

    import hopped.akka.serialization.avro.SearchRequest

    // create a new SearchRequest
    val targets: ListBuffer[CharSequence] = ListBuffer("en")
    val avroObject = new SearchRequest("keyword", "de", targets.asJava)

    // get the appropriate Akka serializer (here: AvroSerializer)
    val avroSerializer = serializerSystem.findSerializerFor(avroObject)

    // serialize the SearchRequest
    val serialized = avroSerializer.toBinary(avroObject)

    // deserialize the binary representation of the SearchRequest
    val deserialized = avroSerializer.fromBinary(serialized, avroObject.getClass).asInstanceOf[SearchRequest]

After deserialization, you can access the fields of the Avro record as usual:

    // access the Query field
    var query = deserialized.getQuery

    // access the Query field via 'get'
    query = deserialized.get("Query")


## Author

Dennis Hoppe ([www.dennis-hoppe.com](http://www.dennis-hoppe.com))


## Copyright and License

Copyright (c) 2013 Dennis Hoppe

This content is released under the [MIT License](http://opensource.org/licenses/MIT).
