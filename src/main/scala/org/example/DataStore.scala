package org.example

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.{CollectionSerializer, MapSerializer}
import com.twitter.chill.{KryoInstantiator, ScalaKryoInstantiator, Tuple2Serializer, WrappedArraySerializer}
import com.twitter.chill.config.{ConfiguredInstantiator, JavaMapConfig}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Path
import scala.collection.immutable
import scala.collection.mutable

object DataStore {
    val kryo = {
        val instantiator = new ScalaKryoInstantiator()
        instantiator.setRegistrationRequired(true)
        val kryo: Kryo = instantiator.newKryo

        //val kryo: Kryo = new Kryo()

        kryo.register(classOf[mutable.WrappedArray.ofRef[Any]], new WrappedArraySerializer[Any])
        //kryo.register(classOf[immutable.Vector[Any]], new CollectionSerializer)
        kryo.register(classOf[(Any, Any)], new Tuple2Serializer)
        kryo.register(classOf[Vector], VectorSerializer)
        //kryo.register(classOf[Map[_, _]], new MapSerializer)
        kryo
    }

    def kstore[T](fileName: String, toStore: T): Unit = {
        val output = new Output(new FileOutputStream(fileName))
        kryo.writeClassAndObject(output, toStore)
        output.close
    }

    def kload[T](fileName: String, ttype: Class[T]): T = {
        val input = new Input(new FileInputStream(fileName))
        val result = kryo.readClassAndObject(input)
        input.close
        result.asInstanceOf[T]
    }

    def kstore[T](fileName: Path, toStore: T): Unit = kstore[T](fileName.toString, toStore)

    def kload[T](fileName: Path, ttype: Class[T]): T = kload[T](fileName.toString, ttype)

    def store[T](fileName: String, toStore: T): Unit = {
        val oos = new ObjectOutputStream(new FileOutputStream(fileName))
        oos.writeObject(toStore)
        oos.close
    }

    def load[T](fileName: String): T = {
        val ois = new ObjectInputStream(new FileInputStream(fileName))
        val result = ois.readObject.asInstanceOf[T]
        ois.close
        result
    }

    def store[T](fileName: Path, toStore: T): Unit = store[T](fileName.toString, toStore)

    def load[T](fileName: Path): T = load[T](fileName.toString)
}

object VectorSerializer extends Serializer[Vector] {
    override def write(kryo: Kryo, output: Output, t: Vector): Unit = {
        output.writeInt(t.toArray.size)
        output.writeDoubles(t.toArray)
    }

    override def read(kryo: Kryo, input: Input, aClass: Class[Vector]): Vector = {
        val size = input.readInt()
        val values = input.readDoubles(size)
        Vectors.dense(values)
    }
}

object InmutableVectorSerializer extends Serializer[Vector] {
    override def write(kryo: Kryo, output: Output, t: Vector): Unit = {
        output.writeInt(t.toArray.size)
        output.writeDoubles(t.toArray)
    }

    override def read(kryo: Kryo, input: Input, aClass: Class[Vector]): Vector = {
        val size = input.readInt()
        val values = input.readDoubles(size)
        Vectors.dense(values)
    }
}