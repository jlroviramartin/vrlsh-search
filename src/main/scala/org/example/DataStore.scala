package org.example

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.twitter.chill.KryoInstantiator
import com.twitter.chill.config.{ConfiguredInstantiator, JavaMapConfig}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Path

class TestInst extends KryoInstantiator { override def newKryo = sys.error("blow up") }

object DataStore {
    var kryo: Kryo = new Kryo()
    {
        kryo.register(classOf[(Any, Any)], new com.twitter.chill.Tuple2Serializer)
        kryo.register(classOf[Vector], VectorSerializer)
    }

    /*var kryo: Kryo = {
        val conf = new JavaMapConfig()
        ConfiguredInstantiator.setReflect(conf, classOf[TestInst])
        val cci = new ConfiguredInstantiator(conf)
        val kryo = cci.newKryo()

        kryo.register(classOf[(Any, Any)], new com.twitter.chill.Tuple2Serializer)
        kryo.register(classOf[Vector], VectorSerializer)

        kryo
    }*/

    def kstore[T](fileName: String, toStore: T): Unit = {
        val output = new Output(new FileOutputStream(fileName))
        kryo.writeObject(output, toStore)
        output.close
    }

    def kload[T](fileName: String, ttype: Class[T]): T = {
        val input = new Input(new FileInputStream(fileName))
        val result = kryo.readObject(input, ttype)
        input.close
        result
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