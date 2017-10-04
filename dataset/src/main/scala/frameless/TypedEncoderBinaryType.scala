package frameless

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType}

object TypedEncoderBinaryType {
  implicit val binaryEncoder:TypedEncoder[Array[Byte]] = new TypedEncoder[Array[Byte]]() {

    def nullable: Boolean = false

    def jvmRepr: DataType = BinaryType
    def catalystRepr: DataType = BinaryType

    def toCatalyst(path: Expression): Expression = path

    def fromCatalyst(path: Expression): Expression = path
  }
}
