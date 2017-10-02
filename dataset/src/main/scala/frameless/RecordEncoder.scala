package frameless

import org.apache.spark.sql.FramelessInternals
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, NewInstance}
import org.apache.spark.sql.types._
import shapeless.labelled.FieldType
import shapeless._

import scala.reflect.ClassTag

case class RecordEncoderField(
  ordinal: Int,
  name: String,
  encoder: TypedEncoder[_]
)

trait RecordEncoderFields[T <: HList] extends Serializable {
  def value: List[RecordEncoderField]
}

object RecordEncoderFields {
  implicit def deriveRecordLast[K <: Symbol, H](
    implicit
    key: Witness.Aux[K],
    head: TypedEncoder[H]
  ): RecordEncoderFields[FieldType[K, H] :: HNil] = new RecordEncoderFields[FieldType[K, H] :: HNil] {
    def value: List[RecordEncoderField] = RecordEncoderField(0, key.value.name, head) :: Nil
  }

  implicit def deriveRecordCons[K <: Symbol, H, T <: HList](
    implicit
    key: Witness.Aux[K],
    head: TypedEncoder[H],
    tail: RecordEncoderFields[T]
  ): RecordEncoderFields[FieldType[K, H] :: T] = new RecordEncoderFields[FieldType[K, H] :: T] {
    def value: List[RecordEncoderField] = {
      val fieldName = key.value.name
      val fieldEncoder = RecordEncoderField(0, fieldName, head)

      fieldEncoder :: tail.value.map(x => x.copy(ordinal = x.ordinal + 1))
    }
  }
}

class RecordEncoder[F, G <: HList](
  implicit
  lgen: LabelledGeneric.Aux[F, G],
  fields: Lazy[RecordEncoderFields[G]],
  classTag: ClassTag[F]
) extends TypedEncoder[F] {
  def nullable: Boolean = false

  def jvmRepr: DataType = FramelessInternals.objectTypeFor[F]

  def catalystRepr: DataType = {
    val structFields = fields.value.value.map { field =>
      StructField(
        name = field.name,
        dataType = field.encoder.catalystRepr,
        nullable = field.encoder.nullable,
        metadata = Metadata.empty
      )
    }

    StructType(structFields)
  }

  def toCatalyst(path: Expression): Expression = {
    val nameExprs = fields.value.value.map { field =>
      Literal(field.name)
    }

    val valueExprs = fields.value.value.map { field =>
      val fieldPath = Invoke(path, field.name, field.encoder.jvmRepr, Nil)
      field.encoder.toCatalyst(fieldPath)
    }

    // the way exprs are encoded in CreateNamedStruct
    val exprs = nameExprs.zip(valueExprs).flatMap {
      case (nameExpr, valueExpr) => nameExpr :: valueExpr :: Nil
    }

    CreateNamedStruct(exprs)
  }

  def fromCatalyst(path: Expression): Expression = {
    val exprs = fields.value.value.map { field =>
      val fieldPath = path match {
        case BoundReference(ordinal, dataType, nullable) =>
          GetColumnByOrdinal(field.ordinal, field.encoder.jvmRepr)
        case other =>
          GetStructField(path, field.ordinal, Some(field.name))
      }
      field.encoder.fromCatalyst(fieldPath)
    }

    NewInstance(classTag.runtimeClass, exprs, jvmRepr, propagateNull = true)
  }
}
