package frameless


import java.sql.Date
import org.apache.spark.sql.catalyst.util.DateTimeUtils._

trait CatalystDateTime[A] {
  /**
    * This method is intended to provide parity between frameless date types and sparks behaviour. After calling some date/time column methods, spark will upcast the resulting type to java.sql.Date.
    * See https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/ScalaReflection.scala#L185
    */
  def toJavaSQLDate(a:A):Date
}

object CatalystDateTime{

  def apply[A](implicit instance:CatalystDateTime[A]):CatalystDateTime[A] = instance

  implicit object sqlDate extends CatalystDateTime[SQLDate] {
    override def toJavaSQLDate(a: SQLDate): Date = toJavaDate(a.days)
  }
  implicit object sqlTimeStamp extends CatalystDateTime[SQLTimestamp] {
    override def toJavaSQLDate(a: SQLTimestamp): Date = toJavaDate(millisToDays(toJavaTimestamp(a.us).getTime))
  }

}
