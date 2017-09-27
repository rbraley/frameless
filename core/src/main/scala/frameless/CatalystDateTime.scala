package frameless

/**
  * Created by domin on 26.09.2017.
  */
trait CatalystDateTime[T]

object CatalystDateTime{
  implicit object sqlDate extends CatalystDateTime[SQLDate]
  implicit object sqlTimeStamp extends CatalystDateTime[SQLTimestamp]
}
