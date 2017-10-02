# Using Cats with Frameless

```tut:invisible
import org.apache.spark.{SparkConf, SparkContext => SC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("cats.bec test")
val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
implicit val sc: SC = spark.sparkContext
implicit val sqlContext = spark.sqlContext

spark.sparkContext.setLogLevel("WARN")
System.clearProperty("spark.master.port")
System.clearProperty("spark.driver.port")
System.clearProperty("spark.hostPort")
System.setProperty("spark.cleaner.ttl", "300")

import spark.implicits._

import cats.implicits._
import cats.effect.{IO, Sync}
import cats.data.ReaderT

implicit val sync: Sync[ReaderT[IO, SparkSession, ?]] = new Sync[ReaderT[IO, SparkSession, ?]] {
  def suspend[A](thunk: => ReaderT[IO, SparkSession, A]) = thunk
  def pure[A](x: A): ReaderT[IO, SparkSession, A] = ReaderT.pure(x)
  def handleErrorWith[A](fa: ReaderT[IO, SparkSession, A])(f: Throwable => ReaderT[IO, SparkSession, A]): ReaderT[IO, SparkSession, A] =
    ReaderT(r => fa.run(r).handleErrorWith(e => f(e).run(r)))
  def raiseError[A](e: Throwable): ReaderT[IO, SparkSession, A] = ReaderT.lift(IO.raiseError(e))
  def flatMap[A, B](fa: ReaderT[IO, SparkSession, A])(f: A => ReaderT[IO, SparkSession, B]): ReaderT[IO, SparkSession, B] = fa.flatMap(f)
  def tailRecM[A, B](a: A)(f: A => ReaderT[IO, SparkSession, Either[A, B]]): ReaderT[IO, SparkSession, B] =
    ReaderT.catsDataMonadForKleisli[IO, SparkSession].tailRecM(a)(f)
}
```

There are two main parts to the `cats` integration offered by frameless:
- effect suspension in `TypedDataset` using `cats-effect` and `cats-mtl`
- `RDD` enhancements using algebraic typeclasses in `cats-kernel`

All the examples below assume you have previously imported `cats.implicits` and `frameless.cats.implicits`.

*Note that you should not import `frameless.syntax._` together with `frameless.cats.implicits._`.*

```tut:book
import cats.implicits._
import frameless.cats.implicits._
```

## Effect Suspension in typed datasets

As noted in the section about `Job`, all operations on `TypedDataset` are lazy. The results of 
operations that would normally block on plain Spark APIs are wrapped in a type constructor `F[_]`, 
for which there exists an instance of `SparkDelay[F]`. This typeclass represents the operation of 
delaying a computation and capturing an implicit `SparkSession`. 

In the `cats` module, we utilize the typeclasses from `cats-effect` for abstracting over these 
effect types - namely, we provide an implicit `SparkDelay` instance for all `F[_]` for which exists
an instance of `cats.effect.Sync[F]`.

This allows one to run operations on `TypedDataset` in an existing monad stack. For example, given
this pre-existing monad stack:
```tut:book
import frameless.TypedDataset
import cats.data.ReaderT
import cats.effect.IO
import cats.effect.implicits._

type Action[T] = ReaderT[IO, SparkSession, T]
```

We will be able to request that values from `TypedDataset` will be suspended in this stack:
```tut:book
val typedDs = TypedDataset.create(Seq((1, "string"), (2, "another")))
val result: Action[(Seq[(Int, String)], Long)] = for {
  sample <- typedDs.take(1)
  count <- typedDs.count()
} yield (sample, count)
```

As with `Job`, note that nothing has been run yet. The effect has been properly suspended. To
run our program, we must first supply the `SparkSession` to the `ReaderT` layer and then
run the `IO` effect:
```tut:book
result.run(spark).unsafeRunSync()
```

### Convenience methods for modifying Spark thread-local variables

The `frameless.cats.implicits._` import also provides some syntax enrichments for any monad
stack that has the same capabilities as `Action` above. Namely, the ability to provide an
instance of `SparkSession` and the ability to suspend effects.

For these to work, we will need to import the implicit machinery from the `cats-mtl` library:
```tut:book
import cats.mtl.implicits._
```

And now, we can set the description for the computation being run:
```tut:book
val resultWithDescription: Action[(Seq[(Int, String)], Long)] = for {
  r <- result.withDescription("fancy cats")
  session <- ReaderT.ask[IO, SparkSession]
  _ <- ReaderT.lift {
         IO {
           println(s"Description: ${session.sparkContext.getLocalProperty("spark.job.description")}")
         }
       }
} yield r

resultWithDescription.run(spark).unsafeRunSync()
```

## Using algebraic typeclasses from Cats with RDDs

Data aggregation is one of the most important operations when working with Spark (and data in general).
For example, we often have to compute the `min`, `max`, `avg`, etc. from a set of columns grouped by
different predicates. This section shows how **cats** simplifies these tasks in Spark by
leveraging a large collection of Type Classes for ordering and aggregating data.


Cats offers ways to sort and aggregate tuples of arbitrary arity.

```tut:book
import frameless.cats.implicits._

val data: RDD[(Int, Int, Int)] = sc.makeRDD((1, 2, 3) :: (1, 5, 3) :: (8, 2, 3) :: Nil)

println(data.csum)
println(data.cmax)
println(data.cmin)
```

The following example aggregates all the elements with a common key.

```tut:book
type User = String
type TransactionCount = Int

val allData: RDD[(User,TransactionCount)] =
   sc.makeRDD(("Bob", 12) :: ("Joe", 1) :: ("Anna", 100) :: ("Bob", 20) :: ("Joe", 2) :: Nil)

val totalPerUser =  allData.csumByKey

totalPerUser.collectAsMap
```

The same example would work for more complex keys.

```tut:book
val allDataComplexKeu =
   sc.makeRDD( ("Bob", Map("task1" -> 10)) ::
    ("Joe", Map("task1" -> 1, "task2" -> 3)) :: ("Bob", Map("task1" -> 10, "task2" -> 1)) :: ("Joe", Map("task3" -> 4)) :: Nil )

val overalTasksPerUser = allDataComplexKeu.csumByKey

overalTasksPerUser.collectAsMap
```

#### Joins

```tut:book
// Type aliases for meaningful types
type TimeSeries = Map[Int,Int]
type UserName = String
```

Example: Using the implicit full-our-join operator

```tut:book
import frameless.cats.outer._

val day1: RDD[(UserName,TimeSeries)] = sc.makeRDD( ("John", Map(0 -> 2, 1 -> 4)) :: ("Chris", Map(0 -> 1, 1 -> 2)) :: ("Sam", Map(0 -> 1)) :: Nil )
val day2: RDD[(UserName,TimeSeries)] = sc.makeRDD( ("John", Map(0 -> 10, 1 -> 11)) :: ("Chris", Map(0 -> 1, 1 -> 2)) :: ("Joe", Map(0 -> 1, 1 -> 2)) :: Nil )

val daysCombined = day1 |+| day2

daysCombined.collect()
```

Note how the user's timeseries from different days have been aggregated together.
The `|+|` (Semigroup) operator for key-value pair RDD will execute a full-outer-join
on the key and combine values using the default Semigroup for the value type.

In `cats`:

```tut:book
Map(1 -> 2, 2 -> 3) |+| Map(1 -> 4, 2 -> -1)
```

```tut:invisible
spark.stop()
```
