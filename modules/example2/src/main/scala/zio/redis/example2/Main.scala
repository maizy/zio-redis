package zio.redis.example2

import zio._
import zio.redis._

object Main extends ZIOAppDefault {

  private def redisRoutine(id: String): ZIO[Redis, RedisError, String] = (for {
    redis    <- ZIO.service[Redis] <* ZIO.log(s"[$id] service is ready")
    sleep    <- Random.nextIntBetween(1, 10).map(_.seconds)
    _        <- ZIO.log(s"[$id] sleep for $sleep") *> Clock.sleep(sleep)
    key       = s"test:$id"
    thisRun  <- Clock.currentDateTime
    before   <- redis.get(key).returning[Long]
                  .catchAllDefect {
                    e => ZIO.log(s"[$id] fallback for $key because of defect $e").as(Option.empty[Long])
                  }
                  .catchAll {
                    e => ZIO.log(s"[$id] fallback for $key because of error $e").as(Option.empty[Long])
                  }
    _        <- ZIO.log(s"[$id] Value of $key before: $before")
    _        <- redis.set(key, thisRun.toInstant.toEpochMilli, expireTime = Some(5.seconds))
    after    <- redis.get(key).returning[Long]
    _        <- ZIO.log(s"[$id] Value of $key after: $after")
  } yield id).onInterrupt(onInterruptRedis(id))

  private def onInterruptRedis(id: String) = (fibers: Set[FiberId])
    => ZIO.log(s"interrupt $id: fibers=[${fibers.map(_.threadName).mkString(";")}]")


  private val scatterGather: ZIO[Redis, RedisError, String] =
    redisRoutine("a") race redisRoutine("b") race redisRoutine("c")

  def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    (for {
      _       <- ZIO.log("start")
      win1   <- scatterGather
      win2   <- scatterGather
      win3   <- scatterGather
      _ <- ZIO.log(s"res: $win1 $win2 $win3")
    } yield ()).provide(
      Redis.singleNode.tapError {
        e => ZIO.log(s"redis connection error: $e")
      },
      ZLayer.succeed[CodecSupplier](CodecSupplier.utf8),
      ZLayer.succeed(RedisConfig("127.0.0.1", 6379))
    )
}
