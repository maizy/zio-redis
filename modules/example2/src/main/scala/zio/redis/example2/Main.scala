package zio.redis.example2

import zio._
import zio.redis._

object Main extends ZIOAppDefault {

  private val redisRoutine: ZIO[Redis, RedisError, Unit] = for {
    redis    <- ZIO.service[Redis]
    key       = "test"
    thisRun  <- Clock.currentDateTime
    before   <- redis.get(key).returning[Long]
                  .catchAllDefect {
                    e => ZIO.log(s"fallback for $key because of defect $e").as(Option.empty[Long])
                  }
                  .catchAll {
                    e => ZIO.log(s"fallback for $key because of error $e").as(Option.empty[Long])
                  }
    _        <- Console.printLine(s"Value of $key before: $before").orDie
    _        <- redis.set(key, thisRun.toInstant.toEpochMilli)
    after    <- redis.get(key).returning[Long]
    _        <- Console.printLine(s"Value of $key after: $after").orDie
  } yield ()


  def run: ZIO[ZIOAppArgs with Scope, Any, Any] =
    redisRoutine.provide(
      Redis.singleNode.tapError {
        e => ZIO.log(s"redis connection error: $e")
      },
      ZLayer.succeed[CodecSupplier](CodecSupplier.utf8),
      ZLayer.succeed(RedisConfig("127.0.0.1", 6379))
    )
}
