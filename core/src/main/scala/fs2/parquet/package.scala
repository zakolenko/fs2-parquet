/*
 * Copyright (c) 2020 the fs2-parquet contributors.
 * See the project homepage at: https://zakolenko.github.io/fs2-parquet/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2

import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}
import org.apache.parquet.hadoop.ParquetWriter

package object parquet {

  def writeRotate[F[_]: Concurrent: ContextShift, T](
    parquetWriter: Resource[F, ParquetWriter[T]],
    limit: Long,
    blocker: Blocker
  ): Pipe[F, T, Unit] = {
    def go(
      parquetHotswap: Hotswap[F, ParquetWriter[T]],
      parquet: ParquetWriter[T],
      s: Stream[F, T]
    ): Pull[F, Unit, Unit] = {
      s.pull.uncons1.flatMap {
        case Some((hd, tl)) =>
          Pull
            .eval {
              blocker.delay {
                parquet.write(hd)
                parquet.getDataSize
              }
            }
            .flatMap { size =>
              if (size >= limit) {
                Pull
                  .eval(parquetHotswap.swap(parquetWriter))
                  .flatMap(go(parquetHotswap, _, tl))
              } else {
                go(parquetHotswap, parquet, tl)
              }
            }

        case None =>
          Pull.done
      }
    }

    in =>
      Stream
        .resource(Hotswap(parquetWriter))
        .flatMap {
          case (parquetHotswap, parquet) =>
            go(parquetHotswap, parquet, in).stream
        }
  }

  def writeRotate[F[_]: Concurrent: ContextShift, T](
    parquetWriter: F[ParquetWriter[T]],
    limit: Long,
    blocker: Blocker
  ): Pipe[F, T, Unit] = {
    writeRotate(
      Resource.make(parquetWriter)(pw => blocker.delay(pw.close())),
      limit,
      blocker
    )
  }

  def writeAll[F[_]: Sync: ContextShift, T](
    parquetWriter: => ParquetWriter[T],
    blocker: Blocker
  ): Pipe[F, T, Unit] = {
    in =>
      Stream
        .resource {
          Resource.make(blocker.delay(parquetWriter))(pw => blocker.delay(pw.close()))
        }
        .flatMap { pw =>
          in.chunks.evalMap { chunk =>
            blocker.delay {
              chunk.foreach(pw.write)
            }
          }
        }
  }
}
