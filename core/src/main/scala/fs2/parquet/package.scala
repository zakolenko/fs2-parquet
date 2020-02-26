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
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

package object parquet {

  def readAll[F[_]: Sync: ContextShift, T <: AnyRef](
    parquetReader: Resource[F, ParquetReader[T]],
    blocker: Blocker,
    chunkSize: Int
  ): Stream[F, T] = {
    def read(pr: ParquetReader[T]): (Chunk[T], Option[ParquetReader[T]]) = {
      @tailrec
      def go(
        pr: ParquetReader[T],
        chunk: ArrayBuffer[T]
      ): (Chunk[T], Option[ParquetReader[T]]) = {
        val record = pr.read()

        if (record eq null) {
          (Chunk.buffer(chunk), None)
        } else {
          chunk += record
          if (chunk.length >= chunkSize) {
            (Chunk.buffer(chunk), Some(pr))
          } else {
            go(pr, chunk)
          }
        }
      }

      val chunk = ArrayBuffer.empty[T]
      chunk.sizeHint(chunkSize)
      go(pr, chunk)
    }

    Stream
      .resource(parquetReader)
      .flatMap {
        Stream.unfoldLoopEval(_) { pr =>
          blocker.delay(read(pr))
        }
      }
      .flatMap(Stream.chunk)
  }

  def readAll[F[_]: Sync: ContextShift, T <: AnyRef: ClassTag](
    parquetReader: F[ParquetReader[T]],
    blocker: Blocker,
    chunkSize: Int
  ): Stream[F, T] = {
    readAll(
      Resource.make(parquetReader)(pr => blocker.delay(pr.close())),
      blocker,
      chunkSize
    )
  }

  def readAll[F[_]: Sync: ContextShift, T <: AnyRef: ClassTag](
    parquetReader: => ParquetReader[T],
    blocker: Blocker,
    chunkSize: Int
  ): Stream[F, T] = {
    readAll(blocker.delay(parquetReader), blocker, chunkSize)
  }

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
    parquetWriter: Resource[F, ParquetWriter[T]],
    blocker: Blocker
  ): Pipe[F, T, Unit] = {
    in =>
      Stream
        .resource(parquetWriter)
        .flatMap { pw =>
          in.chunks.evalMap { chunk =>
            blocker.delay {
              chunk.foreach(pw.write)
            }
          }
        }
  }

  def writeAll[F[_]: Sync: ContextShift, T](
    parquetWriter: F[ParquetWriter[T]],
    blocker: Blocker
  ): Pipe[F, T, Unit] = {
    writeAll(
      Resource.make(parquetWriter)(pr => blocker.delay(pr.close())),
      blocker
    )
  }

  def writeAll[F[_]: Sync: ContextShift, T](
    parquetWriter: => ParquetWriter[T],
    blocker: Blocker
  ): Pipe[F, T, Unit] = {
    writeAll(
      blocker.delay(parquetWriter),
      blocker
    )
  }
}
