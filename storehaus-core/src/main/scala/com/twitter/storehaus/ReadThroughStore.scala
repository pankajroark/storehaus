/*
 * Copyright 2014 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.storehaus

import com.twitter.concurrent.AsyncMutex
import com.twitter.util.Future

/**
 * Provides read-through caching on a readable store fronted by a cache.
 *
 * Keys are fetched from backing store on cache miss and cache read failures.
 *
 * All cache operations are best effort i.e. 'get' will return the key from
 * backing store even if adding/updating the cached copy fails.
 *
 * On the other hand, any failure while reading from backing store
 * is propagated to the client.
 *
 * Thread-safety is achieved using a mutex.
 *
 * @author Ruban Monu
 */
class ReadThroughStore[K, V](backingStore: ReadableStore[K, V], cache: Store[K, V])
  extends ReadableStore[K, V] {

  protected [this] lazy val mutex = new AsyncMutex

  private [this] def getFromBackingStore(k: K) : Future[Option[V]] = {
    // attempt to fetch the key from backing store and
    // write the key to cache, best effort
    backingStore.get(k).flatMap { storeValue =>
      mutex.acquire.flatMap { p =>
        cache.put((k, storeValue))
          .map { u : Unit => storeValue }
          .onFailure { case x: Exception => storeValue }
          .ensure { p.release }
      }
    }
  }

  override def get(k: K): Future[Option[V]] =
    cache.get(k).flatMap { cacheValue =>
      cacheValue match {
        case None => getFromBackingStore(k)
        case some => Future.value(some)
      }
    } onFailure { case x: Exception =>
      getFromBackingStore(k)
    }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    // attempt to read from cache first
    val cacheResults : Map[K1, Future[Either[Option[V], Exception]]] =
      cache.multiGet(ks).map { case (k, f) =>
        (k, f.map { optv => Left(optv) } onFailure { case x: Exception => Right(x) })
      }

    // attempt to read all failed keys and cache misses from backing store
    val f: Future[Map[K1, Option[V]]] =
      FutureOps.mapCollect(cacheResults).flatMap { cacheResult =>
        val failedKeys = cacheResult.filter { _._2.isRight }.keySet
        val responses = cacheResult.filter { _._2.isLeft }.map { case (k, r) => (k, r.left.get) }
        val hits = responses.filter { !_._2.isEmpty }
        val missedKeys = responses.filter { _._2.isEmpty }.keySet

        FutureOps.mapCollect(backingStore.multiGet(missedKeys ++ failedKeys)).flatMap { storeResult =>
          // write fetched keys to cache, best effort
          mutex.acquire.flatMap { p =>
            FutureOps.mapCollect(cache.multiPut(storeResult))(FutureCollector.bestEffort[(K1, Unit)])
              .map { u => hits ++ storeResult }
              .ensure { p.release }
          }
        }
      }
    FutureOps.liftValues(ks, f, { (k: K1) => Future.None })
  }
}

