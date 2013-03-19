/*
 * Copyright 2013 Twitter Inc.
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

package com.twitter.storehaus.algebra

import com.twitter.algebird.StatefulSummer
import com.twitter.util.Future
import com.twitter.storehaus.{ FutureOps, FutureCollector, MergeableStore }

class BufferingStore[-K, V](store: MergeableStore[K, V], summer: StatefulSummer[Map[K, V]])
  extends BufferingMergeable[K, V](store, summer)
  with MergeableStore[K, V] {
  protected implicit val collector = FutureCollector.bestEffort[(Any, Unit)]

  override def get(k: K): Future[Option[V]] =
    summer.flush
      .flatMap { mergeAndFulfill(_).get(k) }
      .getOrElse(Future.Unit)
      .flatMap { _ => store.get(k) }

  override def multiGet[K1 <: K](ks: Set[K1]): Map[K1, Future[Option[V]]] = {
    val writeComputation: Future[Unit] =
      summer.flush.map { m =>
        FutureOps.mapCollect {
          mergeAndFulfill(m).filterKeys(ks.toSet[K])
        }.unit
      }.getOrElse(Future.Unit)
    FutureOps.liftFutureValues(ks, writeComputation.map { _ => store.multiGet(ks) })
  }
  override def put(pair: (K, Option[V])) = flush.flatMap { _ => store.put(pair) }

  override def multiPut[K1 <: K](kvs: Map[K1, Option[V]]) =
    FutureOps.liftFutureValues(kvs.keySet, flush.map { _ => store.multiPut(kvs) })
}
