/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Used for enqueueing input records. Queue limit is controlled by {@link #memoryLimit}. Unlike standard bounded queue
 * implementations, this queue bounds the size by memory bytes occupied by its tenants. The standard implementation
 * bounds by the number of entries in the queue.
 *
 * It internally samples every {@link #RECORD_SAMPLING_RATE}th record and adjusts number of records in queue
 * accordingly. This is done to ensure that we don't OOM.
 *
 * This queue supports multiple producer single consumer pattern.
 *
 * @param <I> input payload data type
 * @param <O> output payload data type
 */
public class BoundedInMemoryQueue<I, O> implements Iterable<O> {

  /** Interval used for polling records in the queue. **/
  public static final int RECORD_POLL_INTERVAL_SEC = 1;

  /** Rate used for sampling records to determine avg record size in bytes. **/
  public static final int RECORD_SAMPLING_RATE = 64;

  /** Maximum records that will be cached. **/
  private static final int RECORD_CACHING_LIMIT = 128 * 1024;

  private static final Logger LOG = LogManager.getLogger(BoundedInMemoryQueue.class);

  /**
   * It indicates number of records to cache. We will be using sampled record's average size to
   * determine how many records we should cache and will change (increase/decrease) permits accordingly.
   */
  public final Semaphore rateLimiter = new Semaphore(1);

  /** Used for sampling records with "RECORD_SAMPLING_RATE" frequency. **/
  public final AtomicLong samplingRecordCounter = new AtomicLong(-1);

  /** Internal queue for records. **/
  private final LinkedBlockingQueue<Option<O>> queue = new LinkedBlockingQueue<>();

  /** Maximum amount of memory to be used for queueing records. **/
  private final long memoryLimit;

  /**
   * it holds the root cause of the Throwable in case either queueing records
   * (consuming from inputIterator) fails or thread reading records from queue fails.
   */
  private final AtomicReference<Throwable> hasFailed = new AtomicReference<>(null);

  /** Used for indicating that all the records from queue are read successfully. **/
  private final AtomicBoolean isReadDone = new AtomicBoolean(false);

  /** used for indicating that all records have been enqueued. **/
  private final AtomicBoolean isWriteDone = new AtomicBoolean(false);

  /** Function to transform the input payload to the expected output payload. **/
  private final Function<I, O> transformFunction;

  /** Payload Size Estimator. **/
  private final SizeEstimator<O> payloadSizeEstimator;

  /** Singleton (w.r.t this instance) Iterator for this queue. **/
  private final QueueIterator iterator;

  /**
   * indicates rate limit (number of records to cache). it is updated
   * whenever there is a change in avg record size.
   */
  public int currentRateLimit = 1;

  /** Indicates avg record size in bytes. It is updated whenever a new record is sampled. **/
  public long avgRecordSizeInBytes = 0;

  /** Indicates number of samples collected so far. **/
  private long numSamples = 0;

  /**
   * Construct BoundedInMemoryQueue with default SizeEstimator.
   *
   * @param memoryLimit MemoryLimit in bytes
   * @param transformFunction Transformer Function to convert input payload type to stored payload type
   */
  public BoundedInMemoryQueue(final long memoryLimit, final Function<I, O> transformFunction) {
    this(memoryLimit, transformFunction, new DefaultSizeEstimator() {});
  }

  /**
   * Construct BoundedInMemoryQueue with passed in size estimator.
   *
   * @param memoryLimit MemoryLimit in bytes
   * @param transformFunction Transformer Function to convert input payload type to stored payload type
   * @param payloadSizeEstimator Payload Size Estimator
   */
  public BoundedInMemoryQueue(final long memoryLimit, final Function<I, O> transformFunction,
      final SizeEstimator<O> payloadSizeEstimator) {
    this.memoryLimit = memoryLimit;
    this.transformFunction = transformFunction;
    this.payloadSizeEstimator = payloadSizeEstimator;
    this.iterator = new QueueIterator();
  }

  public int size() {
    return this.queue.size();
  }

  /**
   * Samples records with "RECORD_SAMPLING_RATE" frequency and computes average record size in bytes. It is used for
   * determining how many maximum records to queue. Based on change in avg size it ma increase or decrease available
   * permits.
   *
   * @param payload Payload to size
   */
  private void adjustBufferSizeIfNeeded(final O payload) throws InterruptedException {
    // 首先看是否已经达到采样频率
    if (this.samplingRecordCounter.incrementAndGet() % RECORD_SAMPLING_RATE != 0) {
      return;
    }

    final long recordSizeInBytes = payloadSizeEstimator.sizeEstimate(payload);
    // 然后计算新的记录平均大小和限流速率
    final long newAvgRecordSizeInBytes =
        Math.max(1, (avgRecordSizeInBytes * numSamples + recordSizeInBytes) / (numSamples + 1));
    final int newRateLimit =
        (int) Math.min(RECORD_CACHING_LIMIT, Math.max(1, this.memoryLimit / newAvgRecordSizeInBytes));

    // If there is any change in number of records to cache then we will either release (if it increased) or acquire
    // (if it decreased) to adjust rate limiting to newly computed value.
    // 该操作可根据采样的记录大小动态调节速率，不至于在记录负载太大和记录负载太小时，放入同等个数，从而起到动态调节作用
    // 如果新的限流速率大于当前速率，则可释放一些许可（供阻塞的生产者获取后继续生产）
    if (newRateLimit > currentRateLimit) {
      rateLimiter.release(newRateLimit - currentRateLimit);
    } else if (newRateLimit < currentRateLimit) { // 否则需要获取（回收）一些许可（许可变少后生产速率自然就降低了）
      rateLimiter.acquire(currentRateLimit - newRateLimit);
    }
    currentRateLimit = newRateLimit;
    avgRecordSizeInBytes = newAvgRecordSizeInBytes;
    numSamples++;
  }

  /**
   * Inserts record into queue after applying transformation.
   *
   * @param t Item to be queued
   */
  public void insertRecord(I t) throws Exception {
    // If already closed, throw exception
    if (isWriteDone.get()) {
      throw new IllegalStateException("Queue closed for enqueueing new entries");
    }

    // We need to stop queueing if queue-reader has failed and exited.
    throwExceptionIfFailed();
    // 首先获取一个许可(Semaphore)，未成功获取会被阻塞直至成功获取
    rateLimiter.acquire();
    // We are retrieving insert value in the record queueing thread to offload computation
    // around schema validation
    // and record creation to it.
    final O payload = transformFunction.apply(t);
    adjustBufferSizeIfNeeded(payload);
    queue.put(Option.of(payload));
  }

  /**
   * Checks if records are either available in the queue or expected to be written in future.
   */
  private boolean expectMoreRecords() {
    return !isWriteDone.get() || (isWriteDone.get() && !queue.isEmpty());
  }

  /**
   * Reader interface but never exposed to outside world as this is a single consumer queue. Reading is done through a
   * singleton iterator for this queue.
   */
  private Option<O> readNextRecord() {
    if (this.isReadDone.get()) {
      return Option.empty();
    }
    // 可以看到首先会释放一个许可，然后判断是否还可以读取记录（还在生产或者停止生产但队列不为空都可读取）
    rateLimiter.release();
    Option<O> newRecord = Option.empty();
    while (expectMoreRecords()) {
      try {
        throwExceptionIfFailed();
        // 然后从内部队列获取记录或返回
        newRecord = queue.poll(RECORD_POLL_INTERVAL_SEC, TimeUnit.SECONDS);
        if (newRecord != null) {
          break;
        }
      } catch (InterruptedException e) {
        LOG.error("error reading records from queue", e);
        throw new HoodieException(e);
      }
    }
    // Check one more time here as it is possible producer erred out and closed immediately
    throwExceptionIfFailed();

    if (newRecord != null && newRecord.isPresent()) {
      return newRecord;
    } else {
      // We are done reading all the records from internal iterator.
      this.isReadDone.set(true);
      return Option.empty();
    }
  }

  /**
   * Puts an empty entry to queue to denote termination.
   */
  public void close() {
    // done queueing records notifying queue-reader.
    isWriteDone.set(true);
  }

  private void throwExceptionIfFailed() {
    if (this.hasFailed.get() != null) {
      close();
      throw new HoodieException("operation has failed", this.hasFailed.get());
    }
  }

  /**
   * API to allow producers and consumer to communicate termination due to failure.
   */
  public void markAsFailed(Throwable e) {
    this.hasFailed.set(e);
    // release the permits so that if the queueing thread is waiting for permits then it will
    // get it.
    this.rateLimiter.release(RECORD_CACHING_LIMIT + 1);
  }

  @Override
  public Iterator<O> iterator() {
    return iterator;
  }

  /**
   * Iterator for the memory bounded queue.
   */
  private final class QueueIterator implements Iterator<O> {

    // next record to be read from queue.
    private O nextRecord;

    @Override
    public boolean hasNext() {
      if (this.nextRecord == null) {
        Option<O> res = readNextRecord();
        this.nextRecord = res.orElse(null);
      }
      return this.nextRecord != null;
    }

    @Override
    public O next() {
      ValidationUtils.checkState(hasNext() && this.nextRecord != null);
      final O ret = this.nextRecord;
      this.nextRecord = null;
      return ret;
    }
  }
}
