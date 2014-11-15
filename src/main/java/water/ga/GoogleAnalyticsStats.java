/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package water.ga;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects the basic stats about successful events that have been posted to GA.
 *
 * @author Santhosh Kumar
 *
 * This copy of google-analytics-java is a back port of version 1.1.1 of the library.
 * This backport removes the slf4j dependency, and modifies the code to work with the
 * 4.1 version of the Apache http client library.
 *
 * Original sources can be found at https://github.com/brsanthu/google-analytics-java.
 * All copyrights retained by original authors.
 */
public class GoogleAnalyticsStats {
  private AtomicLong pageViewHits = new AtomicLong();
  private AtomicLong eventHits = new AtomicLong();
  private AtomicLong appViewHits = new AtomicLong();
  private AtomicLong itemHits = new AtomicLong();
  private AtomicLong transactionHits = new AtomicLong();
  private AtomicLong timingHits = new AtomicLong();
  private AtomicLong socialHits = new AtomicLong();

  void pageViewHit() {
    pageViewHits.incrementAndGet();
  }

  void eventHit() {
    eventHits.incrementAndGet();
  }

  void appViewHit() {
    appViewHits.incrementAndGet();
  }

  void itemHit() {
    itemHits.incrementAndGet();
  }

  void transactionHit() {
    transactionHits.incrementAndGet();
  }

  void socialHit() {
    socialHits.incrementAndGet();
  }

  void timingHit() {
    timingHits.incrementAndGet();
  }

  public long getPageViewHits () {
    return pageViewHits.get();
  }

  public long getEventHits () {
    return eventHits.get();
  }

  public long getAppViewHits () {
    return appViewHits.get();
  }

  public long getItemHits () {
    return itemHits.get();
  }

  public long getTransactionHits () {
    return transactionHits.get();
  }

  public long getTimingHits () {
    return timingHits.get();
  }

  public long getSocialHits () {
    return socialHits.get();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("GoogleAnalyticsStats [");
    if (pageViewHits != null) {
      builder.append("pageViewHits=");
      builder.append(pageViewHits);
      builder.append(", ");
    }
    if (eventHits != null) {
      builder.append("eventHits=");
      builder.append(eventHits);
      builder.append(", ");
    }
    if (appViewHits != null) {
      builder.append("appViewHits=");
      builder.append(appViewHits);
      builder.append(", ");
    }
    if (itemHits != null) {
      builder.append("itemHits=");
      builder.append(itemHits);
      builder.append(", ");
    }
    if (transactionHits != null) {
      builder.append("transactionHits=");
      builder.append(transactionHits);
      builder.append(", ");
    }
    if (timingHits != null) {
      builder.append("timingHits=");
      builder.append(timingHits);
      builder.append(", ");
    }
    if (socialHits != null) {
      builder.append("socialHits=");
      builder.append(socialHits);
    }
    builder.append("]");
    return builder.toString();
  }
}
