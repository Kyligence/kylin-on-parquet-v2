/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.kylin.common.util.DateFormat;

import java.io.Serializable;
import java.util.Map;

/**
 * SegmentRange and TSRange seem similar but are different concepts.
 *
 * - SegmentRange defines the range of a segment.
 * - TSRange is the time series range of the segment data.
 * - When segment range is defined by time, the two can be the same, in that case TSRange is a kind of SegmentRange.
 * - Duration segment creation (build/refresh/merge), a new segment is defined by either one of the two, not both.
 * - And the choice must be consistent across all following segment creation.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
abstract public class SegmentRange<T extends Comparable> implements Comparable<SegmentRange>, Serializable {
    private String id;
    protected T start;
    protected T end;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    abstract public boolean isInfinite();

    abstract public boolean contains(SegmentRange o);

    abstract public boolean entireOverlaps(SegmentRange o);

    abstract public boolean overlaps(SegmentRange o);

    abstract public boolean connects(SegmentRange o);

    abstract public boolean apartBefore(SegmentRange o);

    abstract public boolean startStartMatch(SegmentRange o);

    abstract public boolean endEndMatch(SegmentRange o);

    abstract public boolean startEndMatch(SegmentRange o);

    abstract public SegmentRange getStartDeviation(SegmentRange o);

    abstract public SegmentRange getEndDeviation(SegmentRange o);

    abstract public SegmentRange getOverlapRange(SegmentRange o);

    /**
     * create a new SegmentRange which will start from this.start and end at o.end
     * caller should make sure this.start < o.end
     */
    abstract public SegmentRange coverWith(SegmentRange o);

    /**
     * create a new SegmentRange which will start from this.end and end at o.start
     * caller should make sure this.end < o.start
     */
    abstract public SegmentRange gapTill(SegmentRange o);

    public T getStart() {
        return start;
    }

    public T getEnd() {
        return end;
    }


    // ============================================================================

    abstract public static class BasicSegmentRange extends SegmentRange<Long> {

        BasicSegmentRange() {
        }

        BasicSegmentRange(Long s, Long e) {
            this.start = (s == null || s <= 0) ? 0 : s;
            this.end = (e == null || e == Long.MAX_VALUE) ? Long.MAX_VALUE : e;

            Preconditions.checkState(this.start <= this.end);
        }

        private BasicSegmentRange convert(SegmentRange o) {
            Preconditions.checkState(o instanceof BasicSegmentRange);
            return (BasicSegmentRange) o;
        }

        private void checkSameType(SegmentRange o) {
            Preconditions.checkNotNull(o);
            Preconditions.checkState(getClass() == o.getClass());
        }

        @Override
        public boolean isInfinite() {
            return start == 0 && end == Long.MAX_VALUE;
        }

        @Override
        public boolean contains(SegmentRange o) {
            checkSameType(o);
            BasicSegmentRange t = convert(o);
            return this.start <= t.start && t.end <= this.end;
        }

        @Override
        public boolean entireOverlaps(SegmentRange o) {
            checkSameType(o);
            return this.contains(o) && o.contains(this);
        }

        @Override
        public boolean overlaps(SegmentRange o) {
            checkSameType(o);
            BasicSegmentRange t = convert(o);
            return this.start < t.end && t.start < this.end;
        }

        @Override
        public boolean connects(SegmentRange o) {
            checkSameType(o);
            BasicSegmentRange t = convert(o);
            return this.end.equals(t.start);
        }

        @Override
        public boolean apartBefore(SegmentRange o) {
            checkSameType(o);
            BasicSegmentRange t = convert(o);
            return this.end < t.start;
        }

        @Override
        public boolean startStartMatch(SegmentRange o) {
            checkSameType(o);
            BasicSegmentRange t = convert(o);
            return this.start.equals(t.start);
        }

        @Override
        public boolean endEndMatch(SegmentRange o) {
            checkSameType(o);
            BasicSegmentRange t = convert(o);
            return this.end.equals(t.end);
        }

        @Override
        public boolean startEndMatch(SegmentRange o) {
            checkSameType(o);
            BasicSegmentRange t = convert(o);
            return this.start.equals(t.end);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "[" + start + "," + end + ")";
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(start, end);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            BasicSegmentRange that = (BasicSegmentRange) o;
            return start.equals(that.start) && end.equals(that.end);
        }

        @Override
        public int compareTo(SegmentRange o) {
            BasicSegmentRange t = convert(o);

            int comp = Long.compare(this.start, t.start);
            if (comp != 0)
                return comp;

            return Long.compare(this.end, t.end);
        }
    }

    //TimePartitionedSegmentRange simply treat the long typed start and end as ms
    public static class TimePartitionedSegmentRange extends BasicSegmentRange {
        public TimePartitionedSegmentRange() {
            super();
        }

        public TimePartitionedSegmentRange(Long startMs, Long endMs) {
            super(startMs, endMs);
        }

        public TimePartitionedSegmentRange(String startDate, String endDate) {
            super(dateToLong(startDate), dateToLong(endDate));
        }

        public static TimePartitionedSegmentRange createInfinite() {
            return new TimePartitionedSegmentRange(0L, Long.MAX_VALUE);
        }

        private TimePartitionedSegmentRange convertToTimePartitioned(SegmentRange o) {
            Preconditions.checkState(o instanceof TimePartitionedSegmentRange);
            return (TimePartitionedSegmentRange) o;
        }

        @Override
        public SegmentRange coverWith(SegmentRange o) {
            TimePartitionedSegmentRange other = convertToTimePartitioned(o);
            return new TimePartitionedSegmentRange(this.start, other.end);
        }

        @Override
        public SegmentRange getStartDeviation(SegmentRange o) {
            TimePartitionedSegmentRange other = convertToTimePartitioned(o);
            return new TimePartitionedSegmentRange(this.start, other.start);
        }

        @Override
        public SegmentRange getEndDeviation(SegmentRange o) {
            TimePartitionedSegmentRange other = convertToTimePartitioned(o);
            return new TimePartitionedSegmentRange(this.end, other.end);
        }

        @Override
        public SegmentRange getOverlapRange(SegmentRange o) {
            TimePartitionedSegmentRange other = convertToTimePartitioned(o);
            if (!this.overlaps(o)) {
                return null;
            }
            Long start = this.start < other.start ? other.start : this.start;
            Long end = this.end < other.end ? this.end : other.end;
            return new TimePartitionedSegmentRange(start, end);
        }

        @Override
        public SegmentRange gapTill(SegmentRange o) {
            TimePartitionedSegmentRange other = convertToTimePartitioned(o);
            return new TimePartitionedSegmentRange(this.end, other.start);
        }

        @JsonProperty("date_range_start")
        public Long getStart() {
            return start;
        }

        @JsonProperty("date_range_start")
        public void setStart(Long start) {
            this.start = start;
        }

        @Override
        @JsonProperty("date_range_end")
        public Long getEnd() {
            return end;
        }

        @JsonProperty("date_range_end")
        public void setEnd(Long end) {
            this.end = end;
        }

    }

    public static class KafkaOffsetPartitionedSegmentRange extends BasicSegmentRange {

        @JsonProperty("source_partition_offset_start")
        private Map<Integer, Long> sourcePartitionOffsetStart;

        @JsonProperty("source_partition_offset_end")
        private Map<Integer, Long> sourcePartitionOffsetEnd;

        public static KafkaOffsetPartitionedSegmentRange createInfinite() {
            return new KafkaOffsetPartitionedSegmentRange(0L, Long.MAX_VALUE, null, null);
        }

        public KafkaOffsetPartitionedSegmentRange() {
        }

        public KafkaOffsetPartitionedSegmentRange(Long startOffset, Long endOffset,
                Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd) {
            super(startOffset, endOffset);
            this.sourcePartitionOffsetStart = sourcePartitionOffsetStart == null ? Maps.<Integer, Long> newHashMap()
                    : sourcePartitionOffsetStart;
            this.sourcePartitionOffsetEnd = sourcePartitionOffsetEnd == null ? Maps.<Integer, Long> newHashMap()
                    : sourcePartitionOffsetEnd;
        }

        private KafkaOffsetPartitionedSegmentRange convertToKafkaOffset(SegmentRange o) {
            Preconditions.checkState(o instanceof KafkaOffsetPartitionedSegmentRange);
            return (KafkaOffsetPartitionedSegmentRange) o;
        }

        @Override
        public SegmentRange coverWith(SegmentRange o) {
            KafkaOffsetPartitionedSegmentRange other = convertToKafkaOffset(o);
            return new KafkaOffsetPartitionedSegmentRange(this.start, other.end, this.getSourcePartitionOffsetStart(),
                    other.getSourcePartitionOffsetEnd());
        }

        @Override
        public SegmentRange gapTill(SegmentRange o) {
            KafkaOffsetPartitionedSegmentRange other = convertToKafkaOffset(o);
            return new KafkaOffsetPartitionedSegmentRange(this.end, other.start, this.getSourcePartitionOffsetEnd(),
                    other.getSourcePartitionOffsetStart());
        }

        @Override
        public SegmentRange getStartDeviation(SegmentRange o) {
            KafkaOffsetPartitionedSegmentRange other = convertToKafkaOffset(o);
            return new KafkaOffsetPartitionedSegmentRange(this.start, other.start, this.getSourcePartitionOffsetStart(),
                    other.getSourcePartitionOffsetStart());
        }

        @Override
        public SegmentRange getEndDeviation(SegmentRange o) {
            KafkaOffsetPartitionedSegmentRange other = convertToKafkaOffset(o);
            return new KafkaOffsetPartitionedSegmentRange(this.end, other.end, this.getSourcePartitionOffsetEnd(),
                    other.getSourcePartitionOffsetEnd());
        }

        @Override
        public SegmentRange getOverlapRange(SegmentRange o) {
            KafkaOffsetPartitionedSegmentRange other = convertToKafkaOffset(o);
            if (!this.overlaps(o)) {
                return null;
            }
            Long start = this.start < other.start ? other.start : this.start;
            Long end = this.end < other.end ? this.start : other.start;
            return new TimePartitionedSegmentRange(start, end);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            if (!super.equals(o))
                return false;
            KafkaOffsetPartitionedSegmentRange that = (KafkaOffsetPartitionedSegmentRange) o;
            return java.util.Objects.equals(sourcePartitionOffsetStart, that.sourcePartitionOffsetStart)
                    && java.util.Objects.equals(sourcePartitionOffsetEnd, that.sourcePartitionOffsetEnd);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(super.hashCode(), sourcePartitionOffsetStart, sourcePartitionOffsetEnd);
        }

        public Map<Integer, Long> getSourcePartitionOffsetStart() {
            return sourcePartitionOffsetStart;
        }

        public Map<Integer, Long> getSourcePartitionOffsetEnd() {
            return sourcePartitionOffsetEnd;
        }

        @JsonProperty("source_offset_start")
        public Long getStart() {
            return start;
        }

        @JsonProperty("source_offset_start")
        public void setStart(Long start) {
            this.start = start;
        }

        @JsonProperty("source_offset_end")
        public Long getEnd() {
            return end;
        }

        @JsonProperty("source_offset_end")
        public void setEnd(Long end) {
            this.end = end;
        }
    }

    public static Long dateToLong(String dateString) {
        return DateFormat.stringToMillis(dateString);
    }

    public static void main(String[] args) {
        //        System.out.println();
        //        TimePartitionedSegmentRange t = new TimePartitionedSegmentRange(100L, 200L);
        //        String s1 = JsonUtil.writeValueAsIndentString(t);
        //        TimePartitionedSegmentRange timePartitionedSegmentRange = JsonUtil.readValue(s1,
        //                TimePartitionedSegmentRange.class);
        //
        //        Map<Integer, Long> x = Maps.newHashMap();
        //        x.put(10, 100L);
        //        KafkaOffsetPartitionedSegmentRange k = new KafkaOffsetPartitionedSegmentRange(null, Long.MAX_VALUE, null, null);
        //        String s2 = JsonUtil.writeValueAsIndentString(k);
        //        KafkaOffsetPartitionedSegmentRange kafkaOffsetPartitionedSegmentRange = JsonUtil.readValue(s2,
        //                KafkaOffsetPartitionedSegmentRange.class);
        //
        //        System.out.println(s1);
        //
        //        System.out.println();

    }

}
