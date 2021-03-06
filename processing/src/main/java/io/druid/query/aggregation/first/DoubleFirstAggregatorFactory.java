/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.first;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.metamx.common.StringUtils;
import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class DoubleFirstAggregatorFactory extends AggregatorFactory
{
  public static final Comparator VALUE_COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o1, Object o2)
    {
      return Doubles.compare(((SerializablePair<Long, Double>) o1).rhs, ((SerializablePair<Long, Double>) o2).rhs);
    }
  };

  public static final Comparator TIME_COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o1, Object o2)
    {
      return Longs.compare(((SerializablePair<Long, Object>) o1).lhs, ((SerializablePair<Long, Object>) o2).lhs);
    }
  };

  private static final byte CACHE_TYPE_ID = 16;

  private final String fieldName;
  private final String name;

  @JsonCreator
  public DoubleFirstAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DoubleFirstAggregator(
        name,
        metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
        metricFactory.makeFloatColumnSelector(fieldName)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DoubleFirstBufferAggregator(
        metricFactory.makeLongColumnSelector(Column.TIME_COLUMN_NAME),
        metricFactory.makeFloatColumnSelector(fieldName)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return TIME_COMPARATOR.compare(lhs, rhs) <= 0 ? lhs : rhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleFirstAggregatorFactory(name, name)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        return new DoubleFirstAggregator(name, null, null)
        {
          @Override
          public void aggregate()
          {
            SerializablePair<Long, Double> pair = (SerializablePair<Long, Double>) selector.get();
            if (pair.lhs < firstTime) {
              firstTime = pair.lhs;
              firstValue = pair.rhs;
            }
          }
        };
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
      {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        return new DoubleFirstBufferAggregator(null, null)
        {
          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            SerializablePair<Long, Double> pair = (SerializablePair<Long, Double>) selector.get();
            long firstTime = buf.getLong(position);
            if (pair.lhs < firstTime) {
              buf.putLong(position, pair.lhs);
              buf.putDouble(position + Longs.BYTES, pair.rhs);
            }
          }
        };
      }
    };
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new DoubleFirstAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    Map map = (Map) object;
    return new SerializablePair<>(((Number) map.get("lhs")).longValue(), ((Number) map.get("rhs")).doubleValue());
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((SerializablePair<Long, Double>) object).rhs;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(Column.TIME_COLUMN_NAME, fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);

    return ByteBuffer.allocate(2 + fieldNameBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put((byte)0xff)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES + Doubles.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DoubleFirstAggregatorFactory that = (DoubleFirstAggregatorFactory) o;

    return fieldName.equals(that.fieldName) && name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "DoubleFirstAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
