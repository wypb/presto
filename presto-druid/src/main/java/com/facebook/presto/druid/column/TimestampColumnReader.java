/*
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
package com.facebook.presto.druid.column;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import org.apache.druid.segment.ColumnValueSelector;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TimestampColumnReader
        implements ColumnReader
{
    private final ColumnValueSelector<Long> valueSelector;

    public TimestampColumnReader(ColumnValueSelector valueSelector)
    {
        this.valueSelector = requireNonNull(valueSelector, "value selector is null");
    }

    @Override
    public Block readBlock(Type type, int batchSize)
    {
        checkArgument(type == TIMESTAMP);
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            type.writeLong(builder, valueSelector.getLong());
        }

        return builder.build();
    }
}