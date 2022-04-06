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
package io.trino.operator.aggregation;

import io.trino.operator.AggregationMetrics;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GroupedAggregator
        extends AbstractAggregator
{
    private final GroupedAccumulator accumulator;
    private AggregationNode.Step step;
    private final Type intermediateType;
    private final Type finalType;

    public GroupedAggregator(
            GroupedAccumulator accumulator,
            Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> rawInputChannels,
            OptionalInt intermediateStateChannel,
            OptionalInt rawInputMaskChannel,
            OptionalInt maskChannel,
            AggregationMaskBuilder maskBuilder,
            AggregationMetrics metrics)
    {
        super(step, intermediateType, finalType, rawInputChannels, intermediateStateChannel, rawInputMaskChannel, maskChannel, maskBuilder, metrics);
        this.accumulator = requireNonNull(accumulator, "accumulator is null");
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        checkArgument(step.isInputRaw() || rawInputChannels.size() == 1, "expected 1 input channel for intermediate aggregation");
    }

    public long getEstimatedSize()
    {
        return accumulator.getEstimatedSize();
    }

    public Type getType()
    {
        if (step.isOutputPartial()) {
            return intermediateType;
        }
        return finalType;
    }

    public void processPage(int groupCount, int[] groupIds, Page page)
    {
        accumulator.setGroupCount(groupCount);

        processPage(page, new AccumulatorWrapper()
        {
            @Override
            public void addInput(Page page, AggregationMask aggregationMask)
            {
                accumulator.addInput(groupIds, page, aggregationMask);
            }

            @Override
            public void addIntermediate(Block block)
            {
                accumulator.addIntermediate(groupIds, block);
            }

            @Override
            public void addIntermediate(IntArrayList positions, Block intermediateStateBlock)
            {
                if (positions.size() != intermediateStateBlock.getPositionCount()) {
                    // some rows were eliminated by the filter
                    intermediateStateBlock = intermediateStateBlock.getPositions(positions.elements(), 0, positions.size());
                }

                accumulator.addIntermediate(groupIds, intermediateStateBlock);
            }
        });
    }

    public void prepareFinal()
    {
        accumulator.prepareFinal();
    }

    public void evaluate(int groupId, BlockBuilder output)
    {
        if (isOutputPartial()) {
            accumulator.evaluateIntermediate(groupId, output);
        }
        else {
            accumulator.evaluateFinal(groupId, output);
        }
    }
}
