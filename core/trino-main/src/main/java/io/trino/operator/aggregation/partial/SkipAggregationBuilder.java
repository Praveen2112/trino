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
package io.trino.operator.aggregation.partial;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.AggregationMetrics;
import io.trino.operator.CompletedWork;
import io.trino.operator.FlatHashStrategyCompiler;
import io.trino.operator.GroupByHash;
import io.trino.operator.OperatorContext;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.operator.aggregation.builder.HashAggregationBuilder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.datasketches.cpc.CpcSketch;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.GroupByHash.createGroupByHash;
import static java.util.Objects.requireNonNull;

/**
 * {@link HashAggregationBuilder} that does not aggregate input rows at all.
 * It passes the input pages, augmented with initial accumulator state to the output.
 * It can only be used at the partial aggregation step as it relies on rows be aggregated at the final step.
 */
public class SkipAggregationBuilder
        implements HashAggregationBuilder
{
    private final Optional<PartialAggregationOutputProcessor> partialAggregationOutputProcessor;
    private final LocalMemoryContext memoryContext;
    private final GroupByHash groupByHash;
    private final List<AggregatorFactory> aggregatorFactories;
    private final AggregationMetrics aggregationMetrics;
    @Nullable
    private Page currentPage;
    private final int[] hashChannels;
    private final HyperLogLog hyperLogLog;
    private final CpcSketch cpcSketch;

    public SkipAggregationBuilder(
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> inputHashChannel,
            List<AggregatorFactory> aggregatorFactories,
            OperatorContext operatorContext,
            LocalMemoryContext memoryContext,
            FlatHashStrategyCompiler hashStrategyCompiler,
            HyperLogLog hyperLogLog,
            CpcSketch cpcSketch,
            Optional<PartialAggregationOutputProcessor> partialAggregationOutputProcessor,
            AggregationMetrics aggregationMetrics)
    {
        this.groupByHash = createGroupByHash(
                operatorContext.getSession(),
                groupByTypes,
                inputHashChannel.isPresent(),
                expectedGroups,
                hashStrategyCompiler,
                UpdateMemory.NOOP);
        this.partialAggregationOutputProcessor = requireNonNull(partialAggregationOutputProcessor, "partialAggregationOutputProcessor is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.aggregatorFactories = ImmutableList.copyOf(requireNonNull(aggregatorFactories, "aggregatorFactories is null"));
        this.hashChannels = new int[groupByChannels.size() + (inputHashChannel.isPresent() ? 1 : 0)];
        for (int i = 0; i < groupByChannels.size(); i++) {
            hashChannels[i] = groupByChannels.get(i);
        }
        inputHashChannel.ifPresent(channelIndex -> hashChannels[groupByChannels.size()] = channelIndex);
        this.aggregationMetrics = requireNonNull(aggregationMetrics, "aggregationMetrics is null");
        this.hyperLogLog = requireNonNull(hyperLogLog, "hyperLogLog is null");
        this.cpcSketch = requireNonNull(cpcSketch, "cpcSketch is null");
    }

    @Override
    public Work<?> processPage(Page page)
    {
        checkArgument(currentPage == null);
        currentPage = page;
        return new CompletedWork<>();
    }

    @Override
    public WorkProcessor<HashOutput> buildResult()
    {
        if (currentPage == null) {
            return WorkProcessor.of();
        }

        Page result = partialAggregationOutputProcessor.map(processor -> processor.processRawInputPage(currentPage)).orElse(currentPage);
        //long uniqueValueCount = groupByHash.getApproximateDistinctValue(currentPage.getLoadedPage(this.hashChannels));
        long start = System.nanoTime();
        groupByHash.populateHash(currentPage.getLoadedPage(this.hashChannels), cpcSketch);
        aggregationMetrics.recordHyperLogLogUpdateTimeSince(start);
        //System.out.println("Block " + currentPage.getPositionCount() + " " + groupByHash.getGroupCount());
        currentPage = null;
        return WorkProcessor.of(new HashOutput(result, result.getPositionCount()));
    }

    @Override
    public boolean isFull()
    {
        return currentPage != null;
    }

    @Override
    public void updateMemory()
    {
        if (currentPage != null) {
            memoryContext.setBytes(currentPage.getSizeInBytes());
        }
    }

    @Override
    public void close() {}

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        throw new UnsupportedOperationException("startMemoryRevoke not supported for SkipAggregationBuilder");
    }

    @Override
    public void finishMemoryRevoke()
    {
        throw new UnsupportedOperationException("finishMemoryRevoke not supported for SkipAggregationBuilder");
    }

    private Page buildOutputPage(Page page)
    {
        // Prefix the output with the hash channels
        Block[] outputBlocks = new Block[hashChannels.length + aggregatorFactories.size()];
        for (int i = 0; i < hashChannels.length; i++) {
            outputBlocks[i] = page.getBlock(hashChannels[i]);
        }

        // Create a unique groupId mapping per row
        int positionCount = page.getPositionCount();
        int[] groupIds = new int[positionCount];
        for (int position = 0; position < positionCount; position++) {
            groupIds[position] = position;
        }

        // Evaluate each grouped aggregator into its own output block
        for (int i = 0; i < aggregatorFactories.size(); i++) {
            GroupedAggregator groupedAggregator = aggregatorFactories.get(i).createGroupedAggregator(aggregationMetrics);
            groupedAggregator.processPage(positionCount, groupIds, page);
            BlockBuilder outputBuilder = groupedAggregator.getType().createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                groupedAggregator.evaluate(position, outputBuilder);
            }
            groupedAggregator = null; // ensure the groupedAggregator is eligible for GC
            outputBlocks[hashChannels.length + i] = outputBuilder.build();
        }

        return new Page(positionCount, outputBlocks);
    }
}
