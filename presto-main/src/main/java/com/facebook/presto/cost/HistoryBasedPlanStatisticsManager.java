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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.statistics.EmptyPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.CachingPlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.PlanCanonicalInfoProvider;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getHistoryOptimizationPlanCanonicalizationStrategies;
import static java.util.Objects.requireNonNull;

public class HistoryBasedPlanStatisticsManager
{
    private final SessionPropertyManager sessionPropertyManager;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;
    private final HistoryBasedOptimizationConfig config;

    private HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider = EmptyPlanStatisticsProvider.getInstance();
    private boolean statisticsProviderAdded;
    private final boolean isNativeExecution;
    private final String serverVersion;

    @Inject
    public HistoryBasedPlanStatisticsManager(ObjectMapper objectMapper, SessionPropertyManager sessionPropertyManager, Metadata metadata, HistoryBasedOptimizationConfig config,
            FeaturesConfig featuresConfig, NodeVersion nodeVersion)
    {
        requireNonNull(objectMapper, "objectMapper is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.historyBasedStatisticsCacheManager = new HistoryBasedStatisticsCacheManager();
        ObjectMapper newObjectMapper = objectMapper.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        this.planCanonicalInfoProvider = new CachingPlanCanonicalInfoProvider(historyBasedStatisticsCacheManager, newObjectMapper, metadata);
        this.config = requireNonNull(config, "config is null");
        this.isNativeExecution = featuresConfig.isNativeExecutionEnabled();
        this.serverVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
    }

    public void addHistoryBasedPlanStatisticsProviderFactory(HistoryBasedPlanStatisticsProvider historyBasedPlanStatisticsProvider)
    {
        if (statisticsProviderAdded) {
            throw new IllegalStateException("historyBasedPlanStatisticsProvider can only be set once");
        }
        this.historyBasedPlanStatisticsProvider = historyBasedPlanStatisticsProvider;
        statisticsProviderAdded = true;
    }

    public HistoryBasedPlanStatisticsCalculator getHistoryBasedPlanStatisticsCalculator(StatsCalculator delegate)
    {
        return new HistoryBasedPlanStatisticsCalculator(() -> historyBasedPlanStatisticsProvider, historyBasedStatisticsCacheManager, delegate, planCanonicalInfoProvider);
    }

    public HistoryBasedPlanStatisticsTracker getHistoryBasedPlanStatisticsTracker()
    {
        return new HistoryBasedPlanStatisticsTracker(() -> historyBasedPlanStatisticsProvider, historyBasedStatisticsCacheManager, sessionPropertyManager, config, isNativeExecution, serverVersion);
    }

    public PlanCanonicalInfoProvider getPlanCanonicalInfoProvider()
    {
        return planCanonicalInfoProvider;
    }

    public static List<PlanCanonicalizationStrategy> historyBasedPlanCanonicalizationStrategyList(Session session)
    {
        return getHistoryOptimizationPlanCanonicalizationStrategies(session);
    }
}
