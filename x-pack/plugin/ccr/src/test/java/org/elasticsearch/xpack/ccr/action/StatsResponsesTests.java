/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatsResponsesTests extends AbstractStreamableTestCase<FollowStatsAction.StatsResponses> {

    @Override
    protected FollowStatsAction.StatsResponses createBlankInstance() {
        return new FollowStatsAction.StatsResponses();
    }

    @Override
    protected FollowStatsAction.StatsResponses createTestInstance() {
        int numResponses = randomIntBetween(0, 8);
        List<FollowStatsAction.StatsResponse> responses = new ArrayList<>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            ShardFollowNodeTaskStatus status = new ShardFollowNodeTaskStatus(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Collections.emptyNavigableMap(),
                randomLong(),
                randomBoolean() ? new ElasticsearchException("fatal error") : null);
            responses.add(new FollowStatsAction.StatsResponse(status));
        }
        return new FollowStatsAction.StatsResponses(Collections.emptyList(), Collections.emptyList(), responses);
    }
}
