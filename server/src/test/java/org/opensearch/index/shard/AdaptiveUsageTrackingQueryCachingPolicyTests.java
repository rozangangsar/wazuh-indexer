/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.shard;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AdaptiveUsageTrackingQueryCachingPolicyTests extends OpenSearchTestCase {

    public void testComplexQueriesNeedHigherFrequency() {
        ExposedAdaptivePolicy adaptive = new ExposedAdaptivePolicy();

        BooleanQuery.Builder smallBoolean = new BooleanQuery.Builder();
        smallBoolean.add(new TermQuery(new Term("name", "a")), BooleanClause.Occur.FILTER);
        smallBoolean.add(new TermQuery(new Term("name", "b")), BooleanClause.Occur.FILTER);

        BooleanQuery.Builder mediumBoolean = new BooleanQuery.Builder();
        for (int i = 0; i < 4; i++) {
            mediumBoolean.add(new TermQuery(new Term("name", "m" + i)), BooleanClause.Occur.SHOULD);
        }

        BooleanQuery.Builder largeBoolean = new BooleanQuery.Builder();
        for (int i = 0; i < 8; i++) {
            largeBoolean.add(new TermQuery(new Term("name", "l" + i)), BooleanClause.Occur.SHOULD);
        }

        int small = adaptive.freq(smallBoolean.build());
        int medium = adaptive.freq(mediumBoolean.build());
        int large = adaptive.freq(largeBoolean.build());

        assertTrue(medium > small);
        assertTrue(large > medium);
    }

    public void testSimpleQueriesKeepBaselineBehavior() {
        ExposedAdaptivePolicy adaptive = new ExposedAdaptivePolicy();
        ExposedBaselinePolicy baseline = new ExposedBaselinePolicy();
        Query termQuery = new TermQuery(new Term("name", "x"));
        assertEquals(baseline.freq(termQuery), adaptive.freq(termQuery));
    }

    public void testDisjunctionComplexityRaisesThreshold() {
        ExposedAdaptivePolicy adaptive = new ExposedAdaptivePolicy();

        DisjunctionMaxQuery small = new DisjunctionMaxQuery(
            List.of(new TermQuery(new Term("name", "x")), new TermQuery(new Term("name", "y"))),
            0.1f
        );
        DisjunctionMaxQuery large = new DisjunctionMaxQuery(
            List.of(
                new TermQuery(new Term("name", "d1")),
                new TermQuery(new Term("name", "d2")),
                new TermQuery(new Term("name", "d3")),
                new TermQuery(new Term("name", "d4")),
                new TermQuery(new Term("name", "d5")),
                new TermQuery(new Term("name", "d6"))
            ),
            0.1f
        );

        assertTrue(adaptive.freq(large) > adaptive.freq(small));
    }

    public void testVeryLargeQueriesAreMostSelective() {
        ExposedAdaptivePolicy adaptive = new ExposedAdaptivePolicy();

        BooleanQuery.Builder largeBoolean = new BooleanQuery.Builder();
        for (int i = 0; i < 8; i++) {
            largeBoolean.add(new TermQuery(new Term("name", "lb" + i)), BooleanClause.Occur.SHOULD);
        }

        BooleanQuery.Builder veryLargeBoolean = new BooleanQuery.Builder();
        for (int i = 0; i < 16; i++) {
            veryLargeBoolean.add(new TermQuery(new Term("name", "vb" + i)), BooleanClause.Occur.SHOULD);
        }

        DisjunctionMaxQuery largeDisjunction = new DisjunctionMaxQuery(
            List.of(
                new TermQuery(new Term("name", "d1")),
                new TermQuery(new Term("name", "d2")),
                new TermQuery(new Term("name", "d3")),
                new TermQuery(new Term("name", "d4")),
                new TermQuery(new Term("name", "d5")),
                new TermQuery(new Term("name", "d6"))
            ),
            0.1f
        );

        DisjunctionMaxQuery veryLargeDisjunction = new DisjunctionMaxQuery(
            List.of(
                new TermQuery(new Term("name", "e1")),
                new TermQuery(new Term("name", "e2")),
                new TermQuery(new Term("name", "e3")),
                new TermQuery(new Term("name", "e4")),
                new TermQuery(new Term("name", "e5")),
                new TermQuery(new Term("name", "e6")),
                new TermQuery(new Term("name", "e7")),
                new TermQuery(new Term("name", "e8")),
                new TermQuery(new Term("name", "e9")),
                new TermQuery(new Term("name", "e10"))
            ),
            0.1f
        );

        assertTrue(adaptive.freq(veryLargeBoolean.build()) > adaptive.freq(largeBoolean.build()));
        assertTrue(adaptive.freq(veryLargeDisjunction) > adaptive.freq(largeDisjunction));
    }

    private static final class ExposedAdaptivePolicy extends IndexShard.AdaptiveUsageTrackingQueryCachingPolicy {
        int freq(Query query) {
            return minFrequencyToCache(query);
        }
    }

    private static final class ExposedBaselinePolicy extends UsageTrackingQueryCachingPolicy {
        int freq(Query query) {
            return minFrequencyToCache(query);
        }
    }
}
