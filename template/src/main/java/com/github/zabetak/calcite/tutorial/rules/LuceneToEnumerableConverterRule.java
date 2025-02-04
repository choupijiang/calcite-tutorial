/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zabetak.calcite.tutorial.rules;

import com.github.zabetak.calcite.tutorial.operators.LuceneToEnumerableConverter;
import com.github.zabetak.calcite.tutorial.operators.LuceneRel;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.zabetak.calcite.tutorial.operators.LuceneRel.LUCENE;

/**
 * Planner rule converting any kind of {@link LuceneRel} expression to
 * {@link org.apache.calcite.adapter.enumerable.EnumerableRel} by creating a
 * {@link LuceneToEnumerableConverter}.
 */
public final class LuceneToEnumerableConverterRule extends ConverterRule {
    public LuceneToEnumerableConverterRule(final Config config) {
        super(config);
    }
    // TODO 1. Extend ConverterRule
  // TODO 2. Implement convert method

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        return new LuceneToEnumerableConverter(rel);
    }

    // TODO 3. Create DEFAULT configuration for rule
    public static final Config DEFAULT = Config.INSTANCE
            .withConversion(
                    LuceneRel.class,
                    LUCENE,
                    EnumerableConvention.INSTANCE,
                    "LuceneEnumerableConverterRule")
            .withRuleFactory(LuceneToEnumerableConverterRule::new);
}
