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
package com.github.zabetak.calcite.tutorial.operators;

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.search.Query;

/**
 * Translate row expressions ({@link org.apache.calcite.rex.RexNode}) to Apache Lucene queries
 * ({@link Query}).
 *
 * The translator assumes the expression is in some normalized form. At its current state it cannot
 * translate arbitrary Calcite expressions.
 */
public final class RexToLuceneTranslator extends RexVisitorImpl<Query> {
  // TODO 1. Extend RexVisitorImpl<Query>
  private final Filter filter;

  private RexToLuceneTranslator(Filter filter) {
    super(false);
    this.filter = filter;
  }

  // TODO 2. Override and implement visitCall
  @Override
  public Query visitCall(RexCall call) {
    switch (call.getKind()) {
      // TODO 3. Ensure call.getKind() corresponds to equals operator
      case EQUALS:
        // TODO 4. Obtain input reference from left operand (0)
        RexInputRef colRef = (RexInputRef) call.operands.get(0);
        // TODO 5. Obtain literal from right operand (1)
        RexLiteral literal = (RexLiteral) call.operands.get(1);
        // TODO 6. Obtain RelMetadataQuery from cluster
        RelMetadataQuery relMetadataQuery = filter.getCluster().getMetadataQuery();
        // TODO 7. Use RelMetadataQuery#getExpressionLineage on filter.getInput() to find the table column
        // TODO 8. Cast result to RexTableInputRef
        RexTableInputRef col = (RexTableInputRef)relMetadataQuery.getExpressionLineage(filter.getInput(), colRef)
                .stream().findFirst().get();

        // TODO 9. Obtain RelDataTypeField, which has the column name, using the information in RelTableInputRef
        RelDataTypeField typeField = col.getTableRef().getTable()
                .getRowType()
                .getFieldList()
                .get(col.getTableRef().getEntityNumber());

        // TODO 10. Check the data type is the INTEGER and create the appropriate Lucene  query (use Lucene's IntPoint class)
        if (typeField.getType().getSqlTypeName().equals(SqlTypeName.INTEGER)) {
          return IntPoint.newExactQuery(typeField.getName(), literal.getValueAs(Integer.class));
        }
    }
    throw new AssertionError("Expression " + call + " cannot be translated to Lucene query");
  }





  //
  // TODO 11. Throw assertion/exception if it happens to encounter an expression we cannot handle


  /**
   * Translates the condition in the specified filter to a Lucene query.
   */
  public static Query translate(Filter filter) {
    RexToLuceneTranslator translator = new RexToLuceneTranslator(filter);
    throw new AssertionError("Not implemented yet");
    // TODO 12. Remove the assertion error and aply the translator to the filter condition.
  }
}
