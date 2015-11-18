package it.unibz.krdb.obda.parser;

/*
 * #%L
 * ontop-obdalib-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;


/**
 * Find all the table used for GROUP BY in the statement.
 */

public class AggregationVisitor {

	private static final class AggregationJSQL {
		
	};
	
	
	AggregationJSQL aggregation= new AggregationJSQL();
	
	/**
	 * Return a {@link AggregationJSQL} containing GROUP BY statement
	 * @param select
	 * 			select statement
	 * @return Aggregation visitor
	 */
	public AggregationJSQL getAggregation(Select select, boolean deepParsing){
		if (select.getWithItemsList() != null) {
			for (WithItem withItem : select.getWithItemsList()) {
				withItem.accept(selectVisitor);
			}
		}
		select.getSelectBody().accept(selectVisitor);

		return aggregation;
	}
	

	private SelectVisitor selectVisitor = new SelectVisitor() {
		@Override
		public void visit(PlainSelect plainSelect) {
			if(plainSelect.getGroupByColumnReferences()!=null){
				for(Expression express: plainSelect.getGroupByColumnReferences() ){
					express.accept(expressionVisitor);
				}
			}
		}

		@Override
		public void visit(SetOperationList setOpList) {
			// until now we are not considering the case of UNION statement
			setOpList.getPlainSelects().get(0).accept(this);
		}

		@Override
		public void visit(WithItem withItem) {
			//we are not considering the subquery with WITH
		}
	};

	private ExpressionVisitor expressionVisitor = new ExpressionVisitor() {
		@Override
		public void visit(NullValue nullValue) {

		}

		@Override
		public void visit(Function function) {

		}

		@Override
		public void visit(SignedExpression signedExpression) {

		}

		@Override
		public void visit(JdbcParameter jdbcParameter) {

		}

		@Override
		public void visit(JdbcNamedParameter jdbcNamedParameter) {

		}

		@Override
		public void visit(DoubleValue doubleValue) {

		}

		@Override
		public void visit(LongValue longValue) {

		}

		@Override
		public void visit(DateValue dateValue) {

		}

		@Override
		public void visit(TimeValue timeValue) {

		}

		@Override
		public void visit(TimestampValue timestampValue) {

		}

		@Override
		public void visit(Parenthesis parenthesis) {

		}

		@Override
		public void visit(StringValue stringValue) {

		}

		@Override
		public void visit(Addition addition) {

		}

		@Override
		public void visit(Division division) {

		}

		@Override
		public void visit(Multiplication multiplication) {

		}

		@Override
		public void visit(Subtraction subtraction) {

		}

		@Override
		public void visit(AndExpression andExpression) {

		}

		@Override
		public void visit(OrExpression orExpression) {

		}

		@Override
		public void visit(Between between) {

		}

		@Override
		public void visit(EqualsTo equalsTo) {

		}

		@Override
		public void visit(GreaterThan greaterThan) {

		}

		@Override
		public void visit(GreaterThanEquals greaterThanEquals) {

		}

		@Override
		public void visit(InExpression inExpression) {

		}

		@Override
		public void visit(IsNullExpression isNullExpression) {

		}

		@Override
		public void visit(LikeExpression likeExpression) {

		}

		@Override
		public void visit(MinorThan minorThan) {

		}

		@Override
		public void visit(MinorThanEquals minorThanEquals) {

		}

		@Override
		public void visit(NotEqualsTo notEqualsTo) {

		}

		@Override
		public void visit(Column tableColumn) {

		}

		@Override
		public void visit(SubSelect subSelect) {

		}

		@Override
		public void visit(CaseExpression caseExpression) {

		}

		@Override
		public void visit(WhenClause whenClause) {

		}

		@Override
		public void visit(ExistsExpression existsExpression) {

		}

		@Override
		public void visit(AllComparisonExpression allComparisonExpression) {

		}

		@Override
		public void visit(AnyComparisonExpression anyComparisonExpression) {

		}

		@Override
		public void visit(Concat concat) {

		}

		@Override
		public void visit(Matches matches) {

		}

		@Override
		public void visit(BitwiseAnd bitwiseAnd) {

		}

		@Override
		public void visit(BitwiseOr bitwiseOr) {

		}

		@Override
		public void visit(BitwiseXor bitwiseXor) {

		}

		@Override
		public void visit(CastExpression cast) {

		}

		@Override
		public void visit(Modulo modulo) {

		}

		@Override
		public void visit(AnalyticExpression aexpr) {

		}

		@Override
		public void visit(ExtractExpression eexpr) {

		}

		@Override
		public void visit(IntervalExpression iexpr) {

		}

		@Override
		public void visit(OracleHierarchicalExpression oexpr) {

		}

		@Override
		public void visit(RegExpMatchOperator rexpr) {

		}

		@Override
		public void visit(JsonExpression jsonExpr) {

		}

		@Override
		public void visit(RegExpMySQLOperator regExpMySQLOperator) {

		}
	};



}
