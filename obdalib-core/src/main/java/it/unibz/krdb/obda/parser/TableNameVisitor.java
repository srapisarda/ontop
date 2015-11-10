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

import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.RelationID;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * Collects all table names in a select statement 
 * (except the names introduced in WITH clauses)
 * 
 */

public class TableNameVisitor {

	// Store the table selected by the SQL query in TableJSQL
	private final Map<RelationID, RelationID> tables = new HashMap<>();
	
	private final QuotedIDFactory idfac;
	private final List<RelationID> relations = new LinkedList<>();

	// There are special names that are not table names but are parsed as tables. 
	// These names are collected here and are not included in the table names
	private final Set<String> withTCEs = new HashSet<>();
	
	private boolean unsupported = false;


	private boolean inSubSelect = false;
	private Alias subSelectAlias = null;
	

	/**
	 * Main entry for this Tool class. A list of found tables is returned.
	 *
	 * @param select
	 * @return
	 */
	public TableNameVisitor(Select select, QuotedIDFactory idfac) {
		this.idfac = idfac;
		
 		if (select.getWithItemsList() != null) {
			for (WithItem withItem : select.getWithItemsList()) 
				withItem.accept(selectVisitor);
		}
		select.getSelectBody().accept(selectVisitor);
	}
		
	public Map<RelationID, RelationID> getTables() {	
		return tables;
	}
	
	public List<RelationID> getRelations() {	
		return relations;
	}

	public boolean isSupported() {
		// used to throw exception for the currently unsupported methods
		return !unsupported;
	}
	
	private void unsupported(Object o) {
		System.out.println(this.getClass() + " DOES NOT SUPPORT " + o);
		unsupported = true;
	}
	
	private final SelectVisitor selectVisitor = new SelectVisitor() {
		/* Visit the FROM clause to find tables
		 * Visit the JOIN and WHERE clauses to check if nested queries are present (non-Javadoc)
		 * 
		 * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.select.PlainSelect)
		 */
		@Override
		public void visit(PlainSelect plainSelect) {
			plainSelect.getFromItem().accept(fromItemVisitor);

			// When the mapping contains a DISTINCT we interpret it as a HINT to create a SUBVIEW.
			// Thus we presume that the unusual use of DISTINCT here is done ON PURPOSE for achieving this effect.
			if (plainSelect.getDistinct() != null) 
	            unsupported = true;

			if (plainSelect.getJoins() != null) {
				for (Join join : plainSelect.getJoins()) 
					join.getRightItem().accept(fromItemVisitor);
			}
			if (plainSelect.getWhere() != null) 
				plainSelect.getWhere().accept(expressionVisitor);
			
			for (SelectItem expr : plainSelect.getSelectItems()) 
				expr.accept(selectItemVisitor);
		}


		/*
		 * Visit UNION, INTERSECT, MINUM and EXCEPT to search for table names
		 * 
		 * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.select.SetOperationList)
		 */
		@Override
		public void visit(SetOperationList list) {
			unsupported(list);
			for (PlainSelect plainSelect : list.getPlainSelects()) 
				visit(plainSelect);
		}

		@Override
		public void visit(WithItem withItem) {
			withTCEs.add(withItem.getName().toLowerCase());
			withItem.getSelectBody().accept(this);
		}
	};

	private final FromItemVisitor fromItemVisitor = new FromItemVisitor() {

		/*
		 * Visit Table and store its value in the list of TableJSQL (non-Javadoc)
		 * We maintain duplicate tables to retrieve the different aliases assigned
		 * we use the class TableJSQL to handle quotes and user case choice if present
		 * 
		 * @see net.sf.jsqlparser.statement.select.FromItemVisitor#visit(net.sf.jsqlparser.schema.Table)
		 */
		
		@Override
		public void visit(Table table) {
			if (!withTCEs.contains(table.getFullyQualifiedName().toLowerCase())) {
				
				RelationID relationId = idfac.createRelationID(table.getSchemaName(), table.getName());
				relations.add(relationId);

				if (inSubSelect && subSelectAlias != null) {
					// ONLY SIMPLE SUBSELECTS, WITH ONE TABLE: see WhereClauseVisitor and ProjectionVisitor
					RelationID subSelectAliasId = idfac.createRelationID(null, subSelectAlias.getName());
					tables.put(subSelectAliasId, relationId);
				}
				else {
					Alias as = table.getAlias();
					RelationID aliasId = (as != null) ? idfac.createRelationID(null, as.getName()) : relationId;
					tables.put(aliasId, relationId);
				}
			}
		}

		@Override
		public void visit(SubSelect subSelect) {
			visitSubSelect(subSelect);
		}

		@Override
		public void visit(SubJoin subjoin) {
			unsupported(subjoin);
			subjoin.getLeft().accept(this);
			subjoin.getJoin().getRightItem().accept(this);
		}

		@Override
		public void visit(LateralSubSelect lateralSubSelect) {
			unsupported(lateralSubSelect);
			lateralSubSelect.getSubSelect().getSelectBody().accept(selectVisitor);
		}

		@Override
		public void visit(ValuesList valuesList) {
			unsupported(valuesList);
		}
	};
	
	
	private void visitSubSelect(SubSelect subSelect) {
		if (subSelect.getSelectBody() instanceof PlainSelect) {
			PlainSelect subSelBody = (PlainSelect) (subSelect.getSelectBody());	
			if (subSelBody.getJoins() != null || subSelBody.getWhere() != null) 
				unsupported(subSelect);	
		} 
		else
			unsupported(subSelect);

		inSubSelect = true;
		subSelectAlias = subSelect.getAlias();
		subSelect.getSelectBody().accept(selectVisitor);
		subSelectAlias = null;
		inSubSelect = false;
	}
	
	private final SelectItemVisitor selectItemVisitor = new SelectItemVisitor() {
		@Override
		public void visit(AllColumns expr) {
			//Do nothing!
		}

		@Override
		public void visit(AllTableColumns arg0) {
			//Do nothing!
		}

		@Override
		public void visit(SelectExpressionItem expr) {
			expr.getExpression().accept(expressionVisitor);
		}
	};

	private final ExpressionVisitor expressionVisitor = new ExpressionVisitor() {
	
		/*
		 * We do the same procedure for all Binary Expressions
		 * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Addition)
		 */
		@Override
		public void visit(Addition addition) {
			visitBinaryExpression(addition);
		}

		@Override
		public void visit(AndExpression andExpression) {
			visitBinaryExpression(andExpression);
		}

		@Override
		public void visit(Between between) {
			between.getLeftExpression().accept(this);
			between.getBetweenExpressionStart().accept(this);
			between.getBetweenExpressionEnd().accept(this);
		}

		@Override
		public void visit(Column tableColumn) {
			//it does nothing here, everything is good
		}

		@Override
		public void visit(Division division) {
			visitBinaryExpression(division);
		}

		@Override
		public void visit(DoubleValue doubleValue) {
		}

		@Override
		public void visit(EqualsTo equalsTo) {
			visitBinaryExpression(equalsTo);
		}

		@Override
		public void visit(Function function) {
	        switch (function.getName().toLowerCase()) {
	            case "regexp_like" :
	            case "regexp_replace" :
	            case "replace" :
	            case "concat" :
	            case "substr" : 
	                for (Expression ex :function.getParameters().getExpressions()) 
	                    ex.accept(this);
	                break;

	            default:
	                unsupported(function);
	                break;
	        }
		}

		@Override
		public void visit(GreaterThan greaterThan) {
			visitBinaryExpression(greaterThan);
		}

		@Override
		public void visit(GreaterThanEquals greaterThanEquals) {
			visitBinaryExpression(greaterThanEquals);
		}

		@Override
		public void visit(InExpression inExpression) {
			inExpression.getLeftExpression().accept(this);
			inExpression.getRightItemsList().accept(itemsListVisitor);
		}

		@Override
		public void visit(IsNullExpression isNullExpression) {
		}

		@Override
		public void visit(JdbcParameter jdbcParameter) {
			unsupported(jdbcParameter);
		}

		@Override
		public void visit(LikeExpression likeExpression) {
			visitBinaryExpression(likeExpression);
		}

		@Override
		public void visit(ExistsExpression existsExpression) {
			existsExpression.getRightExpression().accept(this);
		}

		@Override
		public void visit(LongValue longValue) {
		}

		@Override
		public void visit(MinorThan minorThan) {
			visitBinaryExpression(minorThan);
		}

		@Override
		public void visit(MinorThanEquals minorThanEquals) {
			visitBinaryExpression(minorThanEquals);
		}

		@Override
		public void visit(Multiplication multiplication) {
			visitBinaryExpression(multiplication);
		}

		@Override
		public void visit(NotEqualsTo notEqualsTo) {
			visitBinaryExpression(notEqualsTo);
		}

		@Override
		public void visit(NullValue nullValue) {
		}

		@Override
		public void visit(OrExpression orExpression) {
			visitBinaryExpression(orExpression);
		}

		@Override
		public void visit(Parenthesis parenthesis) {
			parenthesis.getExpression().accept(this);
		}

		@Override
		public void visit(StringValue stringValue) {
		}

		@Override
		public void visit(Subtraction subtraction) {
			visitBinaryExpression(subtraction);
		}

		private void visitBinaryExpression(BinaryExpression binaryExpression) {
			binaryExpression.getLeftExpression().accept(this);
			binaryExpression.getRightExpression().accept(this);
		}

		@Override
		public void visit(DateValue dateValue) {
		}

		@Override
		public void visit(TimestampValue timestampValue) {
		}

		@Override
		public void visit(TimeValue timeValue) {
		}


		@Override
		public void visit(CaseExpression caseExpression) {
			unsupported(caseExpression);
		}

		@Override
		public void visit(WhenClause whenClause) {
			unsupported(whenClause);
		}

		@Override
		public void visit(AllComparisonExpression allComparisonExpression) {
			unsupported(allComparisonExpression);
			allComparisonExpression.getSubSelect().getSelectBody().accept(selectVisitor);
		}

		@Override
		public void visit(AnyComparisonExpression anyComparisonExpression) {
			unsupported(anyComparisonExpression);
			anyComparisonExpression.getSubSelect().getSelectBody().accept(selectVisitor);
		}

		@Override
		public void visit(Concat concat) {
			visitBinaryExpression(concat);
		}

		@Override
		public void visit(Matches matches) {
			unsupported(matches);
			visitBinaryExpression(matches);
		}

		@Override
		public void visit(BitwiseAnd bitwiseAnd) {
			unsupported(bitwiseAnd);
			visitBinaryExpression(bitwiseAnd);
		}

		@Override
		public void visit(BitwiseOr bitwiseOr) {
			unsupported(bitwiseOr);
			visitBinaryExpression(bitwiseOr);
		}

		@Override
		public void visit(BitwiseXor bitwiseXor) {
			unsupported(bitwiseXor);
			visitBinaryExpression(bitwiseXor);
		}

		@Override
		public void visit(CastExpression cast) {
			cast.getLeftExpression().accept(this);
		}

		@Override
		public void visit(Modulo modulo) {
			unsupported(modulo);
			visitBinaryExpression(modulo);
		}

		@Override
		public void visit(AnalyticExpression analytic) {
			unsupported(analytic);
		}

		@Override
		public void visit(ExtractExpression eexpr) {
			unsupported(eexpr);
		}

		@Override
		public void visit(IntervalExpression iexpr) {
			unsupported(iexpr);
		}

	    @Override
	    public void visit(JdbcNamedParameter jdbcNamedParameter) {
			unsupported(jdbcNamedParameter);
	    }

		@Override
		public void visit(OracleHierarchicalExpression arg0) {
			unsupported(arg0);		
		}

		@Override
		public void visit(RegExpMatchOperator rexpr) {
//			unsupported = true;
//			visitBinaryExpression(rexpr);
		}


		@Override
		public void visit(SignedExpression arg0) {
			System.out.println("WARNING: SignedExpression   not implemented ");
			unsupported(arg0);
		}

		@Override
		public void visit(JsonExpression arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(RegExpMySQLOperator arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visit(SubSelect subSelect) {
			visitSubSelect(subSelect);
		}
	};

	
	private final ItemsListVisitor itemsListVisitor = new ItemsListVisitor() {
		@Override
		public void visit(ExpressionList expressionList) {
			for (Expression expression : expressionList.getExpressions()) 
				expression.accept(expressionVisitor);
		}

		@Override
		public void visit(MultiExpressionList multiExprList) {
			unsupported(multiExprList);
			for (ExpressionList exprList : multiExprList.getExprList()) 
				exprList.accept(this);
		}

		@Override
		public void visit(SubSelect subSelect) {
			visitSubSelect(subSelect);
		}
	};
}
