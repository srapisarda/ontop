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

import it.unibz.krdb.sql.QuotedID;
import it.unibz.krdb.sql.QuotedIDFactory;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to create an Alias Map for the select statement
 * 
 * BRINGS EXPRESSION ALIAS NAMES in SELECT clauses (including subqueries) into NORMAL FORM
 * 
 */

/**
 * 
 * Find all the references between the column names and their aliases,
 * visiting the {@link SelectItem}.
 * Remove quotes when present.
 */

public class AliasMapVisitor {

	private final QuotedIDFactory idfac;
	private final Map<QuotedID, Expression> aliasMap = new HashMap<>();
	
	/**
	 * NORMALISES EXPRESSION ALIAS NAMES in SELECT clauses (including subqueries)
	 * 
	 * @param select
	 * @param idfac
	 */
	
	public AliasMapVisitor(Select select, QuotedIDFactory idfac) {
		this.idfac = idfac;
		setSelect(select);
	}

	/**
	 * Set the select statement
	 * @param select
	 * 		Set query statement
	 */
	public void setSelect (Select select ){
		if (select.getWithItemsList() != null) {
			for (WithItem withItem : select.getWithItemsList()) {
				withItem.accept(selectVisitor);
			}
		}
	}
	
	/**
	 * Return a map between the column in the select statement and its alias.
	 * @return alias map
	 */
	
	public Map<QuotedID, Expression> getAliasMap() {
		return aliasMap;
	}
	
	
	private SelectVisitor selectVisitor = new SelectVisitor() {
		/*
		 * visit PlainSelect, search for the content in select
		 * Stored in AggregationJSQL. 
		 * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.select.PlainSelect)
		 */
		
		@Override
		public void visit(PlainSelect plainSelect) {
	        plainSelect.getFromItem().accept(fromItemVisitor);

	        if (plainSelect.getJoins() != null) {
	            for (Join join : plainSelect.getJoins()) {
	                join.getRightItem().accept(fromItemVisitor);
	            }
	        }
	        if (plainSelect.getWhere() != null) {
	            plainSelect.getWhere().accept(expressionVisitor);
	        }
			
			for (SelectItem item : plainSelect.getSelectItems()) {
				item.accept(selectItemVisitor);
			}
	    }
		
		@Override
		public void visit(SetOperationList arg0) {
			// we are not considering UNION case
		}

		@Override
		public void visit(WithItem arg0) {
			// we are not considering withItem case
		}
	};
	
	private SelectItemVisitor selectItemVisitor = new SelectItemVisitor() {

		@Override
		public void visit(AllColumns columns) {
			//we are not interested in allcolumns (SELECT *) since it does not have aliases	
		}

		@Override
		public void visit(AllTableColumns tableColumns) {
			//we are not interested in alltablecolumns (SELECT table.*) since it does not have aliases
		}
		
		/*
		 *visit SelectExpressionItem that contains the expression and its alias as in SELECT expr1 AS EXPR
		 * @see net.sf.jsqlparser.statement.select.SelectItemVisitor#visit(net.sf.jsqlparser.statement.select.SelectExpressionItem)
		 */

		@Override
		public void visit(SelectExpressionItem selectExpr) {
			Alias alias = selectExpr.getAlias();
			if (alias  != null) {
				Expression e = selectExpr.getExpression();
				e.accept(expressionVisitor);
				
				// NORMALIZE EXPRESSION ALIAS NAME
				QuotedID aliasName = idfac.createAttributeID(alias.getName());
				alias.setName(aliasName.getSQLRendering());
				aliasMap.put(aliasName, e);
			}
			// ELSE
			// ROMAN (27 Sep 2015): set an error flag -- each complex expression must have a name (alias)
		}
	};
	
	FromItemVisitor fromItemVisitor = new FromItemVisitor() {
	    @Override
	    public void visit(Table tableName) {

	    }

	    @Override
	    public void visit(SubJoin subjoin) {

	    }

	    @Override
	    public void visit(LateralSubSelect lateralSubSelect) {

	    }

	    @Override
	    public void visit(ValuesList valuesList) {

	    }
	    @Override
		public void visit(SubSelect subSelect) {
	        subSelect.getSelectBody().accept(selectVisitor);
		}
	};
    
	private ExpressionVisitor expressionVisitor = new ExpressionVisitor() {
	
	    @Override
		public void visit(SubSelect subSelect) {
	        subSelect.getSelectBody().accept(selectVisitor);
		}

		/*
		 * We do not modify the column we are only interested if the alias is present. 
		 * Each alias has a distinct column (non-Javadoc)
		 * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
		 */
		@Override
		public void visit(Column tableColumn) {
			
		}

		
	@Override
	public void visit(NullValue nullValue) {
	}

	@Override
	public void visit(Function function) {
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue stringValue) {
	}

	@Override
	public void visit(Addition addition) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division division) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication multiplication) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction subtraction) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AndExpression andExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OrExpression orExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between between) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo equalsTo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InExpression inExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression isNullExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression likeExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan minorThan) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		// TODO Auto-generated method stub
		
	}


    @Override
	public void visit(CaseExpression caseExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause whenClause) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression existsExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression allComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression anyComparisonExpression) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat concat) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches matches) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd bitwiseAnd) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr bitwiseOr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor bitwiseXor) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CastExpression cast) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Modulo modulo) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnalyticExpression aexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExtractExpression eexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IntervalExpression iexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHierarchicalExpression oexpr) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMatchOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SignedExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMySQLOperator arg0) {
		// TODO Auto-generated method stub
		
	}
	};

}
