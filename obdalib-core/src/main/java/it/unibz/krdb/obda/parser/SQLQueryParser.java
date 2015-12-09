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
import it.unibz.krdb.sql.RelationID;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
    This visitor is used to parse SQL amd check the OBDA query compatibility
 */
public class SQLQueryParser {

    //region Private Variables
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final QuotedIDFactory idFac;

    private final Map<RelationID, RelationID> tables = new HashMap<>();
    private final Map<QuotedID, Expression> aliasMap = new HashMap<>();
    private final List<Expression> joinConditions = new LinkedList<>();
    private Expression whereClause;
    private final List<SelectItem> projection = new LinkedList<>();

    private final List<RelationID> relations = new LinkedList<>();


    //endregion

    public SQLQueryParser(Select selectQuery, QuotedIDFactory idFac) {
        this.idFac = idFac;

        // CATCH ParseException
        selectQuery.getSelectBody().accept(selectVisitor);
    }


    //region Properties
    /**
     *  All found tables is returned as a Map of RelationID @see it.unibz.krdb.sql.RelationID
     *  @return Map of RelationID
     */
    public Map<RelationID, RelationID> getTables() {
        return tables;
    }

    /**
     * Return a map between the column in the select statement and its alias.
     * @return alias map
    */
    public Map<QuotedID, Expression> getAliasMap() {
        return aliasMap;
    }

    /**
     * Obtain the join conditions in a format "expression condition expression"
     * for example "table1.home = table2.house"<br>
     *
     * BRINGS TABLE NAME / SCHEMA / ALIAS AND COLUMN NAMES in the JOIN / JOIN ON clauses into NORMAL FORM
     *
     * @return a list of Expression containing the join conditions
     */
    public List<Expression> getJoinConditions() {
        return joinConditions;
    }

    /**
     * Give the WHERE clause of the SELECT statement<br>
     *
     * NOTE: is also BRINGS ALL SCHEMA / TABLE / ALIAS / COLUMN NAMES in the WHERE clause into NORMAL FORM
     *
     * @return an Expression represent a where clause
     */
    public Expression getWhereClause() {
        return whereClause;
    }

    /**
     * Return the list of Projection with the expressions between SELECT and FROM<br>
     *
     * @return projection object
     */
    public List<SelectItem> getProjection() {
        return projection;
    }


    //endregion

    private void unsupported(Object unsupportedObject)  {
        log.warn(this.getClass() + " DOES NOT SUPPORT " + unsupportedObject);
        throw new ParseException(unsupportedObject) ;
    }

    /**
     * This exceptions is throwing when the parser cannot support an operation
     */
    private static class ParseException extends RuntimeException {

		private static final long serialVersionUID = 1L;
		
        private final Object unsupportedObject;
        
		public ParseException (Object unsupportedObject) {
            this.unsupportedObject = unsupportedObject;
        }
    }

    private SelectVisitor selectVisitor = new SelectVisitor() {
    	
        @Override
        public void visit(PlainSelect plainSelect) {
            // the SELECT should correspond to a CQ
            validatePlainSelect(plainSelect);

            // todo: do I need to parse the unsupported query??

            // projection
            for (SelectItem item : plainSelect.getSelectItems())
                item.accept(selectItemVisitor);

            // alias
            // this is checking for subSelect in the alias
           // plainSelect.getFromItem().accept(aliasMapFromItemVisitor); // todo: commented this line because is already done after

            // from items
            plainSelect.getFromItem().accept(fromItemVisitor);


            if (plainSelect.getJoins() != null) {
                for (Join join : plainSelect.getJoins()) {
                    // this is checking for subSelect on all the outer items
                    // join.getRightItem().accept(aliasMapFromItemVisitor); // todo: commented this line because is already done after
                    join.getRightItem().accept(fromItemVisitor);
                }
            }
            if (plainSelect.getWhere() != null) {
                plainSelect.getWhere().accept(expressionVisitor);
            }

            /// this operation has been already done
//            for (SelectItem item : plainSelect.getSelectItems()) {
//                item.accept(aliasMapSelectItemVisitor);
//            }

        }
        @Override
        public void visit(SetOperationList setOpList) {
            unsupported(setOpList);
        }

        @Override
        public void visit(WithItem withItem) {
            unsupported(withItem);
        }

        /**
         * This validates that a PlainSelect Object is datalog suitable
         *
         * @param plainSelect
         */
        private void validatePlainSelect(PlainSelect plainSelect) {
            if (plainSelect.getDistinct() != null) 
                unsupported(plainSelect.getDistinct());
            
            // TODO: different kind of exception -- SELECT INTO is not a valid mapping query
            if (plainSelect.getIntoTables() != null) 
                unsupported(plainSelect.getIntoTables());
            
            if (plainSelect.getHaving() != null) 
                unsupported(plainSelect.getHaving());
            
            if (plainSelect.getGroupByColumnReferences() != null) 
                unsupported(plainSelect.getGroupByColumnReferences());
            
            if (plainSelect.getOrderByElements() != null)
                unsupported(plainSelect.getOrderByElements());
            
            if (plainSelect.getLimit() != null) 
                unsupported(plainSelect.getLimit());

            if (plainSelect.getTop() != null) 
                unsupported(plainSelect.getTop());
                
            if (plainSelect.getOracleHierarchical() != null)
                unsupported(plainSelect.getOracleHierarchical());
            
            if (plainSelect.isOracleSiblings())
                unsupported(plainSelect.isOracleSiblings());
        }
    };


    private SelectItemVisitor selectItemVisitor = new SelectItemVisitor() {

        /**
         * SELECT *
         */
    	@Override
        public void visit(AllColumns allColumns) {
            projection.add(allColumns);
        }

        /**
         * SELECT table.*
         */
        @Override
        public void visit(AllTableColumns allTableColumns) {
            projection.add(allTableColumns);
        }

        /**
         * Add the projection for the selectExpressionItem
         */
        @Override
        public void visit(SelectExpressionItem selectExpr) {
            // projection
        	Expression expr = selectExpr.getExpression();
            projection.add(selectExpr);
            expr.accept(expressionVisitor);
            Alias alias = selectExpr.getAlias();
            // all complex expressions in SELECT must be named (by aliases)
            if (!(expr instanceof Column) && alias == null)
                unsupported(selectExpr);

            // alias
            if (alias  != null) {
                // NORMALIZE EXPRESSION ALIAS NAME (ROMAN 9 Dec 2015: this should be done later; the visitor should not modify the query)
                QuotedID aliasName = idFac.createAttributeID(alias.getName());
                alias.setName(aliasName.getSQLRendering());
                aliasMap.put(aliasName, expr);
            }
        }
    };



    private final FromItemVisitor fromItemVisitor = new FromItemVisitor() {

        // from visitor
        private Alias subSelectAlias = null;
        // There are special names that are not table names but are parsed as tables.
        // These names are collected here and are not included in the table names
        private final Set<String> withTCEs = new HashSet<>();
        private boolean inSubSelect = false;
  	
		/*
		 * Visit Table and store its value in the list of TableJSQL (non-Javadoc)
		 * We maintain duplicate tables to retrieve the different aliases assigned
		 * we use the class TableJSQL to handle quotes and user case choice if present
		 *
		 * @see net.sf.jsqlparser.statement.select.FromItemVisitor#visit(net.sf.jsqlparser.schema.Table)
		 */

        @Override
        public void visit(Table table) {
        	// ROMAN (9 Dec 2015): TCEs are not supported 
            if (!withTCEs.contains(table.getFullyQualifiedName().toLowerCase())) {

                RelationID relationId = idFac.createRelationID(table.getSchemaName(), table.getName());
                relations.add(relationId);

                // ROMAN (9 Dec 2015): make a separate visitor for sub-selects
                if (inSubSelect && subSelectAlias != null) {
                    // ONLY SIMPLE SUBSELECTS, WITH ONE TABLE: see WhereClauseVisitor and ProjectionVisitor
                    RelationID subSelectAliasId = idFac.createRelationID(null, subSelectAlias.getName());
                    tables.put(subSelectAliasId, relationId);
                }
                else {
                    Alias as = table.getAlias();
                    RelationID aliasId = (as != null) ? idFac.createRelationID(null, as.getName()) : relationId;
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


        private void visitSubSelect(SubSelect subSelect) {

            if (subSelect.getSelectBody() instanceof PlainSelect) {
                PlainSelect subSelBody = (PlainSelect) (subSelect.getSelectBody());
                if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
                    unsupported(subSelect);
            }
            else {
                unsupported(subSelect);
            }

            inSubSelect = true;
            subSelectAlias = subSelect.getAlias();

            subSelect.getSelectBody().accept(selectVisitor);

            subSelectAlias = null;
            inSubSelect = false;
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
