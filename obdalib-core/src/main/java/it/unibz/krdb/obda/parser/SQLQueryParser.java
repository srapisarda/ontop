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
import it.unibz.krdb.sql.api.ShallowlyParsedSQLQuery;
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

        // the WITH is not supported
        if (selectQuery.getWithItemsList() != null) {
            unsupported( selectQuery.getWithItemsList() );
            //for (WithItem withItem : selectQuery.getWithItemsList())
            //    withItem.accept(selectVisitor);
        }

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

    private void unsupported(Object unsupportedObject )  {
        log.warn(this.getClass() + " DOES NOT SUPPORT " + unsupportedObject);
        throw new ParseException(unsupportedObject) ;
    }

    private void unsupportedMapping(Object unsupportedObject )  {
        log.warn(this.getClass() + " DOES NOT SUPPORT " + unsupportedObject);
        throw new MappingQueryException(unsupportedObject) ;
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

    private static class MappingQueryException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final Object unsupportedObject;

        public MappingQueryException (Object unsupportedObject) {
            this.unsupportedObject = unsupportedObject;
        }
    }

    private void visitSubSelect(SubSelect subSelect ){
        if (subSelect.getSelectBody() instanceof PlainSelect) {
            PlainSelect subSelBody = (PlainSelect) (subSelect.getSelectBody());

            if (subSelBody.getJoins() != null || subSelBody.getWhere() != null) {
                unsupported(subSelect);
            }
            else {
                subSelBody.accept(selectVisitor);
            }
        }
        else
            unsupported(subSelect);
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


//            if (plainSelect.getJoins() != null) {
//                for (Join join : plainSelect.getJoins()) {
//                    // this is checking for subSelect on all the outer items
//                    // join.getRightItem().accept(aliasMapFromItemVisitor); // todo: commented this line because is already done after
//                    join.getRightItem().accept(fromItemVisitor);
//                }
//            }
            if (plainSelect.getWhere() != null) {
                plainSelect.getWhere().accept(tableExpressionVisitor);
            }


            /// this operation has been already done
//            for (SelectItem item : plainSelect.getSelectItems()) {
//                item.accept(aliasMapSelectItemVisitor);
//            }

            joinVisit(plainSelect);


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
            
            if (plainSelect.getIntoTables() != null)
                unsupportedMapping(plainSelect.getIntoTables());
            
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


        private void joinVisit(PlainSelect plainSelect) {
            FromItem fromItem = plainSelect.getFromItem();

            //todo: the operation above has been already performed
//          fromItem.accept(fromItemVisitor);

            List<Join> joins = plainSelect.getJoins();

            if (joins != null)
                for (Join join : joins) {
                    Expression expr = join.getOnExpression();

                    if (join.getUsingColumns() != null) // JOIN USING column
                        for (Column column : join.getUsingColumns()) {

                            if (fromItem instanceof Table && join.getRightItem() instanceof Table) {
                                Table table1 = (Table)fromItem;

                                Column column1 = new Column(table1, column.getColumnName());
                                ShallowlyParsedSQLQuery.normalizeColumnName(idFac, column1);

                                BinaryExpression binaryExpression = new EqualsTo();
                                binaryExpression.setLeftExpression(column1);

                                Column column2 = new Column((Table)join.getRightItem(), column.getColumnName());
                                ShallowlyParsedSQLQuery.normalizeColumnName(idFac, column2);
                                binaryExpression.setRightExpression(column2);
                                joinConditions.add(binaryExpression);
                            }
                            else {
                                //more complex structure in FROM or JOIN e.g. subSelects are not supported
                                unsupported( fromItem );
                            }
                        }

                    else{ //JOIN ON cond
                        if (expr != null) {
                            join.getRightItem().accept(fromItemVisitor);
                            // ROMAN (25 Sep 2015): this transforms (A.id = B.id) OR (A.id2 = B.id2) into the list
                            // { (A.id = B.id), (A.id2 = B.id2) }, which is equivalent to AND!
                            // similarly, it will transform NOT (A <> B) into { (A <> B) }, which gets rid of NOT
                            expr.accept(joinExpressionVisitor);
                        }
                        //we do not consider simple joins
//					else
//						if(join.isSimple())
//							joinConditions.add(plainSelect.getWhere().toString());

                    }

                }
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
            expr.accept(projectorExpressionVisitor);


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


    private final FromItemVisitor subSelectFromItemVisitor = new FromItemVisitor() {
        private boolean inSubSelect = false;
        // from visitor
        private Alias subSelectAlias = null;


        @Override
        public void visit(Table table) {


            RelationID relationId = idFac.createRelationID(table.getSchemaName(), table.getName());
            relations.add(relationId);
            // ONLY SIMPLE SUBSELECTS, WITH ONE TABLE: see WhereClauseVisitor and ProjectionVisitor

            RelationID subSelectAliasId = idFac.createRelationID(null, subSelectAlias.getName());
            tables.put(subSelectAliasId, relationId);
        }

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
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

    private final FromItemVisitor fromItemVisitor = new FromItemVisitor() {

        // from visitor
      //  private Alias subSelectAlias = null;
        // There are special names that are not table names but are parsed as tables.
        // These names are collected here and are not included in the table names
        //private final Set<String> withTCEs = new HashSet<>();
     //   private boolean inSubSelect = false;

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
 //           if (!withTCEs.contains(table.getFullyQualifiedName().toLowerCase())) {

                RelationID relationId = idFac.createRelationID(table.getSchemaName(), table.getName());
                relations.add(relationId);

                // ROMAN (9 Dec 2015): make a separate visitor for sub-selects
//                if (inSubSelect && subSelectAlias != null) {
//                    // ONLY SIMPLE SUBSELECTS, WITH ONE TABLE: see WhereClauseVisitor and ProjectionVisitor
//                    RelationID subSelectAliasId = idFac.createRelationID(null, subSelectAlias.getName());
//                    tables.put(subSelectAliasId, relationId);
//                }
//                else {
                    Alias as = table.getAlias();
                    RelationID aliasId = (as != null) ? idFac.createRelationID(null, as.getName()) : relationId;
                    tables.put(aliasId, relationId);
//                }
//            }
        }

        @Override
        public void visit(SubSelect subSelect) {
            subSelectFromItemVisitor.visit(subSelect);

//                visitSubSelect(subSelect);
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
            //lateralSubSelect.getSubSelect().getSelectBody().accept(selectVisitor);
        }

        @Override
        public void visit(ValuesList valuesList) {
            unsupported(valuesList);
        }


//        private void visitSubSelect(SubSelect subSelect) {
//
//            if (subSelect.getSelectBody() instanceof PlainSelect) {
//                PlainSelect subSelBody = (PlainSelect) (subSelect.getSelectBody());
//                if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
//                    unsupported(subSelect);
//            }
//            else {
//                unsupported(subSelect);
//            }
//
//            inSubSelect = true;
//            subSelectAlias = subSelect.getAlias();
//
//            subSelect.getSelectBody().accept(selectVisitor);
//
//            subSelectAlias = null;
//            inSubSelect = false;
//        }

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

    private final ExpressionVisitor tableExpressionVisitor = new ExpressionVisitor() {

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


    private ExpressionVisitor projectorExpressionVisitor = new ExpressionVisitor() {
        @Override
        public void visit(NullValue nullValue) {
            // TODO Auto-generated method stub
        }

        /*
         * The system cannot support function currently (non-Javadoc)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.Function)
         * @link ConferenceConcatMySQLTest
         */
        @Override
        public void visit(Function function) {
            switch (function.getName().toLowerCase()) {
                case "regexp_like" :
                case "regexp_replace" :
                case "replace" :
                case "concat" :
                    //case "substr" :
                    for (Expression ex :function.getParameters().getExpressions())
                        ex.accept(this);
                    break;
                default:
                    unsupported(function);
            }

        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);
        }

        @Override
        public void visit(Addition addition) {
            visitBinaryExpression(addition);
        }

        @Override
        public void visit(Division division) {
            visitBinaryExpression(division);
        }

        @Override
        public void visit(Multiplication multiplication) {
            visitBinaryExpression(multiplication);
        }

        @Override
        public void visit(Subtraction subtraction) {
            visitBinaryExpression(subtraction);
        }

        @Override
        public void visit(SignedExpression arg0) {
            arg0.getExpression().accept(this);
        }

        @Override
        public void visit(AndExpression andExpression) {
            unsupported(andExpression);
        }

        @Override
        public void visit(OrExpression orExpression) {
            unsupported(orExpression);
        }

        @Override
        public void visit(Between between) {
            between.getLeftExpression().accept(this);
            between.getBetweenExpressionStart().accept(this);
            between.getBetweenExpressionEnd().accept(this);
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            unsupported(equalsTo);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            unsupported(greaterThan);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            unsupported(greaterThanEquals);
        }

        @Override
        public void visit(InExpression inExpression) {
            //Expression e = inExpression.getLeftExpression();
            ItemsList e1 = inExpression.getLeftItemsList();
            if (e1 instanceof SubSelect){
                ((SubSelect)e1).accept(this);
            }
            else if (e1 instanceof ExpressionList) {
                for (Expression expr : ((ExpressionList)e1).getExpressions()) {
                    expr.accept(this);
                }
            }
            else if (e1 instanceof MultiExpressionList) {
                for (ExpressionList exp : ((MultiExpressionList)e1).getExprList()){
                    for (Expression expr : exp.getExpressions()) {
                        expr.accept(this);
                    }
                }
            }
        }

        @Override
        public void visit(IsNullExpression isNullExpression) {
            unsupported(isNullExpression);
        }

        @Override
        public void visit(LikeExpression likeExpression) {
            unsupported(likeExpression);
        }

        @Override
        public void visit(MinorThan minorThan) {
            unsupported(minorThan);
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            unsupported(minorThanEquals);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            unsupported(notEqualsTo);
        }

        /*
         * Visit the column and remove the quotes if they are present(non-Javadoc)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
         */
        @Override
        public void visit(Column tableColumn) {
            // CHANGES TABLE AND COLUMN NAMES
            // TODO: add implementation here
            ShallowlyParsedSQLQuery.normalizeColumnName(idFac, tableColumn);
        }

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
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
        public void visit(ExistsExpression existsExpression) {
            unsupported(existsExpression);
        }

        @Override
        public void visit(AllComparisonExpression allComparisonExpression) {
            unsupported(allComparisonExpression);
        }

        @Override
        public void visit(AnyComparisonExpression anyComparisonExpression) {
            unsupported(anyComparisonExpression);
        }

        @Override
        public void visit(Concat concat) {
            visitBinaryExpression(concat);
        }

        @Override
        public void visit(Matches matches) {
            unsupported(matches);
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            unsupported(bitwiseAnd);
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            unsupported(bitwiseOr);
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            unsupported(bitwiseXor);
        }

        @Override
        public void visit(CastExpression cast) {


        }

        @Override
        public void visit(Modulo modulo) {
            unsupported(modulo);
        }

        @Override
        public void visit(AnalyticExpression aexpr) {
            unsupported(aexpr);
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
        public void visit(OracleHierarchicalExpression oexpr) {
            unsupported(oexpr);
        }

        @Override
        public void visit(RegExpMatchOperator arg0) {
            unsupported(arg0);
        }


        @Override
        public void visit(JsonExpression arg0) {
        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
        }

        @Override
        public void visit(JdbcParameter jdbcParameter) {
            unsupported(jdbcParameter);
        }

        @Override
        public void visit(JdbcNamedParameter jdbcNamedParameter) {
            unsupported(jdbcNamedParameter);
        }


        private void visitBinaryExpression(BinaryExpression binaryExpression) {
            binaryExpression.getLeftExpression().accept(this);
            binaryExpression.getRightExpression().accept(this);
        }

	/*
	 * scalar values: all supported
	 */

        @Override
        public void visit(DoubleValue doubleValue) {
            // NO-OP
        }

        @Override
        public void visit(LongValue longValue) {
            // NO-OP
        }

        @Override
        public void visit(DateValue dateValue) {
            // NO-OP
        }

        @Override
        public void visit(TimeValue timeValue) {
            // NO-OP
        }

        @Override
        public void visit(TimestampValue timestampValue) {
            // NO-OP
        }

        @Override
        public void visit(StringValue stringValue) {
            // NO-OP
        }
    };


    /**
     * Visitor for expression in the JOIN ON conditions
     *
     */
    private final ExpressionVisitor joinExpressionVisitor = new ExpressionVisitor() {
        @Override
        public void visit(NullValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(Function arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(JdbcParameter arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(JdbcNamedParameter arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(DoubleValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(LongValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(DateValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(TimeValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(TimestampValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);

        }

        @Override
        public void visit(StringValue arg0) {
            //we do not execute anything

        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Addition)
         */
        @Override
        public void visit(Addition addition) {
            visitBinaryExpression(addition);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Division)
         */
        @Override
        public void visit(Division arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Multiplication)
         */
        @Override
        public void visit(Multiplication arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Subtraction)
         */
        @Override
        public void visit(Subtraction arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.AndExpression)
         */
        @Override
        public void visit(AndExpression arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.OrExpression)
         */
        @Override
        public void visit(OrExpression arg0) {
            visitBinaryExpression(arg0);
        }

        @Override
        public void visit(Between arg0) {
            //we do not consider the case of BETWEEN

        }

		/*
		 * We store in join conditions the binary expression that are not nested,
		 * for the others we continue to visit the subexpression
		 * Example: AndExpression and OrExpression always have subexpression.
		 */

        public void visitBinaryExpression(BinaryExpression binaryExpression) {
            Expression left = binaryExpression.getLeftExpression();
            Expression right = binaryExpression.getRightExpression();

            if (!(left instanceof BinaryExpression) &&
                    !(right instanceof BinaryExpression)) {

                left.accept(this);
                right.accept(this);
                // ROMAN (25 Sep 2015): this transforms OR into AND
                joinConditions.add(binaryExpression);
            }
            else {
                left.accept(this);
                right.accept(this);
            }
        }

        /*
         *  We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.EqualsTo)
         */
        @Override
        public void visit(EqualsTo arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         *  We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThan)
         */
        @Override
        public void visit(GreaterThan arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         *  We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals)
         */
        @Override
        public void visit(GreaterThanEquals arg0) {
            visitBinaryExpression(arg0);
        }


        @Override
        public void visit(InExpression arg0) {
            //we do not support the case for IN condition

        }

        @Override
        public void visit(IsNullExpression arg0) {
            //we do not execute anything

        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.LikeExpression)
         */
        @Override
        public void visit(LikeExpression arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThan)
         */
        @Override
        public void visit(MinorThan arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(MinorThanEquals arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.NotEqualsTo)
         */
        @Override
        public void visit(NotEqualsTo arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * Remove quotes from columns if they are present (non-Javadoc)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
         */
        @Override
        public void visit(Column tableColumn) {
            // CHANGE TABLE / COLUMN NAME IN THE JOIN CONDITION
            // TableJSQL.unquoteColumnAndTableName(tableColumn);
            ShallowlyParsedSQLQuery.normalizeColumnName(idFac, tableColumn);
        }

        /*
         * We visit also the subselect to find nested joins
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.statement.select.SubSelect)
         */
        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }

        @Override
        public void visit(CaseExpression arg0) {
            // we do not support case expression
            unsupported(arg0);

        }

        @Override
        public void visit(WhenClause arg0) {
            // we do not support when expression
            unsupported(arg0);
        }

        @Override
        public void visit(ExistsExpression exists) {
            // we do not support exists
            unsupported(exists);
        }

        /*
         * We visit the subselect in ALL(...)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AllComparisonExpression)
         */
        @Override
        public void visit(AllComparisonExpression all) {
            unsupported(all);
        }

        /*
         * We visit the subselect in ANY(...)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnyComparisonExpression)
         */
        @Override
        public void visit(AnyComparisonExpression any) {
            unsupported(any);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Concat arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Matches arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseAnd arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseOr arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseXor arg0) {
            unsupported(arg0);
        }

        @Override
        public void visit(CastExpression arg0) {
            // TODO :  this should be ignored
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Modulo arg0) {
            unsupported(arg0);
        }

        @Override
        public void visit(AnalyticExpression arg0) {
            // we do not consider AnalyticExpression
            unsupported(arg0);
        }

        @Override
        public void visit(ExtractExpression arg0) {
            // we do not consider ExtractExpression
            unsupported(arg0);
        }

        @Override
        public void visit(IntervalExpression arg0) {
            // we do not consider IntervalExpression
            unsupported(arg0);
        }

        @Override
        public void visit(OracleHierarchicalExpression arg0) {
            // we do not consider OracleHierarchicalExpression
            unsupported(arg0);
        }


        @Override
        public void visit(RegExpMatchOperator arg0) {
            unsupported(arg0);
        }

        @Override
        public void visit(SignedExpression arg0) {
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
    };


}
