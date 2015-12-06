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
import it.unibz.krdb.sql.api.ProjectionJSQL;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/*
    This visitor is used to parse SQL amd check the OBDA query compatibility
 */
public class ObdaVisitor {

    //region Private Variables
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final String[] invalidObdaFields =
            { "getDistinct", "getIntoTables", "getHaving", "getGroupByColumnReferences",
                    "getOrderByElements", "getLimit", "getTop", "getOracleHierarchical", "isOracleSiblings" };
    private final Select selectQuery; // the parsed query
    private final QuotedIDFactory idFac;

    private  final Map<RelationID, RelationID> tables = new HashMap<>();
    private  final Map<QuotedID, Expression> aliasMap = new HashMap<>();
    private  final List<Expression> joinConditions = new LinkedList<>();
    private  Expression whereClause ;
    private  ProjectionJSQL projection;

    private boolean unsupported = false;
    //endregion

    public  ObdaVisitor(Select selectQuery, QuotedIDFactory idFac ){
        this.selectQuery = selectQuery;
        this.idFac = idFac;

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
    public ProjectionJSQL getProjection() {
        return projection;
    }

    /**
     *
     * @return true if the expression is supported
     */
    public boolean isSupported() {
        // used to throw exception for the currently unsupported methods
        return !unsupported;
    }
    //endregion

    private void unsupported(Object o) {
        System.out.println(this.getClass() + " DOES NOT SUPPORT " + o);
        unsupported = true;
    }

    private SelectVisitor selectVisitor = new SelectVisitor() {
        /*
         * visit PlainSelect, search for the where structure that returns an Expression
         * @see net.sf.jsqlparser.statement.select.SelectVisitor#visit(net.sf.jsqlparser.statement.select.PlainSelect)
         */
        @Override
        public void visit(PlainSelect plainSelect) {
            // The filed should be suitable for datalog expression
            validatePlainSelect(plainSelect);
            //
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
        private  void  validatePlainSelect(PlainSelect plainSelect){
            for (String sField : invalidObdaFields){
                try {
                    Method method =  plainSelect.getClass().getDeclaredMethod(sField, null );
                    Object val = method.invoke( plainSelect, null );
                    if ( method.getReturnType().equals(boolean.class) ){
                        if (Boolean.parseBoolean( val.toString()) ){
                            unsupported(method);
                            break;
                        }
                    }else if ( val != null ){
                        unsupported(method);
                        break;
                    }
                } catch (NoSuchMethodException e) {
                   log.error("NoSuchMethodException on validatePlainSelect", e);
                } catch (InvocationTargetException e) {
                    log.error("InvocationTargetException on validatePlainSelect", e);
                } catch (IllegalAccessException e) {
                    log.error("IllegalAccessException on validatePlainSelect", e);
                }
            }
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
