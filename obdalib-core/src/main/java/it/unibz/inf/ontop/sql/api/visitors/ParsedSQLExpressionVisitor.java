package it.unibz.inf.ontop.sql.api.visitors;

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


import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.QuotedID;
import it.unibz.inf.ontop.sql.RelationID;
import it.unibz.inf.ontop.sql.api.expressions.ParsedSqlAttribute;
import it.unibz.inf.ontop.sql.api.expressions.ParsedSqlCondition;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Salvatore Rapisarda
 * on 04/08/2016.
 */




class ParsedSQLExpressionVisitor implements ExpressionVisitor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ImmutableList.Builder<Column> columnsListBuilder;
    private final ParsedSqlContext context;

    ParsedSQLExpressionVisitor(ParsedSqlContext context) {
        this.columnsListBuilder = new ImmutableList.Builder<>();
        this.context = context;
    }

    ImmutableList<Column> getColumns(){
        return columnsListBuilder.build();
    }

    @Override
    public void visit(NullValue nullValue) {
        logger.debug("Visit NullValue");

    }

    @Override
    public void visit(Function function) {
        logger.debug("Visit Function");
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        logger.debug("Visit signedExpression");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        logger.debug("Visit jdbcParameter");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        logger.debug("Visit jdbcNamedParameter");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        logger.debug("Visit doubleValue");
    }

    @Override
    public void visit(LongValue longValue) {
        logger.debug("Visit longValue");
    }

    @Override
    public void visit(DateValue dateValue) {
        logger.debug("Visit dateValue");
    }

    @Override
    public void visit(TimeValue timeValue) {
        logger.debug("Visit timeValue");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        logger.debug("Visit timestampValue");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        logger.debug("Visit parenthesis");
    }

    @Override
    public void visit(StringValue stringValue) {
        logger.debug("Visit stringValue");
    }

    @Override
    public void visit(Addition addition) {
        logger.debug("Visit addition");
    }

    @Override
    public void visit(Division division) {
        logger.debug("Visit division");
    }

    @Override
    public void visit(Multiplication multiplication) {
        logger.debug("Visit multiplication");
    }

    @Override
    public void visit(Subtraction subtraction) {
        logger.debug("Visit subtraction");
    }

    @Override
    public void visit(AndExpression andExpression) {
        logger.debug("Visit andExpression");
    }

    @Override
    public void visit(OrExpression orExpression) {
        logger.debug("Visit orExpression");
    }

    @Override
    public void visit(Between between) {
        logger.debug("Visit Between");
    }


    private ParsedSqlAttribute getAttributeFromColumn( Column column){
        final QuotedID attributeID = context.getMetadata().getQuotedIDFactory().createAttributeID(column.getColumnName());
        final RelationID relationID = context.getMetadata().getQuotedIDFactory().createRelationID(null, column.getTable().getName());
        QualifiedAttributeID qualifiedAttributeID = new QualifiedAttributeID(relationID, attributeID );
        return  new ParsedSqlAttribute(qualifiedAttributeID);
    }

    private void addAttributeIfColumn(OldOracleJoinBinaryExpression expression ){
        ParsedSQLExpressionVisitor visitor = new ParsedSQLExpressionVisitor(context);
        expression.getLeftExpression().accept( visitor);

        Expression rightExpression;
        if  ( ! visitor.getColumns().isEmpty() )
            rightExpression =  getAttributeFromColumn ( visitor.getColumns().get(0) );
        else
            rightExpression = expression.getRightExpression();


        visitor = new ParsedSQLExpressionVisitor(context);
        expression.getRightExpression().accept( visitor);
        Expression leftExpression;
        if  ( ! visitor.getColumns().isEmpty() )
            leftExpression =  getAttributeFromColumn ( visitor.getColumns().get(0) );
        else
            leftExpression = expression.getLeftExpression();

        ParsedSqlCondition condition = new ParsedSqlCondition(expression, rightExpression, leftExpression);

        context.getJoins().add(  condition  );

    }


    @Override
    public void visit(EqualsTo equalsTo) {
        logger.debug("Visit EqualsTo");
        addAttributeIfColumn(equalsTo);

    }

    @Override
    public void visit(GreaterThan greaterThan) {
        logger.debug("Visit GreaterThan");
        addAttributeIfColumn(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        logger.debug("Visit GreaterThanEquals");
        addAttributeIfColumn(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        logger.debug("Visit InExpression");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        logger.debug("Visit IsNullExpression");
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        logger.debug("Visit LikeExpression");
    }

    @Override
    public void visit(MinorThan minorThan) {
        logger.debug("Visit MinorThan");
        addAttributeIfColumn(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        logger.debug("Visit MinorThanEquals");
        addAttributeIfColumn(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        logger.debug("Visit NotEqualsTo");
        addAttributeIfColumn(notEqualsTo);
    }

    @Override
    public void visit(Column tableColumn) {
        logger.debug("Visit Column ", tableColumn);
        columnsListBuilder.add(tableColumn);
        //tableColumn.accept(this);
    }

    @Override
    public void visit(SubSelect subSelect) {
        logger.debug("Visit SubSelect");
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        logger.debug("Visit CaseExpression");
    }

    @Override
    public void visit(WhenClause whenClause) {
        logger.debug("Visit WhenClause");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        logger.debug("Visit ExistsExpression");
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        logger.debug("Visit AllComparisonExpression");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        logger.debug("Visit AnyComparisonExpression");
    }

    @Override
    public void visit(Concat concat) {
        logger.debug("Visit Concat");
    }

    @Override
    public void visit(Matches matches) {
        logger.debug("Visit Matches");
        addAttributeIfColumn(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        logger.debug("Visit BitwiseAnd");
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        logger.debug("Visit BitwiseOr");
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        logger.debug("Visit BitwiseXor");
    }

    @Override
    public void visit(CastExpression cast) {
        logger.debug("Visit CastExpression");
    }

    @Override
    public void visit(Modulo modulo) {
        logger.debug("Visit Modulo");
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        logger.debug("Visit AnalyticExpression");
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        logger.debug("Visit ExtractExpression");
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        logger.debug("Visit IntervalExpression");
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        logger.debug("Visit OracleHierarchicalExpression");
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        logger.debug("Visit RegExpMatchOperator");
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        logger.debug("Visit JsonExpression");
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        logger.debug("Visit RegExpMySQLOperator");
    }
}
