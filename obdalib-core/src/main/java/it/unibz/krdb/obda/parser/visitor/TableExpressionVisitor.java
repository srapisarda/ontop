package it.unibz.krdb.obda.parser.visitor;

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

import it.unibz.krdb.obda.parser.exception.ParseException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;


public class TableExpressionVisitor implements ExpressionVisitor {

    private final ItemsListVisitor itemsListVisitor;

    public TableExpressionVisitor(ItemsListVisitor itemsListVisitor) {
        this.itemsListVisitor = itemsListVisitor;
    }

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
                throw new ParseException(function);
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
        throw new ParseException(jdbcParameter);
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
        throw new ParseException(caseExpression);
    }

    @Override
    public void visit(WhenClause whenClause) {
        throw new ParseException(whenClause);
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        throw new ParseException(allComparisonExpression);
        //allComparisonExpression.getSubSelect().getSelectBody().accept(selectVisitor);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new ParseException(anyComparisonExpression);
        //anyComparisonExpression.getSubSelect().getSelectBody().accept(selectVisitor);
    }

    @Override
    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        throw new ParseException(matches);
        //visitBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        throw new ParseException(bitwiseAnd);
        //visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        throw new ParseException(bitwiseOr);
        //visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new ParseException(bitwiseXor);
        //visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        throw new ParseException(modulo);
        //visitBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression analytic) {
        throw new ParseException(analytic);
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        throw new ParseException(eexpr);
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        throw new ParseException(iexpr);
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        throw new ParseException(jdbcNamedParameter);
    }

    @Override
    public void visit(OracleHierarchicalExpression arg0) {
        throw new ParseException(arg0);
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
//			unsupported = true;
//			visitBinaryExpression(rexpr);
    }


    @Override
    public void visit(SignedExpression arg0) {
        System.out.println("WARNING: SignedExpression   not implemented ");
        throw new ParseException(arg0);
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

        //visitSubSelect(subSelect);
        if (!(subSelect.getSelectBody() instanceof PlainSelect))
            throw new ParseException(subSelect);

        PlainSelect subSelBody = (PlainSelect)subSelect.getSelectBody();

        // only very simple subqueries are supported at the moment
        if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
            throw new ParseException(subSelect);

        // TODO: Review the code above
        //  subSelBody.accept(selectVisitor); TODO: this has been commented out because selectVisitor dependency unresolved
    }



}
