package it.unibz.krdb.obda.parser.visitor;

import it.unibz.krdb.obda.parser.SQLQueryParser;
import it.unibz.krdb.obda.parser.exception.ParseException;
import it.unibz.krdb.sql.QuotedIDFactory;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Created by salvo on 13/01/2016.
 */
public class ProjectorExpressionVisitor implements ExpressionVisitor {
    private final QuotedIDFactory idFac;

    public ProjectorExpressionVisitor(QuotedIDFactory idFac) {
        this.idFac = idFac;
    }

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
                    throw new ParseException(function);
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
            throw new ParseException(andExpression);
        }

        @Override
        public void visit(OrExpression orExpression) {
            throw new ParseException(orExpression);
        }

        @Override
        public void visit(Between between) {
            between.getLeftExpression().accept(this);
            between.getBetweenExpressionStart().accept(this);
            between.getBetweenExpressionEnd().accept(this);
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            throw new ParseException(equalsTo);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            throw new ParseException(greaterThan);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            throw new ParseException(greaterThanEquals);
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
            throw new ParseException(isNullExpression);
        }

        @Override
        public void visit(LikeExpression likeExpression) {
            throw new ParseException(likeExpression);
        }

        @Override
        public void visit(MinorThan minorThan) {
            throw new ParseException(minorThan);
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            throw new ParseException(minorThanEquals);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            throw new ParseException(notEqualsTo);
        }

        /*
         * Visit the column and remove the quotes if they are present(non-Javadoc)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
         */
        @Override
        public void visit(Column tableColumn) {
            SQLQueryParser.normalizeColumnName(idFac, tableColumn);
        }

        @Override
        public void visit(SubSelect subSelect) {

            if (!(subSelect.getSelectBody() instanceof PlainSelect))
                throw new ParseException(subSelect);

            PlainSelect subSelBody = (PlainSelect)subSelect.getSelectBody();

            // only very simple subqueries are supported at the moment
            if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
                throw new ParseException(subSelect);
            // visitSubSelect(subSelect);
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
        public void visit(ExistsExpression existsExpression) {
            throw new ParseException(existsExpression);
        }

        @Override
        public void visit(AllComparisonExpression allComparisonExpression) {
            throw new ParseException(allComparisonExpression);
        }

        @Override
        public void visit(AnyComparisonExpression anyComparisonExpression) {
            throw new ParseException(anyComparisonExpression);
        }

        @Override
        public void visit(Concat concat) {
            visitBinaryExpression(concat);
        }

        @Override
        public void visit(Matches matches) {
            throw new ParseException(matches);
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            throw new ParseException(bitwiseAnd);
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            throw new ParseException(bitwiseOr);
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            throw new ParseException(bitwiseXor);
        }

        @Override
        public void visit(CastExpression cast) {


        }

        @Override
        public void visit(Modulo modulo) {
            throw new ParseException(modulo);
        }

        @Override
        public void visit(AnalyticExpression aexpr) {
            throw new ParseException(aexpr);
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
        public void visit(OracleHierarchicalExpression oexpr) {
            throw new ParseException(oexpr);
        }

        @Override
        public void visit(RegExpMatchOperator arg0) {
            throw new ParseException(arg0);
        }


        @Override
        public void visit(JsonExpression arg0) {
        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
        }

        @Override
        public void visit(JdbcParameter jdbcParameter) {
            throw new ParseException(jdbcParameter);
        }

        @Override
        public void visit(JdbcNamedParameter jdbcNamedParameter) {
            throw new ParseException(jdbcNamedParameter);
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
}
