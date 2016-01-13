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
public class WhereExpressionVisitor implements ExpressionVisitor {
    private final QuotedIDFactory idFac;

    public WhereExpressionVisitor(QuotedIDFactory idFac) {
        this.idFac = idFac;
    }


    @Override
        public void visit(NullValue nullValue) {
            // we do not execute anything
        }

        @Override
        public void visit(Function function) {
            // ROMAN (22 Sep 2015): longer list of supported functions?
            if (function.getName().toLowerCase().equals("regexp_like")) {
                for (Expression ex :function.getParameters().getExpressions())
                    ex.accept(this);
            }
            else
                throw new ParseException(function);
        }

        @Override
        public void visit(JdbcParameter jdbcParameter) {
            //we do not execute anything
        }

        @Override
        public void visit(JdbcNamedParameter jdbcNamedParameter) {
            // we do not execute anything
        }

        @Override
        public void visit(DoubleValue doubleValue) {
            // we do not execute anything
        }

        @Override
        public void visit(LongValue longValue) {
            // we do not execute anything
        }

        @Override
        public void visit(DateValue dateValue) {
            // we do not execute anything
        }

        @Override
        public void visit(TimeValue timeValue) {
            // we do not execute anything
        }

        @Override
        public void visit(TimestampValue timestampValue) {
            // we do not execute anything
        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);
        }

        @Override
        public void visit(StringValue stringValue) {
            // we do not execute anything
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
        public void visit(AndExpression andExpression) {
            visitBinaryExpression(andExpression);
        }

        @Override
        public void visit(OrExpression orExpression) {
            visitBinaryExpression(orExpression);
        }

        @Override
        public void visit(Between between) {
            between.getLeftExpression().accept(this);
            between.getBetweenExpressionStart().accept(this);
            between.getBetweenExpressionEnd().accept(this);
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            visitBinaryExpression(equalsTo);
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

            //Expression e = inExpression.getLeftExpression();

            // ROMAN (25 Sep 2015): why not getLeftExpression? getLeftItemList can be empty
            // what about the right-hand side list?!

            ItemsList e1 = inExpression.getLeftItemsList();
            if (e1 instanceof SubSelect) {
                ((SubSelect)e1).accept((ExpressionVisitor)this);
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

        /*
         * We add the content of isNullExpression in SelectionJSQL
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.InExpression)
         */
        @Override
        public void visit(IsNullExpression isNullExpression) {

        }

        @Override
        public void visit(LikeExpression likeExpression) {
            visitBinaryExpression(likeExpression);
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
        public void visit(NotEqualsTo notEqualsTo) {
            visitBinaryExpression(notEqualsTo);
        }

    	/*
    	 * Visit the column and remove the quotes if they are present(non-Javadoc)
    	 * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
    	 */

        @Override
        public void visit(Column tableColumn) {
            // CHANGES THE TABLE SCHEMA / NAME AND COLUMN NAME
            SQLQueryParser.normalizeColumnName(idFac, tableColumn);
        }

        /*
         * we search for nested where in SubSelect
         * @see net.sf.jsqlparser.statement.select.FromItemVisitor#visit(net.sf.jsqlparser.statement.select.SubSelect)
         */
        @Override
        public void visit(SubSelect subSelect) {

            // todo: this is just a draft
            if (!(subSelect.getSelectBody() instanceof PlainSelect))
                throw new ParseException(subSelect);

            PlainSelect subSelBody = (PlainSelect)subSelect.getSelectBody();

            // only very simple subqueries are supported at the moment
            if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
                throw new ParseException(subSelect);

        }

        @Override
        public void visit(CaseExpression caseExpression) {
            // it is not supported

        }

        @Override
        public void visit(WhenClause whenClause) {
            // it is not supported

        }

        @Override
        public void visit(ExistsExpression existsExpression) {
            // it is not supported

        }

        /*
         * We add the content of AllComparisonExpression in SelectionJSQL
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AllComparisonExpression)
         */
        @Override
        public void visit(AllComparisonExpression allComparisonExpression) {

        }

        /*
         * We add the content of AnyComparisonExpression in SelectionJSQL
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnyComparisonExpression)
         */
        @Override
        public void visit(AnyComparisonExpression anyComparisonExpression) {

        }

        @Override
        public void visit(Concat concat) {
            visitBinaryExpression(concat);
        }

        @Override
        public void visit(Matches matches) {
            visitBinaryExpression(matches);
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            visitBinaryExpression(bitwiseAnd);
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            visitBinaryExpression(bitwiseOr);
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            visitBinaryExpression(bitwiseXor);
        }

        @Override
        public void visit(Modulo modulo) {
            visitBinaryExpression(modulo);
        }


        @Override
        public void visit(CastExpression cast) {
            // not supported
        }

        @Override
        public void visit(AnalyticExpression aexpr) {
            // not supported
        }

        @Override
        public void visit(ExtractExpression eexpr) {
            // not supported
        }

        @Override
        public void visit(IntervalExpression iexpr) {
            // not supported
        }

        @Override
        public void visit(OracleHierarchicalExpression oexpr) {
            //not supported
        }

    	/*
    	 * We handle differently AnyComparisonExpression and AllComparisonExpression
    	 *  since they do not have a toString method, we substitute them with ad hoc classes.
    	 *  we continue to visit the subexpression.
    	 */

        private void visitBinaryExpression(BinaryExpression binaryExpression) {

            Expression left = binaryExpression.getLeftExpression();
            Expression right = binaryExpression.getRightExpression();

            if (right instanceof AnyComparisonExpression){
                right = new AnyComparison(((AnyComparisonExpression) right).getSubSelect());
                binaryExpression.setRightExpression(right);
            }

            if (right instanceof AllComparisonExpression){
                right = new AllComparison(((AllComparisonExpression) right).getSubSelect());
                binaryExpression.setRightExpression(right);
            }

            left.accept(this);
            right.accept(this);
        }

        @Override
        public void visit(SignedExpression arg0) {
            // do nothing
        }

        @Override
        public void visit(RegExpMatchOperator arg0) {
            // do nothing
        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
            // do nothing
        }

        @Override
        public void visit(JsonExpression arg0) {
            throw new ParseException(arg0);
        }




    //region Auxiliary Classes

    /**
     * Auxiliary Class used to visualize AnyComparison in string format.
     * Any and Some are the same in SQL so we consider always the case of ANY
     *
     */
    private final static class AllComparison extends AllComparisonExpression {

        public AllComparison(SubSelect subSelect) {
            super(subSelect);
        }

        @Override
        public String toString(){
            return "ALL "+ getSubSelect();
        }
    }

    private final static class AnyComparison extends AnyComparisonExpression {

        public AnyComparison(SubSelect subSelect) {
            super(subSelect);
        }

        @Override
        public String toString(){
            return "ANY "+ getSubSelect();
        }

    }
    //endregion

}


