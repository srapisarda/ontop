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

import java.util.List;

/**
 * Created by salvo on 13/01/2016.
 */
public class JoinExpressionVisitor implements ExpressionVisitor {
    private final QuotedIDFactory idFac;

    public List<Expression> getJoinConditions() {
        return joinConditions;
    }

    private final List<Expression> joinConditions;


    public JoinExpressionVisitor(QuotedIDFactory idFac, List<Expression> joinConditions) {
        this.idFac = idFac;
        this.joinConditions = joinConditions;
    }


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


        /**
         * We store in join conditions the binary expression that are not nested,
         * for the others we continue to visit the subexpression
         * Example: AndExpression and OrExpression always have subexpression.
         */
        public void visitBinaryExpression(BinaryExpression binaryExpression) {
            Expression left = binaryExpression.getLeftExpression();
            Expression right = binaryExpression.getRightExpression();

            if (!( (left instanceof BinaryExpression) ||
                    (right instanceof BinaryExpression) )) {

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
            SQLQueryParser.normalizeColumnName(idFac, tableColumn);
        }

        /*
         * We visit also the subselect to find nested joins
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.statement.select.SubSelect)
         */
        @Override
        public void visit(SubSelect subSelect) {

            if (!(subSelect.getSelectBody() instanceof PlainSelect))
                throw new ParseException(subSelect);

            PlainSelect subSelBody = (PlainSelect)subSelect.getSelectBody();

            // only very simple subqueries are supported at the moment
            if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
                throw new ParseException(subSelect);

            // TODO: Review the code above
            //  subSelBody.accept(selectVisitor); TODO: this has been commented out because selectVisitor dependency unresolved

        }

        @Override
        public void visit(CaseExpression arg0) {
            // we do not support case expression
            throw new ParseException(arg0);

        }

        @Override
        public void visit(WhenClause arg0) {
            // we do not support when expression
            throw new ParseException(arg0);
        }

        @Override
        public void visit(ExistsExpression exists) {
            // we do not support exists
            throw new ParseException(exists);
        }

        /*
         * We visit the subselect in ALL(...)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AllComparisonExpression)
         */
        @Override
        public void visit(AllComparisonExpression all) {
            throw new ParseException(all);
        }

        /*
         * We visit the subselect in ANY(...)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnyComparisonExpression)
         */
        @Override
        public void visit(AnyComparisonExpression any) {
            throw new ParseException(any);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Concat arg0) {
            throw new ParseException(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Matches arg0) {
            throw new ParseException(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseAnd arg0) {
            throw new ParseException(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseOr arg0) {
            throw new ParseException(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseXor arg0) {
            throw new ParseException(arg0);
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
            throw new ParseException(arg0);
        }

        @Override
        public void visit(AnalyticExpression arg0) {
            // we do not consider AnalyticExpression
            throw new ParseException(arg0);
        }

        @Override
        public void visit(ExtractExpression arg0) {
            // we do not consider ExtractExpression
            throw new ParseException(arg0);
        }

        @Override
        public void visit(IntervalExpression arg0) {
            // we do not consider IntervalExpression
            throw new ParseException(arg0);
        }

        @Override
        public void visit(OracleHierarchicalExpression arg0) {
            // we do not consider OracleHierarchicalExpression
            throw new ParseException(arg0);
        }


        @Override
        public void visit(RegExpMatchOperator arg0) {
            throw new ParseException(arg0);
        }

        @Override
        public void visit(SignedExpression arg0) {
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
    }



