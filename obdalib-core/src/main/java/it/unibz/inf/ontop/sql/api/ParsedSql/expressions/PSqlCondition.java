package it.unibz.inf.ontop.sql.api.ParsedSql.expressions;

import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins.PSqlExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;

/**
 * Created by salvo on 25/09/2016.
 *
 */
public class PSqlCondition extends OldOracleJoinBinaryExpression implements PSqlExpression {

    private final OldOracleJoinBinaryExpression expression;

    public PSqlCondition(OldOracleJoinBinaryExpression expression, Expression leftExpression, Expression rightExpression ){
        this.expression = expression;
        this.setLeftExpression(leftExpression);
        this.setRightExpression(rightExpression);

    }

    public boolean isNot () {
        return  expression.isNot();
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {
    }

    @Override
    public void accept(PSqlExpressionVisitor expressionVisitor) {

    }

    @Override
    public String getStringExpression() {
        return expression.getStringExpression();
    }
}
