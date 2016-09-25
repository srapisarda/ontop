package it.unibz.inf.ontop.sql.api.expressions;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;

/**
 * Created by salvo on 25/09/2016.
 *
 */
public class ParsedSqlCondition extends OldOracleJoinBinaryExpression implements Expression {

    private final OldOracleJoinBinaryExpression expression;

    public ParsedSqlCondition(OldOracleJoinBinaryExpression expression, Expression leftExpression, Expression rightExpression ){
        this.expression = expression;
        this.setLeftExpression(leftExpression);
        this.setRightExpression(rightExpression);
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {
    }

    @Override
    public String getStringExpression() {
        return expression.getStringExpression();
    }
}
