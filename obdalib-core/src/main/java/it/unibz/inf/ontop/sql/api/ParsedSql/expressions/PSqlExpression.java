package it.unibz.inf.ontop.sql.api.ParsedSql.expressions;

import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins.PSqlExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;

/**
 * Created by rapissal on 30/09/2016.
 *
 */
public interface PSqlExpression extends Expression {
    void accept(PSqlExpressionVisitor expressionVisitor);
}
