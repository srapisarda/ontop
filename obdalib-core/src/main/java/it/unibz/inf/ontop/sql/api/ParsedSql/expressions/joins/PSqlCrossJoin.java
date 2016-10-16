package it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins;

import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlExpression;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlContext;
import net.sf.jsqlparser.expression.ExpressionVisitor;

/**
 * Created by Salvatore Rapisarda on 30/09/2016.
 *
 */
public class PSqlCrossJoin implements PSqlExpression {
    final PSqlContext context;

    public PSqlCrossJoin(PSqlContext context) {
        this.context = context;
    }

    @Override
    public void accept(PSqlExpressionVisitor expressionVisitor) {
        
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {

    }
}
