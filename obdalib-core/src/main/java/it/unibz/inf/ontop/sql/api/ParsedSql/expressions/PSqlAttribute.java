package it.unibz.inf.ontop.sql.api.ParsedSql.expressions;

import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins.PSqlExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitor;

/**
 * Created by Salvatore Rapisarda on 17/09/2016.
 *
 */
public class PSqlAttribute implements PSqlExpression {

    private final QualifiedAttributeID attributeID;

    public PSqlAttribute(QualifiedAttributeID attributeID) {
        this.attributeID = attributeID;
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {
    }


    public QualifiedAttributeID getAttributeID() {
        return attributeID;
    }

    @Override
    public void accept(PSqlExpressionVisitor expressionVisitor) {

    }
}
