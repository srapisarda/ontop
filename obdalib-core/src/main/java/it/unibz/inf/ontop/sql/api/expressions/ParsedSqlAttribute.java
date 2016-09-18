package it.unibz.inf.ontop.sql.api.expressions;

import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;

/**
 * Created by Salvatore Rapisarda on 17/09/2016.
 *
 */
public class ParsedSqlAttribute implements Expression {

    private final QualifiedAttributeID attributeID;

    public ParsedSqlAttribute(QualifiedAttributeID attributeID) {
        this.attributeID = attributeID;
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {
    }


    public QualifiedAttributeID getAttributeID() {
        return attributeID;
    }

}
