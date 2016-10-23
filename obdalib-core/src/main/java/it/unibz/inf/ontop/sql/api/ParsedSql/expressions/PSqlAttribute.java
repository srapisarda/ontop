package it.unibz.inf.ontop.sql.api.ParsedSql.expressions;

import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins.ParsedSqlExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitor;

/**
 * Created by Salvatore Rapisarda on 17/09/2016.
 *
 */
public class PSqlAttribute implements PSqlExpression {

    private QualifiedAttributeID attributeID;
    private Object value;


    PSqlAttribute(QualifiedAttributeID attributeID) {
        this.attributeID = attributeID;
    }

    PSqlAttribute(String value){
        this.value =value;
    }

    public boolean hasAttributeId(){
        return attributeID!=null ;
    }


    public Object getValue() {
        return hasAttributeId()? attributeID :  value;
    }

    public QualifiedAttributeID getAttributeID() {
        return attributeID;
    }


    @Override
    public void accept(ExpressionVisitor expressionVisitor) {
    }

    @Override
    public void accept(ParsedSqlExpressionVisitor expressionVisitor) {

    }
    @Override
    public String toString(){
        if ( this.attributeID != null)
            return this.attributeID.toString();
        else
            return this.value.toString();
    }
}
