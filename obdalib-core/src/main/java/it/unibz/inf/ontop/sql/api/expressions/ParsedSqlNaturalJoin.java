package it.unibz.inf.ontop.sql.api.expressions;

import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.api.visitors.ParsedSqlContext;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by salvo on 17/09/2016.
 *
 */
public class ParsedSqlNaturalJoin extends BinaryExpression implements Expression {

    private final Set<QualifiedAttributeID> commonAttributes;


    public ParsedSqlNaturalJoin(ParsedSqlContext context) {
        commonAttributes = new HashSet<>();

        context.getAttributes().keySet().forEach( p ->
            commonAttributes.addAll(
                    context.getAttributes().keySet().stream().filter(q ->
                            p != q && p.getRelation() != null && q.getRelation() != null
                                    && ! p.getRelation().equals(q.getRelation() )
                                    &&  p.getAttribute().equals(q.getAttribute()) ).collect(Collectors.toSet()))
            );

    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {

    }
    @Override
    public String getStringExpression() {
        return "natural join";
    }

    public Set<QualifiedAttributeID> getCommonAttributes() {
        return commonAttributes;
    }

}
