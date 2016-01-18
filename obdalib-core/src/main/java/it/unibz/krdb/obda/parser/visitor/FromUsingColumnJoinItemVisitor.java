package it.unibz.krdb.obda.parser.visitor;

import it.unibz.krdb.obda.parser.SQLQueryParser;
import it.unibz.krdb.obda.parser.exception.MappingQueryException;
import it.unibz.krdb.obda.parser.exception.ParseException;
import it.unibz.krdb.sql.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by salvo on 18/01/2016.
 */
public class FromUsingColumnJoinItemVisitor implements FromItemVisitor {
    private final DBMetadata dbMetadata;
    private final QuotedIDFactory idFac;
    private final List<Column> joinUsingColumns;
    private final Map<QualifiedAttributeID, Attribute> fromAttributesIds;
    private final List<Expression> joinConditions;
    private final ExpressionVisitor joinExpressionVisitor;


    public FromUsingColumnJoinItemVisitor(DBMetadata dbMetadata, List<Column> joinUsingColumns,
                                          Map<QualifiedAttributeID, Attribute> fromAttributesIds,
                                          List<Expression> joinConditions, ExpressionVisitor joinExpressionVisitor) {
        this.dbMetadata = dbMetadata;
        this.joinConditions = joinConditions;
        this.joinExpressionVisitor = joinExpressionVisitor;
        this.idFac = this.dbMetadata.getQuotedIDFactory();
        this.joinUsingColumns = joinUsingColumns;
        this.fromAttributesIds = fromAttributesIds;

    }

    /**
     * this is used to visit join using columns
     *
     * @param table join
     **/
    @Override
    public void visit(Table table) {

        RelationID relationId = idFac.createRelationID(table.getSchemaName(), table.getName());
        RelationDefinition relation = dbMetadata.getRelation(relationId);
        if (relation == null)
            throw new MappingQueryException("Relation does not exist", relationId);

        Map<QualifiedAttributeID, Attribute> rightAttributes = new HashMap<>();
        for (Attribute attribute : relation.getAttributes())
            rightAttributes.put(attribute.getQualifiedID(), attribute);

        for (Column column : joinUsingColumns) {
            QuotedID attributeID = idFac.createAttributeID(column.getColumnName());
            QualifiedAttributeID rightColumnId = new QualifiedAttributeID(relationId, attributeID);
            QualifiedAttributeID leftColumnId = new QualifiedAttributeID(null, attributeID);
            if (fromAttributesIds.containsKey(leftColumnId) && rightAttributes.containsKey(rightColumnId)) {
                Attribute rightAttribute = rightAttributes.get(rightColumnId);
                Attribute leftAttribute = fromAttributesIds.get(leftColumnId);
                if (leftAttribute == null)
                    throw new MappingQueryException("Ambiguous attribute", leftAttribute); // ambiguity

                SQLQueryParser.addNewBinaryJoinCondition(leftAttribute, rightAttribute, leftAttribute.getID().getName(),
                        new EqualsTo(), joinExpressionVisitor, joinConditions);
            } else
                throw new MappingQueryException("Ambiguous attribute", rightAttributes);
        }

    }

    @Override
    public void visit(SubSelect subSelect) {
        // todo : for now it rises an exception but this should be implemented for query such as: SELECT * FROM ...
        throw new ParseException(subSelect);
    }

    @Override
    public void visit(SubJoin subjoin) {
        throw new ParseException(subjoin);
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new ParseException(lateralSubSelect);
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new ParseException(valuesList);
    }

}
