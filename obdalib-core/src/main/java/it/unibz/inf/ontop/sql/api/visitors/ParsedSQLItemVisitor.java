package it.unibz.inf.ontop.sql.api.visitors;

import com.google.common.collect.ImmutableList;
import com.sun.tools.javac.util.Pair;
import it.unibz.inf.ontop.sql.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;


/**
 * @author Salvatore Rapisarda on 20/07/2016.
 */
class ParsedSQLItemVisitor implements SelectItemVisitor {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final DBMetadata metadata;
    private final QuotedIDFactory idFac;

    private final RelationID relationID;
    private final Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> attributeAliasMap = new LinkedHashMap<>();;

    Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> getAttributeAliasMap() {
        return attributeAliasMap;
    }


    ParsedSQLItemVisitor(DBMetadata metadata, RelationID relationID){
        this.metadata = metadata;
        this.idFac = metadata.getQuotedIDFactory();
        this.relationID = relationID;
    }

    @Override
    public void visit(AllColumns allColumns) {
        logger.info("visit AllColumns");

    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        logger.info("visit allTableColumns");
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        logger.info("visit selectExpressionItem");

        // TODO:  should support complex expressions
        final ParsedSQLExpressionVisitor parsedSQLExpressionVisitor = new ParsedSQLExpressionVisitor();
        selectExpressionItem.getExpression().accept(parsedSQLExpressionVisitor);
        parsedSQLExpressionVisitor
                .getColumns()
                .forEach(column -> addAttributeAliasMap(column.getColumnName(),
                        selectExpressionItem.getAlias() == null? column.getColumnName() : selectExpressionItem.getAlias().getName().toString(),
                        idFac.createRelationID(null,
                                column.getTable().getAlias() != null ? column.getTable().getAlias().getName() : column.getTable().getName())));

//        addAttributeAliasMap(
//                selectExpressionItem.getExpression().toString(),
//                selectExpressionItem.getAlias() == null?null: selectExpressionItem.getAlias().getName().toString(),
//                this.relationID);



    }


    private void addAttributeAliasMap(String attributeId, String alias, RelationID relationID) {
        ImmutableList.Builder<RelationID> b =  ImmutableList.builder();

        // R: why not ImmutableList.of ?!!
        if (relationID == null || relationID.getTableName() == null)
            b.add(idFac.createRelationID(null, "")); // ?????!
        else
            b.add(relationID);

        QuotedID quotedID = idFac.createAttributeID(attributeId);
        QuotedID quotedIdAlias  =  alias == null ?
                quotedID : idFac.createAttributeID(alias);

        Pair<ImmutableList<RelationID>,QualifiedAttributeID> pair = new Pair<>(b.build(), new QualifiedAttributeID(relationID, quotedIdAlias));

        attributeAliasMap.put(pair, quotedID);
    }
}
