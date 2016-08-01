package it.unibz.inf.ontop.sql.api.visitors;

import com.google.common.collect.ImmutableList;
import com.sun.tools.javac.util.Pair;
import it.unibz.inf.ontop.sql.*;
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
    private final DBMetadata metadata;
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private final RelationID relationID;
    private final Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> attributeAliasMap;

    Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> getAttributeAliasMap() {
        return attributeAliasMap;
    }


    ParsedSQLItemVisitor(DBMetadata metadata, RelationID relationID){
        attributeAliasMap =new LinkedHashMap<>();
        this.metadata = metadata;
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

        ImmutableList.Builder<RelationID> b =  ImmutableList.builder();

       // ImmutableList<RelationID> key = this.relationID == null ? null : ImmutableList.of(this.relationID);
        if (this.relationID == null )
            b.add(RelationID.createRelationIdFromDatabaseRecord(metadata.getQuotedIDFactory() , null,  "" ));
        else
            b.add( this.relationID);

        if ( selectExpressionItem.getAlias() != null )
            b.add(RelationID.createRelationIdFromDatabaseRecord(metadata.getQuotedIDFactory() , null,  selectExpressionItem.getAlias().getName().toString() ));

        QuotedID quotedID = metadata.getQuotedIDFactory().createAttributeID(selectExpressionItem.getExpression().toString() );
        Pair<ImmutableList<RelationID>,QualifiedAttributeID> pair = new Pair<>(b.build(), new QualifiedAttributeID( relationID, quotedID));
        attributeAliasMap.put( pair, quotedID );
    }



}
