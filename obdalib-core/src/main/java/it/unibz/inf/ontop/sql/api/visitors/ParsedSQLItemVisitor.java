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
public class ParsedSQLItemVisitor implements SelectItemVisitor {
    private final DBMetadata metadata;
    Logger logger = LoggerFactory.getLogger(this.getClass());
    private final RelationID relationID;
    private final Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> attributeAliasMap;
    public Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> getAttributeAliasMap() {
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
      //  String  alias = selectExpressionItem.getAlias() != null ? selectExpressionItem.getAlias().getName() : "" ;

        ImmutableList<RelationID> key = this.relationID == null ? null :
                ImmutableList.<RelationID>builder()
                        .add(this.relationID)
                        .build();
        QuotedID quotedID = QuotedID.createIdFromDatabaseRecord ( metadata.getQuotedIDFactory(), selectExpressionItem.toString());
        Pair<ImmutableList<RelationID>,QualifiedAttributeID> pair =
                new Pair(key, new QualifiedAttributeID( relationID, quotedID));
        attributeAliasMap.put( pair, quotedID );
    }
    //javafx.util.Pair


}
