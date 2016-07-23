package it.unibz.inf.ontop.sql.api.visitors;

import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.sql.DBMetadata;
import it.unibz.inf.ontop.sql.QuotedID;
import it.unibz.inf.ontop.sql.QuotedIDFactoryIdentity;
import it.unibz.inf.ontop.sql.RelationID;
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

    // TODO: This is not correct stucture should be change to  Map<Pair<ImmutableList<RelationID>,QualifiedAttributeID>>, Attribute>
    private final Map<ImmutableList<RelationID>, QuotedID> attributeAliasMap;

    ParsedSQLItemVisitor(DBMetadata metadata){
        attributeAliasMap =new LinkedHashMap<>();
        this.metadata = metadata;
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
        String  alias = selectExpressionItem.getAlias() != null ? selectExpressionItem.getAlias().getName() : selectExpressionItem.toString() ;
        ImmutableList<RelationID> key =  ImmutableList.<RelationID>builder().add(RelationID.createRelationIdFromDatabaseRecord(metadata.getQuotedIDFactory(), null,  selectExpressionItem.toString() )).build();
        attributeAliasMap.put(key,
                new QuotedIDFactoryIdentity(
                        selectExpressionItem.toString()).createAttributeID(alias) ) ;
    }


    // TODO: This is not correct stucture should be change to  Map<Pair<ImmutableList<RelationID>,QualifiedAttributeID>>, Attribute>
    public Map<ImmutableList<RelationID>,QuotedID> getAttributeAliasMap() {
        return attributeAliasMap;
    }
}
