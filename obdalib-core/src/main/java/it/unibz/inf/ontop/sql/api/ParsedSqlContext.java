package it.unibz.inf.ontop.sql.api;

import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.sql.*;

import java.util.*;

/**
 * Created by Salvatore Rapisarda
 *  on 27/08/2016.
 *
 */
public class ParsedSqlContext {

    public DBMetadata getMetadata() {
        return metadata;
    }

    public QuotedIDFactory getIdFac() {
        return idFac;
    }

    private final DBMetadata metadata;
    private final QuotedIDFactory idFac;

    public List<ParsedSqlContext> getChildContext() {
        return childContext;
    }

    List<ParsedSqlContext> childContext;


    private final Map<ImmutableList<RelationID>, DatabaseRelationDefinition> relationAliasMap = new LinkedHashMap<>();

    public Map<ImmutableList<RelationID>, DatabaseRelationDefinition> getRelationAliasMap() {
        return relationAliasMap;
    }

    private final Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> relationAttributesMap = new LinkedHashMap<>();
    public Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> getRelationAttributesMap() {
        return relationAttributesMap;
    }

    private final Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> attributeAliasMap = new LinkedHashMap<>();

    public Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> getAttributeAliasMap() {
        return attributeAliasMap;
    }
    private final Set<RelationID>  tables = new HashSet<>();
    public Set<RelationID> getTables() {
        return tables;
    }

    public ParsedSqlContext(DBMetadata metadata){
        this.metadata = metadata;
        this.idFac = metadata.getQuotedIDFactory();
    }


}
