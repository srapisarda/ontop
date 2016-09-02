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

    public Map<QuotedID, ParsedSqlContext> getChildContext() {
        return childContext;
    }

    public void setChildContext(Map<QuotedID, ParsedSqlContext> childContext) {
        this.childContext = childContext;
    }

    private Map<QuotedID, ParsedSqlContext> childContext = new LinkedHashMap<>();


    private final Map<ImmutableList<RelationID>, DatabaseRelationDefinition> relationAliasMap = new LinkedHashMap<>();

    public Map<ImmutableList<RelationID>, DatabaseRelationDefinition> getRelationAliasMap() {
        return relationAliasMap;
    }

//    private final Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> relationAttributesMap = new LinkedHashMap<>();
//    public Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> getRelationAttributesMap() {
//        return relationAttributesMap;
//    }

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

    public QuotedID getAlias() {
        return alias;
    }

    public void setAlias(QuotedID alias) {
        this.alias = alias;
    }

    QuotedID alias;

    //region predicates atoms scope

    public Map<RelationID, DatabaseRelationDefinition> getRelations() {
        return relations;
    }

    public Map<ParsedSqlPair<RelationID, QualifiedAttributeID>, QuotedID> getProjectedAttributes() {
        return projectedAttributes;
    }

    public Map<ParsedSqlPair<RelationID, QualifiedAttributeID>, QuotedID> getAttributes() {
        return attributes;
    }

    // scope  relations
    public final Map<RelationID, DatabaseRelationDefinition> relations = new HashMap<>();

    // scope attribute projected
    public final Map<ParsedSqlPair<RelationID, QualifiedAttributeID>, QuotedID > projectedAttributes = new HashMap<>();

    // scope all attribute
    public final Map<ParsedSqlPair<RelationID, QualifiedAttributeID>, QuotedID > attributes = new HashMap<>();

    //endregion

}
