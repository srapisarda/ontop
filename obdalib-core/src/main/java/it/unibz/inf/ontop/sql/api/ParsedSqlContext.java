package it.unibz.inf.ontop.sql.api;

/*
 * #%L
 * ontop-obdalib-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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


    private final Map<ImmutableList<RelationID>, DatabaseRelationDefinition> globalRelations = new LinkedHashMap<>();

    public Map<ImmutableList<RelationID>, DatabaseRelationDefinition> getGlobalRelations() {
        return globalRelations;
    }

//    private final Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> relationAttributesMap = new LinkedHashMap<>();
//    public Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> getRelationAttributesMap() {
//        return relationAttributesMap;
//    }

    private final Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> globalProjectedAttributes = new LinkedHashMap<>();

    public Map<ParsedSqlPair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> getGlobalProjectedAttributes() {
        return globalProjectedAttributes;
    }
    private final Set<RelationID> globalTables = new HashSet<>();
    public Set<RelationID> getGlobalTables() {
        return globalTables;
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
