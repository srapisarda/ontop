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

import it.unibz.inf.ontop.sql.*;

import java.util.*;

/**
 * Created by Salvatore Rapisarda
 *  on 27/08/2016.
 *
 */
public class ParsedSqlContext {

    private final DBMetadata metadata;
    private final QuotedIDFactory idFac;
    private final Set<RelationID> globalTables = new HashSet<>();
    private final Map<RelationID, DatabaseRelationDefinition> relations = new HashMap<>();
    private final Map<QuotedID, QualifiedAttributeID> projectedAttributes = new HashMap<>();
    private final Map<QuotedID, QualifiedAttributeID> attributes = new HashMap<>();
    private Map<QuotedID, ParsedSqlContext> childContext = new LinkedHashMap<>();
    private QuotedID alias;

    public ParsedSqlContext(DBMetadata metadata){
        this.metadata = metadata;
        this.idFac = metadata.getQuotedIDFactory();
    }

    //region properties
    public DBMetadata getMetadata() {
        return metadata;
    }

    public QuotedIDFactory getIdFac() {
        return idFac;
    }

    public Map<QuotedID, ParsedSqlContext> getChildContext() {
        return childContext;
    }

    public void setChildContext(Map<QuotedID, ParsedSqlContext> childContext) {
        this.childContext = childContext;
    }

    public Set<RelationID> getGlobalTables() {
        return globalTables;
    }

    public QuotedID getAlias() {
        return alias;
    }

    public void setAlias(QuotedID alias) {
        this.alias = alias;
    }

    public Map<RelationID, DatabaseRelationDefinition> getRelations() {
        return relations;
    }

    public Map<QuotedID, QualifiedAttributeID> getProjectedAttributes() {
        return projectedAttributes;
    }

    public Map<QuotedID, QualifiedAttributeID> getAttributes() {
        return attributes;
    }
    //endregion

}
