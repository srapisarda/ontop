package it.unibz.inf.ontop.sql.api.ParsedSql.visitors;

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
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlCondition;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;

/**
 * Created by Salvatore Rapisarda
 *  on 27/08/2016.
 *
 */
public class PSqlContext {

    private final DBMetadata metadata;
    private final QuotedIDFactory idFac;
    private final Map<RelationID, DatabaseRelationDefinition> relations = new HashMap<>();
    private final Map<QualifiedAttributeID, QualifiedAttributeID> projectedAttributes = new HashMap<>();
    private final Map<QualifiedAttributeID, QualifiedAttributeID> tableAttributes = new HashMap<>();
    private final Map<QualifiedAttributeID, QualifiedAttributeID> attributes = new HashMap<>();
    private final Map<QualifiedAttributeID, QualifiedAttributeID> joinAttributes = new HashMap<>();
    private final List<Expression> joins = new LinkedList<>();
    private final List<PSqlCondition> conditions = new ArrayList<>();

    private Map<QuotedID, PSqlContext> childContext = new LinkedHashMap<>();
    private QuotedID alias;

    PSqlContext(DBMetadata metadata){
        this.metadata = metadata;
        this.idFac = metadata.getQuotedIDFactory();
    }

    PSqlContext(DBMetadata metadata, QuotedID contextAlias) {
        this(metadata);
        this.alias = contextAlias;
    }

    //region properties
    public DBMetadata getMetadata() {
        return metadata;
    }

    QuotedIDFactory getIdFac() {
        return idFac;
    }

    public Map<QuotedID, PSqlContext> getChildContext() {
        return childContext;
    }


    public QuotedID getAlias() {
        return alias;
    }

    public Map<RelationID, DatabaseRelationDefinition> getRelations() {
        return relations;
    }

    public Map<QualifiedAttributeID, QualifiedAttributeID> getProjectedAttributes() {
        return projectedAttributes;
    }

    public Map<QualifiedAttributeID, QualifiedAttributeID> getTableAttributes() {
        return tableAttributes;
    }

    public Map<QualifiedAttributeID, QualifiedAttributeID> getAttributes() {
        return attributes;
    }

    public List<Expression> getJoins() {
        return joins;
    }

    public Map<QualifiedAttributeID, QualifiedAttributeID> getJoinAttributes() {
        return joinAttributes;
    }

    public List<PSqlCondition> getConditions() {
        return conditions;
    }

    //endregion

}
