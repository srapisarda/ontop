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


import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.sql.DBMetadata;
import it.unibz.inf.ontop.sql.QuotedIDFactory;
import it.unibz.inf.ontop.sql.RelationID;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A structure to store the parsed SQL query string. It returns the information
 * about the query using the visitor classes
 */
public class ParsedSqlQueryVisitor implements  Serializable, FromItemVisitor, SelectVisitor {

    private static final long serialVersionUID = -1L;
    private final QuotedIDFactory idFac;
    private final DBMetadata metadata;

    private final List<RelationID> tables;
    public List<RelationID> getTables() {
        return tables;
    }


    private  final Logger logger =  LoggerFactory.getLogger(getClass());

    public ParsedSqlQueryVisitor(Select selectQuery, DBMetadata metadata){
        this.metadata = metadata;
        this.idFac = metadata.getQuotedIDFactory();
        this.tables = new ArrayList<>();
        if (selectQuery == null){return;}
        selectQuery.getSelectBody().accept(this);
    }


    // ****** FromItemVisitor implementation
    @Override
    public void visit(Table table) {
        logger.info("Visit Table");
        RelationID name =  RelationID.createRelationIdFromDatabaseRecord ( idFac,  table.getSchemaName(),  table.getName() );
        if ( metadata.getRelation( name ) != null ) {
            this.tables.add(name);
        }else {
            throw new MappingQueryException("the table " + table.getFullyQualifiedName() + " does not exist.", table);
        }


    }

    @Override
    public void visit(SubSelect subSelect) {
        logger.info("Visit SubSelect");
        if (!(subSelect.getSelectBody() instanceof PlainSelect))
            throw new ParseException(subSelect);

        PlainSelect subSelBody = (PlainSelect)subSelect.getSelectBody();

        // only very simple subqueries are supported at the moment
        if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
            throw new ParseException(subSelect);

        subSelBody.accept(this);

        plainSelectSelectJoin(subSelBody);

//        if (subSelBody.getWhere() != null)
//            subSelBody.getWhere().accept(this);
    }

    @Override
    public void visit(SubJoin subjoin) {
        logger.info("Visit SubJoin");
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        logger.info("Visit LateralSubSelect");
    }

    @Override
    public void visit(ValuesList valuesList) {
        logger.info("Visit ValuesList");
    }



    // *************SelectVisitor**********

    @Override
    public void visit(PlainSelect plainSelect) {
        logger.info("Visit PlainSelect");
        plainSelect.getFromItem().accept(this);
        plainSelectSelectJoin(plainSelect);

//        if (subSelBody.getWhere() != null)
//            subSelBody.getWhere().accept(this);
    }

    @Override
    public void visit(SetOperationList setOpList) {
        logger.info("Visit SetOperationList");
    }

    @Override
    public void visit(WithItem withItem) {
        logger.info("Visit WithItem");
    }

    private void plainSelectSelectJoin(PlainSelect plainSelect){
        if (plainSelect.getJoins() != null) {
            plainSelect.getJoins().forEach( join -> join.getRightItem().accept(this) );
        }
    }

}
