package it.unibz.inf.ontop.sql.api.visitors;

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
import it.unibz.inf.ontop.sql.*;
import it.unibz.inf.ontop.sql.api.ParsedSqlContext;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Salvatore Rapisarda on 20/07/2016.
 */
class ParsedSQLItemVisitor implements SelectItemVisitor {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     *
     * @return  an instance of {@link ParsedSqlContext}
     */
    public ParsedSqlContext getContext() {
        return context;
    }
    private final ParsedSqlContext context;
    private final ParsedSqlContext callerContext;
    ParsedSQLItemVisitor( ParsedSqlContext parentContext){
        context= new ParsedSqlContext(parentContext.getMetadata());
        this.callerContext = parentContext;
    }

    @Override
    public void visit(AllColumns allColumns) {
        logger.debug("visit AllColumns");

    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        logger.debug("visit allTableColumns");
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        logger.debug("visit selectExpressionItem");

        // TODO:  should support complex expressions
        final ParsedSQLExpressionVisitor parsedSQLExpressionVisitor = new ParsedSQLExpressionVisitor();
        selectExpressionItem.getExpression().accept(parsedSQLExpressionVisitor);

        parsedSQLExpressionVisitor
                .getColumns()
                .forEach(column -> {

                    // QualifiedAttributeID as key
                    RelationID relationID=null;
                    if ( column.getTable() != null && column.getTable().getName() != null )
                        relationID = context.getIdFac().createRelationID(column.getTable().getSchemaName(), column.getTable().getName());

                    QualifiedAttributeID  key = new QualifiedAttributeID(
                            relationID,
                            context.getIdFac().createAttributeID(column.getColumnName())
                    ) ;
                    //

                    final QualifiedAttributeID qualifiedAttributeID =  callerContext.getAttributes().get(key); //callerContext.getAttributes().get(quotedAttributeID);

                    if ( qualifiedAttributeID != null )
                        context.getProjectedAttributes().put(key, qualifiedAttributeID);
                    else
                        throw new MappingQueryException( "the attribute is not present in any relation.", key  );
                });
    }

}
