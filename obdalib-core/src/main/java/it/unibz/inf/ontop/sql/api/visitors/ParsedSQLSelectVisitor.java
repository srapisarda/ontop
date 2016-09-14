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
import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.sql.DBMetadata;
import it.unibz.inf.ontop.sql.QuotedID;
import it.unibz.inf.ontop.sql.api.ParsedSqlContext;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Salvatore Rapisarda on 10/07/2016.
 */
public class ParsedSQLSelectVisitor implements SelectVisitor {
    private final Logger logger = LoggerFactory.getLogger(getClass());


    /**
     * @return an instance of {@link ParsedSqlContext}
     */
    public ParsedSqlContext getContext() {
        return context;
    }

    private final ParsedSqlContext context;


    /**
     * select visitor used by the ParsedSQLVisitor
     *
     * @param metadata db metadata object {@link DBMetadata}
     */
    public ParsedSQLSelectVisitor(DBMetadata metadata) {
        context = new ParsedSqlContext(metadata);

    }

    /**
     * select visitor used by the ParsedSQLVisitor
     *
     * @param metadata db metadata object {@link DBMetadata}
     * @param contextAlias is a {@link QuotedID} that  identify the context
     */
    ParsedSQLSelectVisitor(DBMetadata metadata, QuotedID contextAlias) {
        context = new ParsedSqlContext(metadata, contextAlias);

    }

    /**
     * The only operation sported for this visitor is the PlainSelect
     * <p>
     * This the grammar for this visitor
     * SelectBody ::=
     * PlainSelect
     * | SetOperationList  	# ns
     * | WithItem 		        # ns
     * "ns" means not supported.
     * <p>
     * <p>
     * PlainSelect ::-
     * selectItems: SelectItem*,
     * fromTable: FromItem,
     * join: Join*,
     * where: Expression,
     * distinct: Distinct, 	# ns
     * having: Expression, 	# ns
     * groupByColumnReferences: Expression*, 	# ns
     * queryLevelByElements : OrderByElement*, 	# ns
     * limit: Limit, 		# ns
     * top: Top, 		# ns
     * oracleHierarchical: OracleHierarchicalExpression, 	# ns
     * intoTables: Table* 	# FAIL
     *
     * @param plainSelect {@link PlainSelect}
     */
    @Override
    public void visit(PlainSelect plainSelect) {
        logger.debug("Visit PlainSelect");

        if (plainSelect.getDistinct() != null)
            throw new ParseException(plainSelect.getDistinct());
        if (plainSelect.getHaving() != null)
            throw new ParseException(plainSelect.getHaving());
        if (plainSelect.getGroupByColumnReferences() != null && !plainSelect.getGroupByColumnReferences().isEmpty())
            throw new ParseException(plainSelect.getGroupByColumnReferences());
        if (plainSelect.getOrderByElements() != null && !plainSelect.getOrderByElements().isEmpty())
            throw new ParseException(plainSelect.getOrderByElements());
        if (plainSelect.getLimit() != null)
            throw new ParseException(plainSelect.getLimit());
        if (plainSelect.getTop() != null)
            throw new ParseException(plainSelect.getTop());
        if (plainSelect.getOracleHierarchical() != null)
            throw new ParseException(plainSelect.getOracleHierarchical());
        if (plainSelect.getIntoTables() != null && !plainSelect.getIntoTables().isEmpty())
            throw new MappingQueryException("Only SELECT queries are allowed", plainSelect.getIntoTables());

        logger.debug(String.format("PlainSelect:  %1$s", plainSelect.toString()));

        ParsedSQLFromItemVisitor fromItemVisitor = new ParsedSQLFromItemVisitor(context.getMetadata());
        plainSelect.getFromItem().accept(fromItemVisitor);

        if (plainSelect.getJoins() != null)
            plainSelect.getJoins().forEach(join -> join.getRightItem().accept(fromItemVisitor));

        context.getRelations().putAll(fromItemVisitor.getContext().getRelations());
        context.getTableAttributes().putAll(fromItemVisitor.getContext().getTableAttributes());
        context.getAttributes().putAll( fromItemVisitor.getContext().getAttributes());

        if (!(fromItemVisitor.getContext().getChildContext() == null || fromItemVisitor.getContext().getChildContext().isEmpty()))
            context.setChildContext(fromItemVisitor.getContext().getChildContext());

        plainSelect.getSelectItems().forEach(selectItem -> {
            if (selectItem instanceof AllColumns)
                context.getProjectedAttributes().putAll( context.getTableAttributes() );
            else {
                ParsedSQLItemVisitor parsedSQLItemVisitor = new ParsedSQLItemVisitor(context);
                selectItem.accept(parsedSQLItemVisitor);
                context.getProjectedAttributes().putAll(parsedSQLItemVisitor.getContext().getProjectedAttributes());
            }
        });




        //  this.getGlobalRelations().entrySet().stream().


//         this.getFromItemVisitor().getRelationMapIndex()
//        this.getGlobalRelations().put(  )
//         this.fromItemVisitor.getRelationMapIndex()

//        if (subSelBody.getWhere() != null)
//            subSelBody.getWhere().accept(this);
    }

    /**
     * This is a not supported method. It throw a {@link ParseException}
     *
     * @param setOpList {@link SetOperationList}
     */
    @Override
    public void visit(SetOperationList setOpList) {
        throw new ParseException(setOpList);
    }

    /**
     * This is a not supported method. It throw a {@link ParseException}
     *
     * @param withItem {@link WithItem}
     */
    @Override
    public void visit(WithItem withItem) {
        throw new ParseException(withItem);
    }


}
