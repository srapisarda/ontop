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
import it.unibz.inf.ontop.sql.DatabaseRelationDefinition;
import it.unibz.inf.ontop.sql.RelationID;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author  Salvatore Rapisarda on 10/07/2016.
 */
public class ParsedSQLSelectVisitor implements SelectVisitor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Set<RelationID> tables;
    private final DBMetadata metadata;

    // private List<String> relationMapIndex;
    //  private DBMetadata metadata;

    public Map<List<RelationID>, DatabaseRelationDefinition> getRelationAliasMap() {
        return relationAliasMap;
    }

    private Map<List<RelationID>, DatabaseRelationDefinition> relationAliasMap;


//    public List<String> getParent() {
//        return parent;
//    }
//
//    public void setParent(List<String> parent) {
//        this.parent = parent;
//    }

   // List<String> parent = new LinkedList<>();

    public ParsedSQLSelectVisitor(DBMetadata metadata) {
        this.metadata = metadata;
        this.tables = new HashSet<>();
        this.relationAliasMap = new LinkedHashMap<>();
    }

//    public void setRelationMapIndex(List<String> relationMapIndex) {
//        this.relationMapIndex = relationMapIndex;
//    }

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
     */
    @Override
    public void visit(PlainSelect plainSelect) {
        logger.info("Visit PlainSelect");


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
        if (plainSelect.getLimit() != null)
            throw new ParseException(plainSelect.getLimit());
        if (plainSelect.getTop() != null)
            throw new ParseException(plainSelect.getTop());
        if (plainSelect.getOracleHierarchical() != null)
            throw new ParseException(plainSelect.getOracleHierarchical());
        if (plainSelect.getIntoTables() != null && !plainSelect.getIntoTables().isEmpty())
            throw new MappingQueryException("INTO TABLE IS NOT ALLOWED!!! FAIL!", plainSelect.getIntoTables());


        logger.info(String.format("PlainSelect:  %1$s", plainSelect.toString()));

        ParsedSQLFromItemVisitor fromItemVisitor = new ParsedSQLFromItemVisitor(this.metadata);

        plainSelect.getFromItem().accept(fromItemVisitor);

        if (plainSelect.getJoins() != null)
            plainSelect.getJoins().forEach(join -> {
                join.getRightItem().accept(fromItemVisitor);
            });
    //    Map<List<RelationID>, DatabaseRelationDefinition> a =  fromItemVisitor.getRelationMapIndex();

        this.tables.addAll(fromItemVisitor.getTables() );
        this.getRelationAliasMap().putAll( fromItemVisitor.getRelationAliasMap() );
//         this.getFromItemVisitor().getRelationMapIndex()
//        this.getRelationAliasMap().put(  )
//         this.fromItemV   isitor.getRelationMapIndex()

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
        logger.info("Visit SetOperationList");
        throw new ParseException(setOpList);
    }

    /**
     * This is a not supported method. It throw a {@link ParseException}
     *
     * @param withItem {@link WithItem}
     */
    @Override
    public void visit(WithItem withItem) {
        logger.info("Visit WithItem");
        throw new ParseException(withItem);
    }

    public Set<RelationID> getTables() {
        return tables;
    }
}
