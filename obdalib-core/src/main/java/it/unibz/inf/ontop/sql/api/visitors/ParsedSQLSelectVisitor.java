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

import com.google.common.collect.ImmutableList;
import com.sun.tools.javac.util.Pair;
import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.sql.*;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author  Salvatore Rapisarda on 10/07/2016.
 */
public class ParsedSQLSelectVisitor implements SelectVisitor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Set<RelationID> tables;
    private final DBMetadata metadata;

    private final Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> attributeAliasMap;
    public Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> getAttributeAliasMap() {
        return attributeAliasMap;
    }

    public Map<ImmutableList<RelationID>, DatabaseRelationDefinition> getRelationAliasMap() {
        return  relationAliasMap ;
    }

    private Map<ImmutableList<RelationID>, DatabaseRelationDefinition> relationAliasMap;

    /**
     * select visitor used by the ParsedSQLVisitor
     * @param metadata db metadata object {@link DBMetadata}
     */
    public ParsedSQLSelectVisitor(DBMetadata metadata) {
        this.metadata = metadata;
        this.tables = new HashSet<>();
        this.relationAliasMap = new LinkedHashMap<>();
        this.attributeAliasMap = new LinkedHashMap<>();
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
            plainSelect.getJoins().forEach(join -> join.getRightItem().accept(fromItemVisitor));

        this.tables.addAll(fromItemVisitor.getTables() );
        this.getRelationAliasMap().putAll( fromItemVisitor.getRelationAliasMap() );
        this.getAttributeAliasMap().putAll( fromItemVisitor.getAttributeAliasMap() );

        plainSelect.getSelectItems().forEach(selectItem -> {
            ParsedSQLItemVisitor parsedSQLItemVisitor = new ParsedSQLItemVisitor(metadata, null );
            selectItem.accept(parsedSQLItemVisitor);
            List<Map.Entry<Pair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID>> entryList =
                    parsedSQLItemVisitor.getAttributeAliasMap()
                            .entrySet()
                            .stream()
                            .filter(es ->
                                    es.getKey().fst == null || es.getKey().fst.isEmpty()).collect(Collectors.toList());

            if( ! entryList.isEmpty() ) {
                entryList.forEach( entry -> {
                      final Optional<Map.Entry<ImmutableList<RelationID>, DatabaseRelationDefinition>> first = fromItemVisitor.getRelationAliasMap().entrySet().stream()
                            .filter(p -> p.getValue().getAttributes().stream()
                                    .anyMatch(q ->
                                            q.getID().getName().toLowerCase().equals(entry.getKey().snd.getAttribute().getName().toLowerCase()))).findFirst();
                    // .collect(Collectors.toList());

                            if (first.isPresent()) {
                                Pair<ImmutableList<RelationID>, QualifiedAttributeID> n =
                                        new Pair<>(ImmutableList.<RelationID>builder().add( first.get().getValue().getID()).build(),  entry.getKey().snd);
                                this.getAttributeAliasMap().put(n, entry.getValue()  );
                            }
                        });
            }
        });



      //  this.getRelationAliasMap().entrySet().stream().



//         this.getFromItemVisitor().getRelationMapIndex()
//        this.getRelationAliasMap().put(  )
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
