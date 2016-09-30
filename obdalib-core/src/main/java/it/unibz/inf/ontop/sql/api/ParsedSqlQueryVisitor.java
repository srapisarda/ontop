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


import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.sql.DBMetadata;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlSelectVisitor;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlContext;
import net.sf.jsqlparser.statement.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A structure to store the parsed SQL query string. It returns the information
 * about the query using the visitor classes
 *
 * @author Salvatore Rapisarda
 */
public class ParsedSqlQueryVisitor  {
    private PSqlSelectVisitor selectVisitor;

    /**
     *  This constructor get in input the instance of
     *  {@link Select} and  {@link DBMetadata} object
     *
     * @param selectQuery {@link Select}
     * @param metadata {@link DBMetadata}
     */
    public ParsedSqlQueryVisitor(Select selectQuery, DBMetadata metadata){
        Logger logger = LoggerFactory.getLogger(getClass());
        logger.info("Parsed select query: " + selectQuery);

        // WITH operations are not supported
        if (selectQuery.getWithItemsList() != null && ! selectQuery.getWithItemsList().isEmpty())
            throw new ParseException(selectQuery.getWithItemsList());

        selectVisitor = new PSqlSelectVisitor(metadata);
        selectQuery.getSelectBody().accept(selectVisitor);
    }

    public PSqlContext getContext () {
        return selectVisitor.getContext();
    }

}
