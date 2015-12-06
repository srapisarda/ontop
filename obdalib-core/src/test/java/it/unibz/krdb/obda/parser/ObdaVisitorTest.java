package it.unibz.krdb.obda.parser;

import it.unibz.krdb.sql.DBMetadata;
import it.unibz.krdb.sql.DBMetadataExtractor;
import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.api.DeeplyParsedSQLQuery;
import junit.framework.TestCase;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

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


/**
 *
 */
public class ObdaVisitorTest extends TestCase {

    //add support for CAST also in unquoted visited query
    public void testPalinSelect1(){
        final boolean result = parseUnquotedJSQL("SELECT * "
                + "FROM people p "
                + "WHERE p.id IS NOT NULL AND p.salvo IS NOT NULL");
        printJSQL("test_Unquoted1", result);
        assertTrue(result);
    }

    //add support for CAST also in unquoted visited query
    public void testPalinSelect(){
        final boolean result = parseUnquotedJSQL("SELECT 3 AS \"v0QuestType\", NULL AS \"v0Lang\", CAST(\"QpeopleVIEW0\".\"nick2\" AS CHAR) AS \"v0\", 1 AS \"v1QuestType\", NULL AS \"v1Lang\", QpeopleVIEW0.id AS \"v1\""
                + "FROM people \"QpeopleVIEW0\" "
                + "WHERE \"QpeopleVIEW0\".\"id\" IS NOT NULL AND \"QpeopleVIEW0\".\"nick2\" IS NOT NULL");
        printJSQL("test_Unquoted1", result);
        assertTrue(result);
    }


    //distinct is not supported
    public void testDistrinct(){
        final boolean result = parseUnquotedJSQL("SELECT DISTINCT 3 AS \"v0QuestType\", NULL AS \"v0Lang\", CAST(\"QpeopleVIEW0\".\"nick2\" AS CHAR) AS \"v0\", 1 AS \"v1QuestType\", NULL AS \"v1Lang\", QpeopleVIEW0.id AS \"v1\""
                + "FROM people \"QpeopleVIEW0\" "
                + "WHERE \"QpeopleVIEW0\".\"id\" IS NOT NULL AND \"QpeopleVIEW0\".\"nick2\" IS NOT NULL");
        printJSQL("test_Unquoted1", result);
        assertFalse(result);
    }

    private String queryText;

    ObdaVisitor obdaVisitor;


    private boolean parseUnquotedJSQL(String input) {

        queryText = input;

        try {
            DBMetadata dbMetadata = DBMetadataExtractor.createDummyMetadata();
            QuotedIDFactory idfac = dbMetadata.getQuotedIDFactory();
            Statement st = CCJSqlParserUtil.parse(input);
            if (!(st instanceof Select))
                throw new JSQLParserException("The inserted query is not a SELECT statement");

            obdaVisitor = new ObdaVisitor((Select)st, idfac);
            return obdaVisitor.isSupported();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


    private void printJSQL( String title, boolean isSupported) {

        if (isSupported) {
            System.out.println(title + ": " + obdaVisitor.toString());

            try {
                System.out.println("  Tables: " + obdaVisitor.getTables());
                System.out.println("  Projection: " + obdaVisitor.getProjection());

                System.out.println("  Selection: "
                        + ((obdaVisitor.getWhereClause() == null) ? "--" : obdaVisitor
                        .getWhereClause()));

                System.out.println("  Aliases: "
                        + (obdaVisitor.getAliasMap().isEmpty() ? "--" : obdaVisitor
                        .getAliasMap()));
                //System.out.println("  GroupBy: " + queryP.getGroupByClause());
                System.out.println("  Join conditions: "
                        + (obdaVisitor.getJoinConditions().isEmpty() ? "--" : obdaVisitor
                        .getJoinConditions()));
            } catch (Exception e) {

                e.printStackTrace();
            }
        } else {
            System.out.println("Parser JSQL doesn't support for query: "
                    + queryText);
        }
        System.out.println();
    }

}
