package it.unibz.krdb.obda.parser;

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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import it.unibz.krdb.sql.DBMetadata;
import it.unibz.krdb.sql.DBMetadataExtractor;
import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.api.DeeplyParsedSQLQuery;
import junit.framework.TestCase;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class JSQLDeeplyParserTest extends TestCase {
//    final static Logger log = LoggerFactory.getLogger(JSQLDeeplyParserTest.class);

    //add support for CAST also in unquoted visited query
    public void testUnquoted1(){
        final boolean result = parseUnquotedJSQL("SELECT 3 AS \"v0QuestType\", NULL AS \"v0Lang\", CAST(\"QpeopleVIEW0\".\"nick2\" AS CHAR) AS \"v0\", 1 AS \"v1QuestType\", NULL AS \"v1Lang\", QpeopleVIEW0.id AS \"v1\""
                + "FROM people \"QpeopleVIEW0\" "
                + "WHERE \"QpeopleVIEW0\".\"id\" IS NOT NULL AND \"QpeopleVIEW0\".\"nick2\" IS NOT NULL");
        printJSQL("test_Unquoted1", result);
        assertTrue(result);
    }

    // Does not parse SELECT DISTINCT (on purpose)
    public void testUnquoted2(){
        final boolean result = parseUnquotedJSQL("SELECT DISTINCT 3 AS \"v0QuestType\", NULL AS \"v0Lang\", CAST(\"QpeopleVIEW0\".\"nick2\" AS CHAR) AS \"v0\", 1 AS \"v1QuestType\", NULL AS \"v1Lang\", QpeopleVIEW0.id AS \"v1\""
                + "FROM people \"QpeopleVIEW0\" "
                + "WHERE \"QpeopleVIEW0\".\"id\" IS NOT NULL AND \"QpeopleVIEW0\".\"nick2\" IS NOT NULL");
        printJSQL("test_Unquoted1", result);
        assertFalse(result);
    }

    public void testCast1(){
        final boolean result = parseUnquotedJSQL("SELECT CAST(`view0`.`nick2` AS CHAR (8000) CHARACTER SET utf8) AS `v0` FROM people `view0` WHERE `view0`.`nick2` IS NOT NULL");
        printJSQL("testCast", result);
        assertTrue(result);
    }

    // Does not parse SELECT DISTINCT (on purpose)
    public void testCast2(){
        final boolean result = parseUnquotedJSQL("SELECT DISTINCT CAST(`view0`.`nick2` AS CHAR (8000) CHARACTER SET utf8) AS `v0` FROM people `view0` WHERE `view0`.`nick2` IS NOT NULL");
        printJSQL("testCast", result);
        assertFalse(result);
    }

	/* Regex in MySQL, Oracle and Postgres*/

    public void testRegexMySQL(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM pet WHERE name REGEXP '^b'");
        printJSQL("testRegexMySQL", result);
        assertTrue(result);
    }

    public void testRegexBinaryMySQL(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM pet WHERE name REGEXP BINARY '^b'");
        printJSQL("testRegexBinaryMySQL", result);
        assertTrue(result);
    }

    public void testRegexPostgres(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM pet WHERE name ~ 'foo'");
        printJSQL("testRegexPostgres", result);
        assertTrue(result);
    }

    //no support for similar to in postgres
    public void testRegexPostgresSimilarTo(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM pet WHERE 'abc' SIMILAR TO 'abc'");
        printJSQL("testRegexPostgresSimilarTo", result);
        assertFalse(result);
    }

    public void testRegexOracle(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM pet WHERE REGEXP_LIKE(testcol, '[[:alpha:]]')");
        printJSQL("testRegexMySQL", result);
        assertTrue(result);
    }

    //no support for not without parenthesis
    public void testRegexNotOracle(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM pet WHERE NOT REGEXP_LIKE(testcol, '[[:alpha:]]')");
        printJSQL("testRegexNotMySQL", result);
        assertFalse(result);
    }



    private String queryText;

    DeeplyParsedSQLQuery deeplyQueryP;


    private boolean parseUnquotedJSQL(String input) {

        queryText = input;

        try {
            DBMetadata dbMetadata = DBMetadataExtractor.createDummyMetadata();
            QuotedIDFactory idfac = dbMetadata.getQuotedIDFactory();
            Statement st = CCJSqlParserUtil.parse(input);
            if (!(st instanceof Select))
            	throw new JSQLParserException("The inserted query is not a SELECT statement");

            deeplyQueryP = new DeeplyParsedSQLQuery((Select)st, idfac);
            
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }


    private void printJSQL( String title, boolean isSupported) {

        if (isSupported) {
            System.out.println(title + ": " + deeplyQueryP.toString());

            try {
                System.out.println("  Tables: " + deeplyQueryP.getTables());
                System.out.println("  Projection: " + deeplyQueryP.getProjection());

                System.out.println("  Selection: "
                        + ((deeplyQueryP.getWhereClause() == null) ? "--" : deeplyQueryP
                        .getWhereClause()));

                System.out.println("  Aliases: "
                        + (deeplyQueryP.getAliasMap().isEmpty() ? "--" : deeplyQueryP
                        .getAliasMap()));
                //System.out.println("  GroupBy: " + queryP.getGroupByClause());
                System.out.println("  Join conditions: "
                        + (deeplyQueryP.getJoinConditions().isEmpty() ? "--" : deeplyQueryP
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
