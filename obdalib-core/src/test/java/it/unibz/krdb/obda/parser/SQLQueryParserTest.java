package it.unibz.krdb.obda.parser;

import it.unibz.krdb.sql.DBMetadata;
import it.unibz.krdb.sql.DBMetadataExtractor;
import it.unibz.krdb.sql.QuotedIDFactory;
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
public class SQLQueryParserTest extends TestCase {

    //add support for CAST also in unquoted visited query
    public void testPalinSelect1(){
        final boolean result = parseUnquotedJSQL("SELECT * "
                + "FROM people p "
                + "WHERE p.id IS NOT NULL AND p.salvo IS NOT NULL");
        printJSQL("testPalinSelect1", result);
        assertTrue(result);
    }

    //add support for CAST also in unquoted visited query
    public void testPalinSelec2(){
        final boolean result = parseUnquotedJSQL("SELECT 3 AS \"v0QuestType\", NULL AS \"v0Lang\", CAST(\"QpeopleVIEW0\".\"nick2\" AS CHAR) AS \"v0\", 1 AS \"v1QuestType\", NULL AS \"v1Lang\", QpeopleVIEW0.id AS \"v1\""
                + "FROM people \"QpeopleVIEW0\" "
                + "WHERE \"QpeopleVIEW0\".\"id\" IS NOT NULL AND \"QpeopleVIEW0\".\"nick2\" IS NOT NULL");
        printJSQL("testPalinSelec2", result);
        assertTrue(result);
    }


    //distinct is not supported
    public void testDistrincNS(){
        final boolean result = parseUnquotedJSQL("SELECT DISTINCT 3 AS \"v0QuestType\", NULL AS \"v0Lang\", CAST(\"QpeopleVIEW0\".\"nick2\" AS CHAR) AS \"v0\", 1 AS \"v1QuestType\", NULL AS \"v1Lang\", QpeopleVIEW0.id AS \"v1\""
                + "FROM people \"QpeopleVIEW0\" "
                + "WHERE \"QpeopleVIEW0\".\"id\" IS NOT NULL AND \"QpeopleVIEW0\".\"nick2\" IS NOT NULL");
        printJSQL("testDistrincNS", result);
        assertFalse(result);
    }

    //IntoTables is not supported
    public void testIntoTablesNS(){
        final boolean result = parseUnquotedJSQL("SELECT INTO peopleInto "
                + "FROM people p "
                + "WHERE p.id IS NOT NULL AND p.salvo IS NOT NULL  ");
        printJSQL("testIntoTablesNS", result);
        assertFalse(result);
    }


    //Having is not supported
    public void testHavingNS(){
        final boolean result = parseUnquotedJSQL("SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders FROM (Orders "
               + " INNER JOIN Employees "
               + " ON Orders.EmployeeID=Employees.EmployeeID) "
               + " GROUP BY LastName "
               + " HAVING COUNT(Orders.OrderID) > 10; " );
        printJSQL("testHavingNS", result);
        assertFalse(result);
    }

    //getGroupByColumnReferences is not supported
    public void testgetGroupByColumnReferencesNS(){
        final boolean result = parseUnquotedJSQL("SELECT * "
                + " FROM People "
                + " GROUP BY lastName " );
        printJSQL("testgetGroupByColumnReferencesNS", result);
        assertFalse(result);
    }

    //OrderBy is not supported
    public void testOrderByElementsNS(){
        final boolean result = parseUnquotedJSQL("SELECT lastName, age "
                + "FROM people p "
                + " ORDER BY age ");

        printJSQL("testOrderByElementsNS", result);
        assertFalse(result);
    }

    //Limit is not supported
    public void testLimitNS(){
        final boolean result = parseUnquotedJSQL("SELECT * "
                + "FROM people p "
                + "LIMIT 2 ");

        printJSQL("testLimitNS", result);
        assertFalse(result);
    }


    //Top is not supported
    public void testTopNS(){
        final boolean result = parseUnquotedJSQL(
                "SELECT TOP 2 * "
                + "FROM people p " );

        printJSQL("testTopNS", result);
        assertFalse(result);
    }

    //Top is not supported
    public void testOracleHierarchicalNS(){
        final boolean result = parseUnquotedJSQL(
                 " SELECT employee_id, last_name, manager_id "
                + " FROM employees  "
                + " CONNECT BY PRIOR employee_id = manager_id; ); " );

        printJSQL("testOracleHierarchicalNS", result);
        assertFalse(result);
    }

    public void testOracleSiblingslNS(){

        final boolean result = parseUnquotedJSQL(
                " select lpad('*', level, '*' ) || ename ename"
                + " from emp "
                + " start with mgr is null "
                + " connect by prior empno = mgr "
                + " order SIBLINGS by enameivate String queryText; " );
        printJSQL("testOracleSiblingslNS", result);
        assertFalse(result);
    }

    SQLQueryParser obdaVisitor;
    private String queryText;

    private boolean parseUnquotedJSQL(String input) {

        queryText = input;

        try {
            DBMetadata dbMetadata = DBMetadataExtractor.createDummyMetadata();
            QuotedIDFactory idfac = dbMetadata.getQuotedIDFactory();
            Statement st = CCJSqlParserUtil.parse(input);
            if (!(st instanceof Select))
                throw new JSQLParserException("The inserted query is not a SELECT statement");

            obdaVisitor = new SQLQueryParser((Select)st, idfac);
            return true;

        } catch (SQLQueryParser.ParseException e) {
            System.out.println( e.getUnsupportedObject() ) ;
            return false;
        }catch (JSQLParserException e){
            System.out.println( e.getStackTrace() ) ;
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
