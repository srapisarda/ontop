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

    public void testCast1(){
        final boolean result = parseUnquotedJSQL("SELECT CAST(`view0`.`nick2` AS CHAR (8000) CHARACTER SET utf8) AS `v0` FROM people `view0` WHERE `view0`.`nick2` IS NOT NULL");
        printJSQL("testCast1", result);
        assertTrue(result);
    }

    // Does not parse SELECT DISTINCT (on purpose)
    public void testCast2(){
        final boolean result = parseUnquotedJSQL("SELECT DISTINCT CAST(`view0`.`nick2` AS CHAR (8000) CHARACTER SET utf8) AS `v0` FROM people `view0` WHERE `view0`.`nick2` IS NOT NULL");
        printJSQL("testCast", result);
        assertFalse(result);
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

    public void testOracleSiblingsNS(){

        final boolean result = parseUnquotedJSQL(
                " select lpad('*', level, '*' ) || ename ename"
                + " from emp "
                + " start with mgr is null "
                + " connect by prior empno = mgr "
                + " order SIBLINGS by enameivate String queryText; " );
        printJSQL("testOracleSiblingsNS", result);
        assertFalse(result);
    }


    public void testSubSelect(){
        final boolean result = parseUnquotedJSQL("SELECT p.firstName, p.secondName "
                + "FROM " +
                " ( SELECT * FROM people) p");
        printJSQL("testSubSelect", result);
        assertTrue(result);

    }


    public void testSubSelectWhereNS(){
        final boolean result = parseUnquotedJSQL("SELECT p.firstName, p.secondName "
                + "FROM " +
                " ( SELECT * FROM people sp where sp.age > 20 ) p");
        printJSQL("testSubSelect", result);
        assertFalse(result);

    }

    public void testSelectWithNS(){
        final boolean result = parseUnquotedJSQL(
                "WITH p2 as ( SELECT * FROM person p where p.age > 20 ) select * from p2 ");
        printJSQL("testSelectWithNS", result);
        assertFalse(result);

    }

    public void testJoin(){
        final boolean result = parseUnquotedJSQL("SELECT p.firstName, p.secondName, b.email "
                + "FROM person a  " +
                " INNER JOIN email b on a.idPerson = b.idPerson ");
        printJSQL("testJoin", result);
        assertTrue(result);

    }

    public void testTwoJoin(){

        final boolean result = parseUnquotedJSQL("SELECT p.firstName, p.secondName, b.email "
                + "FROM person a  " +
                " INNER JOIN email b on a.personId = b.personId  " +
                " INNER JOIN address c on a.personId = c.personId " +
                " INNER JOIN postcodes d on d.postcode = c.postcode ");
        printJSQL("testTwoJoin", result);
        assertTrue(result);

        String expected = "[A.PERSONID = B.PERSONID, A.PERSONID = C.PERSONID, D.POSTCODE = C.POSTCODE]";
        String actual = obdaVisitor.getJoinConditions().toString();
        assertEquals(expected, actual);

    }

    public void testTwoJoinNoAlias(){

        final boolean result = parseUnquotedJSQL("SELECT firstName, secondName, email, address, postcode "
                + " FROM person " +
                " INNER JOIN email  on person.personId = email.personId  " +
                " INNER JOIN address  on person.personId = address.personId " +
                " INNER JOIN postcodes on postcodes.postcode = address.postcode ");
        printJSQL("testTwoJoinNoAlias", result);
        assertTrue(result);

        assertEquals("[PERSON.PERSONID = EMAIL.PERSONID, PERSON.PERSONID = ADDRESS.PERSONID, POSTCODES.POSTCODE = ADDRESS.POSTCODE]", obdaVisitor.getJoinConditions().toString());

    }

    public void testTwoJoinAlias(){

        final boolean result = parseUnquotedJSQL("SELECT person.firstName as fname, person.secondName as surname, email.email as emailAddress, address.address as personAddress, postcode.postcode as postc"
                + " FROM person " +
                " INNER JOIN email  on person.personId = email.personId  " +
                " INNER JOIN address  on person.personId = address.personId " +
                " INNER JOIN postcodes on postcodes.postcode = address.postcode ");
        printJSQL("testTwoJoinNoAlias", result);
        assertTrue(result);

        assertEquals("[PERSON.PERSONID = EMAIL.PERSONID, PERSON.PERSONID = ADDRESS.PERSONID, POSTCODES.POSTCODE = ADDRESS.POSTCODE]", obdaVisitor.getJoinConditions().toString());
        assertEquals("{FNAME=PERSON.FIRSTNAME, SURNAME=PERSON.SECONDNAME, EMAILADDRESS=EMAIL.EMAIL, PERSONADDRESS=ADDRESS.ADDRESS, POSTC=POSTCODE.POSTCODE}", obdaVisitor.getExpressionAlias().toString() );
    }

    public void testNaturalJoin(){

        final boolean result = parseUnquotedJSQL("SELECT personId, name, email"
                + " FROM person " +
                " NATURAL JOIN email ");
        printJSQL("testNaturalJoin", result);
        assertTrue(result);

        assertEquals("{PERSON=PERSON, EMAIL=EMAIL}", obdaVisitor.getTableAlias().toString());
        assertEquals("[PERSONID, NAME, EMAIL]", obdaVisitor.getProjection().toString());
    }

    public void testNaturalAndInnerJoin(){

        final boolean result = parseUnquotedJSQL("SELECT personId, name, email, address, postcode "
                + " FROM person " +
                " NATURAL JOIN email " +
                " NATURAL JOIN address " +
                " INNER JOIN postcodes on postcodes.postcode = address.postcode ");
        printJSQL("testNaturalAndInnerJoin", result);
        assertTrue(result);

        assertEquals("{ADDRESS=ADDRESS, PERSON=PERSON, EMAIL=EMAIL, POSTCODES=POSTCODES}", obdaVisitor.getTableAlias().toString());
        assertEquals("[PERSONID, NAME, EMAIL, ADDRESS, POSTCODE]", obdaVisitor.getProjection().toString());
        assertEquals("[POSTCODES.POSTCODE = ADDRESS.POSTCODE]", obdaVisitor.getJoinConditions().toString() );

    }

    public void testInnerAndNaturalJoin(){

        final boolean result = parseUnquotedJSQL("SELECT personId, name, email, address, postcode " +
                " FROM postcodes " +
                " INNER JOIN address on postcodes.postcode = address.postcode " +
                " NATURAL JOIN email " +
                " NATURAL JOIN person ");

        printJSQL("testInnerAndNaturalJoin", result);
        assertTrue(result);

        assertEquals("{ADDRESS=ADDRESS, PERSON=PERSON, POSTCODES=POSTCODES, EMAIL=EMAIL}", obdaVisitor.getTableAlias().toString());
        assertEquals("[PERSONID, NAME, EMAIL, ADDRESS, POSTCODE]", obdaVisitor.getProjection().toString());
        assertEquals("[POSTCODES.POSTCODE = ADDRESS.POSTCODE]", obdaVisitor.getJoinConditions().toString() );

    }

    public void testCrossJoin(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM CITIES CROSS JOIN FLIGHTS" );
        printJSQL("testCrossJoin", result);
        assertTrue(result);



    }

    public void testJoinUsingColumns(){
        final boolean result = parseUnquotedJSQL("select owners.name as owner, pets.name as pet, pets.animal " +
                " from owners join pets using (owners_id);");

        printJSQL("testJoinUsingColumns", result);
        assertTrue(result);

        assertEquals("{PETS=PETS, OWNERS=OWNERS}", obdaVisitor.getTableAlias().toString());
        assertEquals("[OWNERS.NAME AS OWNER, PETS.NAME AS PET, PETS.ANIMAL]", obdaVisitor.getProjection().toString());
        assertEquals("{OWNER=OWNERS.NAME, PET=PETS.NAME}", obdaVisitor.getExpressionAlias().toString() );
        assertEquals("[OWNERS.OWNERS_ID = PETS.OWNERS_ID]", obdaVisitor.getJoinConditions().toString() );
    }



    public void testJoinAndAlias() {
        final boolean result = parseUnquotedJSQL("SELECT t1.id as sid, t1.name as fullName FROM student t1 JOIN grade t2 ON t1.id=t2.st_id AND t2.mark='A'");
        printJSQL("testJoinAndAlias", result);
        assertTrue(result);

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

    /*
     join grammar definition
    Join ::-
        rightItem: FromItem,
        natural: Boolean,
        inner: Boolean,
        cross: Boolean,
        simple: Boolean, 	# check
        onExpression: Expression,
        usingColumns: Column*, 	# column names
        outer: Boolean,  	# ns
        right: Boolean,  	# ns
        left: Boolean,  	# ns
        full: Boolean  	# ns
    */


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
        printJSQL("testRegexNotOracle", result);
        assertFalse(result);
    }


    // the Left Outer join condition is not supported
    public  void testLeftOuterJoinNS(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM person a  left outer join  email b on a.idPerson = b.idPerson");
        printJSQL("testLeftOuterJoinNS", result);
        assertFalse(result);
    }

    // the Right Outer join condition is not supported
    public  void testRightOuterJoinNS(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM person a  right outer join  email b on a.idPerson = b.idPerson");
        printJSQL("testRightOuterJoinNS", result);
        assertFalse(result);
    }

    // the Right join condition is not supported
    public  void testRightJoinNS(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM person a right join email b on a.idPerson = b.idPerson");
        printJSQL("testRightJoinNS", result);
        assertFalse(result);
    }

    // the Right join condition is not supported
    public  void testLeftJoinNS(){
        final boolean result = parseUnquotedJSQL("SELECT * FROM person a left join email b on a.idPerson = b.idPerson");
        printJSQL("testLeftJoinNS", result);
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

            obdaVisitor = new SQLQueryParser((Select) st, idfac);
            return true;

        }catch (Exception e) {
            System.out.println( e ) ;
            return false;
        }
    }


    private void printJSQL( String title, boolean isSupported) {

        if (isSupported) {
            System.out.println(title + ": " + obdaVisitor.toString());

            try {
                System.out.println("  Table Alias: " + obdaVisitor.getTableAlias());
                System.out.println("  Projection: " + obdaVisitor.getProjection());

                System.out.println("  Selection: "
                        + ((obdaVisitor.getWhereClause() == null) ? "--" : obdaVisitor
                        .getWhereClause()));

                System.out.println("  Expression Aliases: "
                        + (obdaVisitor.getExpressionAlias().isEmpty() ? "--" : obdaVisitor
                        .getExpressionAlias()));
                //System.out.println("  GroupBy: " + queryP.getGroupByClause());
                System.out.println("  Join conditions: "
                        + (obdaVisitor.getJoinConditions().isEmpty() ? "--" : obdaVisitor
                        .getJoinConditions()));

                System.out.println("  Natural Join conditions: "
                        + (obdaVisitor.getNaturalJoinTables().isEmpty() ? "--" : obdaVisitor
                        .getNaturalJoinTables()));

                System.out.println("  Cross Join conditions: "
                        + (obdaVisitor.getCrossJoinTables().isEmpty() ? "--" : obdaVisitor
                        .getCrossJoinTables()));

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
