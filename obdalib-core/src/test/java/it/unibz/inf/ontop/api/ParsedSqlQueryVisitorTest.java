package it.unibz.inf.ontop.api;
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
import it.unibz.inf.ontop.sql.api.ParsedSqlQueryVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;


public class ParsedSqlQueryVisitorTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static DBMetadata dbMetadata;


    @BeforeClass
    public static void  Before(){
        createDatabaseRelationDefinition();
    }

    @Before
    public void beforeEach(){
        logger.info("***************** Test *****************");
    }

    @After
    public void afterEach(){
        logger.info(" ");
    }

    @Test
    public void CreateNewInstance(){
        String sql = "select * from PERSON p";

        assertNotNull( new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata));
    }


    @Test(expected= MappingQueryException.class)
    public void MetadataDoesNotContaisTable(){
        String sql = "select * from PERSONSSS p";
        new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }




    @Test
    public void MetadataContainsExpectedTable(){
        String [] expected = {"PERSON"};
        String sql = "select * from " + expected[0] + " p";
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);

        assertFalse(  p.getTables().isEmpty() );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }



    @Test
    public void MetadataContainsExpectedAliasTable(){
        String [] expected = {"PERSON"};
        String [] expectedAlias = {"P"};
        String sql = "select * from " + expected[0] + " " + expectedAlias[0];
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);

        assertFalse( p.getRelationAliasMap().isEmpty()   );
        p.getRelationAliasMap().forEach( (k, v) -> {
            assertTrue( k.size() == 1);
            assertEquals(expectedAlias[0], k.get(0).getTableName());
            assertEquals(expected[0], v.getID().getTableName() );
        });

    }


    @Test
    public void MetadataContainsExpectedTupleAlias(){
        String [] expected = {"name", "age"};
        String sql = String.format("select %1$s, %2$s from PERSON ", expected[0], expected[1]) ;
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        assertFalse( p.getRelationAliasMap().isEmpty()   );
        assertTrue( p.getAttributeAliasMap().size()== expected.length);
    }


    @Test
    public void MetadataContainsExpectedTwoTables(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = "select * from " + String.join(",", (CharSequence[]) expected) ;
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedTwoAliasTables(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = "select * from " + String.join(",", (CharSequence[]) expected) ;
        ParsedSqlQueryVisitor p;
        p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        assertTrue( p.getRelationAliasMap().size()== expected.length );
        int index = 0;
        for (Map.Entry<ImmutableList<RelationID>, DatabaseRelationDefinition> entry : p.getRelationAliasMap().entrySet()) {
            String table = expected[index];
            assertTrue(entry.getKey().size() == 1);
            assertEquals(table, entry.getKey().get(0).getTableName());
            assertEquals(table, entry.getValue().getID().getTableName());
            index++;
        }
    }



    @Test
    public void MetadataContainsExpectedThreeTables(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = "select * from " + String.join(",", (CharSequence[]) expected) ;
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getRelationAliasMap().size() == expected.length );
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));

    }


    @Test
    public void MetadataContainsExpectedThreeAliasTables() {
        String[] expected = {"PERSON", "EMAIL", "ADDRESS"};
        String sql = "select * from " + String.join(",", (CharSequence[]) expected);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor((Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format("expected.length: %d, p.getTables().size(): %d ", expected.length, p.getTables().size()));
        assertTrue(  p.getRelationAliasMap().size() == expected.length );
        final int[] index = {0};
        p.getRelationAliasMap().forEach( (k, v) -> {
            String table = expected[index[0]];
            assertTrue(k.size() == 1);
            assertEquals(table, k.get(0).getTableName());
            assertEquals(table, v.getID().getTableName());
            index[0]++;

        } );
    }

    @Test
    public void MetadataContainsExpectedTwoTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s natural join %2$s", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }



    @Test
    public void MetadataContainsAttributeInJoinTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        // select a.name, a.age, b.email from PERSON as a natural join EMAIL as b
        String sql = String.format( "select a.name, a.age, b.email from %1$s as a natural join %2$s as b", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }


    @Test
    public void MetadataContainsExpectedTwoAliaTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String [] expectedAlias = {"P", "E"};
        String sql = String.format( "select * from %1$s %2$s natural join %3$s %4$s ", expected[0], expectedAlias[0], expected[1], expectedAlias[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getRelationAliasMap().size() == expected.length );
        final int[] index = {0};
        p.getRelationAliasMap().forEach( (k, v) -> {
            String alias = expectedAlias[index[0]];
            String table = expected[index[0]];
            assertTrue(k.size() == 1);
            assertEquals(alias, k.get(0).getTableName());
            assertEquals(table, v.getID().getTableName());
            index[0]++;
        } );
    }


    @Test
    public void MetadataContainsExpectedThreeTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s natural join %2$s natural join %3$s", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedTwoTablesInJoinUsingWhere(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s a, %2$s b where a.idPerson=b.idPerson ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }


    @Test
    public void MetadataContainsExpectedTwoTablesInCrossJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s cross join %2$s", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedThreeTablesInCrossJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s cross join %2$s cross join %3$s", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedTwoTablesInnerJoinON(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s a inner join %2$s b on a.idPerson=b.idPerson ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedThreeTablesInnerJoinON(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s a inner join %2$s b on a.idPerson=b.idPerson inner join %3$s c on a.idPerson=c.idPerson", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test(expected = MappingQueryException.class)
    public void MetadataContainsExpectedThreeTablesInnerJoinONMiddleWrong(){
        String [] expected = { "PERSON", "EMAILSSXX", "ADDRESS"};
        String sql = String.format( "select * from %1$s a inner join %2$s b on a.idPerson=b.idPerson inner join %3$s c on a.idPerson=c.idPerson", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedTwoTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s a inner join %2$s b using (idPerson) ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedThreeTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s a inner join %2$s b inner join %3$s using (idPerson) ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }



    @Test
    public void MetadataContainsExpectedThreeAliasTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String [] expectedAlias = {"A", "B", "ADDRESS"};
        String sql = String.format( "select * from %1$s %2$s inner join %3$s %4$s inner join %5$s using (idPerson) ", expected[0], expectedAlias[0], expected[1], expectedAlias[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getRelationAliasMap().size() == expected.length );
        final int[] index = {0};
        p.getRelationAliasMap().entrySet().forEach(entry -> {
            String alias = expectedAlias[index[0]];
            String table = expected[index[0]];
            assertTrue(entry.getKey().size() == 1);
            assertEquals(alias, entry.getKey().get(0).getTableName());
            assertEquals(table, entry.getValue().getID().getTableName());
            index[0]++;
        });
    }

    @Test(expected= MappingQueryException.class)
    public void MetadataContainsExpectedThreeTablesInnerJoinUsingLastWrong(){
        String [] expected = { "PERSON", "EMAIL", "ADDRE"};
        String sql = String.format( "select * from %1$s a inner join %2$s b inner join %3$s using (idPerson) ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }


    @Test
    public void MetadataContainsExpectedFourTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS", "POSTCODE"};
        // select a.name as fname, b.email as emailAddress, c.address as personAddress, d.postcode as postc from PERSON a inner join EMAIL b on a.personId = b.personId inner join ADDRESS c on a.personId = c.personId inner join POSTCODE d on d.idPostcode = c.idPostcode;
        String sql = String.format(
                "select a.name as fname, " +
                        "b.email as emailAddress, " +
                        "c.address as personAddress, " +
                        "d.postcode as postc " +
                "from %1$s a " +
                "inner join %2$s b on a.personId = b.personId " +
                        "inner join %3$s c on a.personId = c.personId " +
                        "inner join %4$s d on d.idPostcode = c.idPostcode;",
                expected[0], expected[1], expected[2], expected[3]);

        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));



        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream2 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().toLowerCase().equals("fname")).collect(Collectors.toList()));

        assertTrue( pairStream2.size() == 1);
        assertTrue( pairStream2.get(0).fst.size()==1 );
        assertTrue( pairStream2.get(0).fst.get(0).getTableName().equals("A") );
        assertTrue( pairStream2.get(0).snd.getAttribute().getName().toLowerCase().equals("fname"));

        // "Pair[["d"],"d".POSTC]" -> "POSTCODE"
        List<Map.Entry<Pair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID>> postc =
                p.getAttributeAliasMap().entrySet().stream().filter(pairQuotedIDEntry ->
                        pairQuotedIDEntry.getKey().snd.getAttribute().getName().toLowerCase().equals("postc")).collect(Collectors.toList());
        assertTrue( postc.size() == 1);
        assertTrue( postc.get(0).getValue().getName().toLowerCase().equals("postcode"));
        assertTrue( postc.get(0).getKey().snd.getRelation().getTableName().equals("D"));
        assertTrue( postc.get(0).getKey().fst.get(0).getTableName().equals("D"));
        // ["d"] -> POSTCODE
        assertTrue( p.getRelationAliasMap().get(postc.get(0).getKey().fst).getID().getTableName().equals("POSTCODE"));
    }

    @Test
    public void MetadataContainsExpectedTwoTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s, (select * from %2$s ) c ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }


    @Test
    public void MetadataContainsExpectedTwoAliasAsAttribute(){
        String [] expectedTable = { "PERSON c"};
        String [] expectedAttributes = { "NAME a", "AGE b"};
        String [] expectedT1 = { "NAME", "AGE"};

        String sql = String.format( "select %1$s, %2$s from %3$s ", expectedAttributes[0], expectedAttributes[1], expectedTable[0] );
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        assertTrue(p.getAttributeAliasMap() != null);

       //  [["c"], "a"] --> "NAME"

        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream2 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals("A")).collect(Collectors.toList()));

        assertTrue( pairStream2.size() == 1);
        assertTrue( pairStream2.get(0).fst.size()==1 );
        assertTrue( pairStream2.get(0).fst.get(0).getTableName().equals("C") );
        assertTrue( pairStream2.get(0).snd.getAttribute().getName().equals("A"));

        // ["c"] -> PERSON ************************
        DatabaseRelationDefinition r =  p.getRelationAliasMap().get(pairStream2.get(0).fst);
        assertEquals("Expected table PERSON.", "PERSON",  r.getID().getTableName());


        //  [["c"], "b"] --> "AGE"

        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals("B")).collect(Collectors.toList()));

        assertTrue( pairStream.size() == 1);
        assertTrue( pairStream.get(0).fst.size()==1 );
        assertTrue( pairStream.get(0).fst.get(0).getTableName().equals("C") );
        assertTrue( pairStream.get(0).snd.getAttribute().getName().equals("B"));

    }


    @Test
    public void MetadataContainsExpectedAttributesAndAliasOnTheSubSelect() {
        String[] expectedT1 = {"NAME", "AGE"};
        String[] expectedT2 = {"EMAIL", "ACTIVE"};
        String[] expectedTables = {"PERSON", "EMAIL"};
        String [] expectedAlias = {"A", "B", "C", "D", "E", "F", "G"};
        String sql = String.format("select %1$s %2$s, %3$s %4$s from %5$s %6$s, " +
                        "(select %7$s %8$s, %9$s %10$s from %11$s %12$s ) %13$s",
                expectedT1[0], expectedAlias[0], expectedT1[1], expectedAlias[1], expectedTables[0], expectedAlias[2],
                expectedT2[0], expectedAlias[3], expectedT2[1], expectedAlias[4], expectedTables[1], expectedAlias[5], expectedAlias[6] );
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor((Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        assertTrue(p.getAttributeAliasMap() != null);

        assertEquals(expectedT1.length + expectedT2.length, p.getAttributeAliasMap().size() );

        // select NAME a, AGE b from PERSON c, (select EMAIL d, ACTIVE e from EMAIL f ) g


        // [["c"], "a"] -> NAME
        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream2 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals("A")).collect(Collectors.toList()));

        assertTrue( pairStream2.size() == 1);
        assertTrue( pairStream2.get(0).fst.size()==1 );
        assertTrue( pairStream2.get(0).fst.get(0).getTableName().equals("C") );
        assertTrue( pairStream2.get(0).snd.getAttribute().getName().equals("A"));

        // ["c"] -> PERSON ************************
        DatabaseRelationDefinition r =  p.getRelationAliasMap().get(pairStream2.get(0).fst);
        assertEquals("Expected table PERSON.", expectedTables[0],  r.getID().getTableName());

        // [["c"], "b"] -> AGE
        pairStream2 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals("B")).collect(Collectors.toList()));

        assertTrue( pairStream2.size() == 1);
        assertTrue( pairStream2.get(0).fst.size()==1 );
        assertTrue( pairStream2.get(0).fst.get(0).getTableName().equals("C") );
        assertTrue( pairStream2.get(0).snd.getAttribute().getName().equals("B"));
        // key relation already tested

        // [["g", "f"], "d"] -> EMAIL
        pairStream2 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals("D")).collect(Collectors.toList()));

        assertTrue( pairStream2.size() == 1);
        assertTrue( pairStream2.get(0).fst.size()==2 );
        assertTrue( pairStream2.get(0).fst.get(0).getTableName().equals("G") );
        assertTrue( pairStream2.get(0).fst.get(1).getTableName().equals("F") );
        assertTrue( pairStream2.get(0).snd.getAttribute().getName().equals("D"));

        // ["g", "f"] -> EMAIL (relation) **************************
        r =  p.getRelationAliasMap().get(pairStream2.get(0).fst);
        assertEquals("Expected table EMAIL.",expectedTables[1],  r.getID().getTableName());



        // [["g", "f"], "e"] -> ACTIVE
        pairStream2 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals("E")).collect(Collectors.toList()));

        assertTrue( pairStream2.size() == 1);
        assertTrue( pairStream2.get(0).fst.size()==2 );
        assertTrue( pairStream2.get(0).fst.get(0).getTableName().equals("G") );
        assertTrue( pairStream2.get(0).fst.get(1).getTableName().equals("F") );
        assertTrue( pairStream2.get(0).snd.getAttribute().getName().equals("E"));

    }


    @Test
    public void MetadataContainsExpectedAttributesAliasSubSelectJoin(){
        String [] expectedT1 = { "NAME", "AGE"};
        String [] expectedT2 = { "EMAIL", "ACTIVE"};
        String [] expectedTables = { "PERSON", "EMAIL"};
        String expectedAliasTable= "C";
        String sql = String.format( "select %1$s, %2$s from %3$s, " +
                                    "(select %4$s, %5$s from %6$s ) %7$s",
                expectedT1[0], expectedT1[1] , expectedTables[0],
                expectedT2[0], expectedT2[1] , expectedTables[1],
                expectedAliasTable);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        assertTrue(p.getAttributeAliasMap() != null );

        assertEquals( p.getAttributeAliasMap().size() , expectedT1.length + expectedT2.length ) ;

            // select NAME, AGE from PERSON, (select EMAIL, ACTIVE from EMAIL ) c

        // CHECKING KEYS
        assertTrue( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .anyMatch( key-> key.snd.getAttribute().getName().equals(expectedT1[0])));

        // Pair[["c", "EMAIL"], "EMAIL"] --> "EMAIL"
        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals(expectedT2[0])).collect(Collectors.toList()));

        assertTrue( pairStream.size() == 1);
        assertTrue( pairStream.get(0).fst.size()==2 );
        assertTrue( pairStream.get(0).fst.get(0).getTableName().equals(expectedAliasTable) );
        assertTrue( pairStream.get(0).fst.get(1).getTableName().equals(expectedTables[1]));
        assertTrue( pairStream.get(0).snd.getAttribute().getName().equals(expectedT2[0]));

        Assert.assertEquals(expectedT2[0],  p.getAttributeAliasMap()
                .entrySet()
                .stream()
                .filter(co -> co.getKey().snd.getAttribute().getName().equals(expectedT2[0])).findFirst().get().getValue().getName());



        // Pair[["c", "EMAIL"], "ACTIVE"] --> "ACTIVE"
        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream2 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals(expectedT2[1])).collect(Collectors.toList()));

        assertTrue( pairStream2.size() == 1);
        assertTrue( pairStream2.get(0).fst.size()==2 );
        assertTrue( pairStream2.get(0).fst.get(0).getTableName().equals(expectedAliasTable) );
        assertTrue( pairStream2.get(0).fst.get(1).getTableName().equals(expectedTables[1]));
        assertTrue( pairStream2.get(0).snd.getAttribute().getName().equals(expectedT2[1]));

        Assert.assertEquals(expectedT2[1],  p.getAttributeAliasMap()
                .entrySet()
                .stream()
                .filter(co -> co.getKey().snd.getAttribute().getName().equals(expectedT2[1])).findFirst().get().getValue().getName());


        // Pair[["PERSON"], "NAME"] -> NAME
        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream3 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals(expectedT1[0])).collect(Collectors.toList()));

        assertTrue( pairStream3.size() == 1);
        assertTrue( pairStream3.get(0).fst.size()==1 );
        assertTrue( pairStream3.get(0).fst.get(0).getTableName().equals(expectedTables[0]));
        assertTrue( pairStream3.get(0).snd.getAttribute().getName().equals(expectedT1[0]));

        assertEquals(expectedT1[0],  p.getAttributeAliasMap()
                .entrySet()
                .stream()
                .filter(co -> co.getKey().snd.getAttribute().getName().equals(expectedT1[0])).findFirst().get().getValue().getName());



        // Pair[["PERSON"], "AGE" ] -> AGE
        ImmutableList<Pair<ImmutableList<RelationID>, QualifiedAttributeID>> pairStream4 = ImmutableList.copyOf( p.getAttributeAliasMap()
                .keySet()
                .stream()
                .filter(co -> co.snd.getAttribute().getName().equals(expectedT1[1])).collect(Collectors.toList()));

        assertTrue( pairStream4.size() == 1);
        assertTrue( pairStream4.get(0).fst.size()==1 );
        assertTrue( pairStream4.get(0).fst.get(0).getTableName().equals(expectedTables[0]));
        assertTrue( pairStream4.get(0).snd.getAttribute().getName().equals(expectedT1[1]));

        assertEquals(expectedT1[1],  p.getAttributeAliasMap()
                .entrySet()
                .stream()
                .filter(co -> co.getKey().snd.getAttribute().getName().equals(expectedT1[1])).findFirst().get().getValue().getName());

        // Check that the db-metadata has not been modified.
        assertEquals( 4, dbMetadata.getDatabaseRelations().size()  );
    }


    @Test
    public void MetadataContainsExpectedTwoAliasTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String [] expectedAlias = { "A", "B", "C" };

        String sql = String.format( "select * from %1$s %2$s, (select * from %3$s %4$s ) %5$s ", expected[0], expectedAlias[0], expected[1], expectedAlias[1], expectedAlias[2] );
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getRelationAliasMap().size() == expected.length );

        final int[] index = {0};
        p.getRelationAliasMap().forEach( (k,v) -> {
            logger.info( "alias: " + k.toString() );

            if ( index[0] == 1 ) {
                assertTrue(k.size() == 2);
                assertEquals(expectedAlias[2], k.get(0).getTableName());
                assertEquals(expectedAlias[1], k.get(1).getTableName());
            }
            index[0]++;
        });

    }

    @Test (expected = MappingQueryException.class)
    public void AnySubqueryShouldContainsAnAlias(){
        String [] expected = { "PERSON", "EMAIL"};
        String [] expectedAlias = { "a", "b", "c" };

        String sql = String.format( "select * from %1$s %2$s, (select * from %3$s %4$s )  ", expected[0], expectedAlias[0], expected[1], expectedAlias[1]);
        new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);

    }


    @Test // (expected = ParseException.class) // this need to be reviewed
    public void MetadataContainsExpectedThreeTableSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s, (select * from %2$s, (select * from %3$s ) a ) b ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test // (expected = ParseException.class) // this need to be reviewed
    public void MetadataContainsExpectedFourTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format(
                "select * from %1$s a, " +
                "(select * from %2$s a, " +
                        "(select * from  %3$s a inner join %2$s b using(personId) ) c ) b, " +
                "%2$s c , " +
                "(select * from %2$s a, (select * from %1$s a inner join   %2$s b  on a.personId= b.personId) f ) d, " +
                        "%3$s e;", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }


    /**
     *
     *  SELECT * FROM PERSON a,                                                                (a     -> PERSON)
     *        (SELECT * FROM EMAIL a,                                                          (b,a   -> EMAIL)
     *          (SELECT * FROM ADDRESS a INNER JOIN EMAIL b USING (personId)) g) b,            (b,g,a -> ADDRESS)
     *                                                                                         (b,g,b -> EMAIL)
     *
     *      EMAIL c,                                                                           (c     -> EMAIL)
     *
     *    (SELECT * FROM EMAIL a,                                                              (d,a   -> EMAIL)
     *         (SELECT * FROM PERSON a INNER JOIN EMAIL b ON a.personId = b.personId) g) d,    (d,g,a -> PERSON)
     *                                                                                         (d,g,b -> EMAIL)
     *    ADDRESS e                                                                            (e     -> ADDRESS)
     */
    @Test
    public void checkRelationsMapTest (){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};

        String sql = String.format(
                "select * from %1$s a, " +
                        "(select * from %2$s a, " +
                        "(select * from  %3$s a inner join %2$s b using(idPerson)) g) b, " +
                        "%2$s c , " +
                        "(select * from %2$s a, (select * from %1$s a inner join   %2$s b  on a.idPerson= b.idPerson) g ) d, " +
                        "%3$s e;", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        assertTrue( p.getRelationAliasMap().size() == 9);

        //(a     -> PERSON)
        List<RelationID> key = getKeyFromStringArray( new String[]{"A"});
        assertEquals( expected[0]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (b,a   -> EMAIL)
        key = getKeyFromStringArray( new String[]{"B","A"});
        assertEquals( expected[1]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (b,g,a -> ADDRESS)
        key = getKeyFromStringArray( new String[]{"B", "G", "A"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[2]  );


        // (b,g,b -> EMAIL)
        key = getKeyFromStringArray( new String[]{"B", "G", "B"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[1]  );


        //(c     -> EMAIL)
        key = getKeyFromStringArray( new String[]{"C"});
        assertEquals( expected[1]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (d,a   -> EMAIL)
        key = getKeyFromStringArray( new String[]{"D","A"});
        assertEquals( expected[1]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (d,g,a -> PERSON)
        key = getKeyFromStringArray( new String[]{"D", "G", "A"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[0]  );

        // (d,g,b -> EMAIL)
        key = getKeyFromStringArray( new String[]{"D", "G", "B"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[1]  );

        //(e     -> ADDRESS)
        key = getKeyFromStringArray( new String[]{"E"});
        assertEquals( expected[2]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

    }

    private  List<RelationID> getKeyFromStringArray( String [] aliases ){
        List<RelationID> key = new LinkedList<>();
        for (  String alias :  aliases )
            key.add(RelationID.createRelationIdFromDatabaseRecord(dbMetadata.getQuotedIDFactory(), null, alias));
        return  key;
    }

    @Test(expected = ParseException.class)
    public void WithOperationIsNotSupported(){
        String [] expected = { "PERSON"};
        String sql = String.format(
                        "WITH names as ( " +
                        "select name from %1$s ) "
                                + "select * from names "
                        , expected[0]);
        logger.info(sql);
      new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }


    /*
    All the set operation are not supported !!!
    INTERSECT,
    EXCEPT,
    MINUS,
    UNION
    */

    @Test(expected = ParseException.class)
    public void UnionOperationIsNotSupported(){
        String [] expected = { "PERSON"};
        String sql = String.format(
                        "select name from %1$s  where  name = 'a' " +
                                "union "
                        + "select name from %1$s  where  name = 'b'"
                , expected[0]);
        logger.info(sql);
        new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }

    @Test(expected = ParseException.class)
    public void UnionAllOperationIsNotSupported(){
        String [] expected = { "PERSON"};
        String sql = String.format(
                "select name from %1$s  where  name = 'a' " +
                        "union all "
                        + "select name from %1$s  where  name = 'b'"
                , expected[0]);
        logger.info(sql);
        new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }

    @Test(expected = ParseException.class)
    public void IntersectOperationIsNotSupported(){
        String [] expected = { "PERSON"};
        String sql = String.format(
                "select name from %1$s  where  name = 'a' " +
                        "intersect "
                        + "select name from %1$s  where  name = 'b'"
                , expected[0]);
        logger.info(sql);
        new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }

    @Test(expected = ParseException.class)
    public void MinusOperationIsNotSupported(){
        String [] expected = { "PERSON"};
        String sql = String.format(
                "select name from %1$s  " +
                        "minus "
                        + "select name from %1$s  where  name = 'b'"
                , expected[0]);
        logger.info(sql);
        new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }

    @Test(expected = ParseException.class)
    public void ExceptOperationIsNotSupported(){
        String [] expected = { "PERSON"};
        String sql = String.format(
                "select name from %1$s  " +
                        "Except "
                        + "select name from %1$s  where  name = 'b'"
                , expected[0]);
        logger.info(sql);
        new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }

    // TODO:
    // Add tests for not supported: PIVOT,
    //                              LateralSubSelect, Distinct, Expression,
    //                              groupByColumnReferences, OrderByElement,
    //                              Limit, Top, oracleHierarchical, into Tables


    private  Statement getStatementFromUnquotedSQL(String input) {
        try {
            Statement st = CCJSqlParserUtil.parse(input);
            if (!(st instanceof Select)) {
                throw new JSQLParserException("The inserted query is not a SELECT statement");
            }
            return st;
        }catch (Exception e) {
           logger.error("getStatementFromUnquotedSQL", e ) ;
            return null;
        }
    }


    // http://sqlfiddle.com/#!15/3319c
    private static void createDatabaseRelationDefinition(){

        dbMetadata = DBMetadataExtractor.createDummyMetadata();
        QuotedIDFactory quotedIDFactory = dbMetadata.getQuotedIDFactory();

        DatabaseRelationDefinition tdPerson = dbMetadata.createDatabaseRelation(quotedIDFactory.createRelationID(null, "PERSON"));
        tdPerson.addAttribute(quotedIDFactory.createAttributeID("idPerson"), Types.INTEGER, null, false);
        tdPerson.addAttribute(quotedIDFactory.createAttributeID("name"), Types.VARCHAR, null, false);
        tdPerson.addAttribute(quotedIDFactory.createAttributeID("age"), Types.INTEGER, null, false);

        DatabaseRelationDefinition tdEmail = dbMetadata.createDatabaseRelation(quotedIDFactory.createRelationID(null, "EMAIL"));
        tdEmail.addAttribute(quotedIDFactory.createAttributeID("idEmail"), Types.INTEGER, null, false);
        tdEmail.addAttribute(quotedIDFactory.createAttributeID("idPerson"), Types.INTEGER, null, false);
        tdEmail.addAttribute(quotedIDFactory.createAttributeID("email"), Types.VARCHAR, null, false);
        tdEmail.addAttribute(quotedIDFactory.createAttributeID("active"), Types.BIT, null, false);

        DatabaseRelationDefinition tdAddress = dbMetadata.createDatabaseRelation( quotedIDFactory.createRelationID(null, "ADDRESS") );
        tdAddress.addAttribute(quotedIDFactory.createAttributeID("idAddress"), Types.INTEGER, null, false);
        tdAddress.addAttribute(quotedIDFactory.createAttributeID("idPerson"), Types.INTEGER, null, false);
        tdAddress.addAttribute(quotedIDFactory.createAttributeID("idPostcode"), Types.INTEGER, null, false);
        tdAddress.addAttribute(quotedIDFactory.createAttributeID("address"), Types.VARCHAR, null, false);

        DatabaseRelationDefinition tdPostcode = dbMetadata.createDatabaseRelation( quotedIDFactory.createRelationID(null, "POSTCODE") );
        tdPostcode.addAttribute(quotedIDFactory.createAttributeID("idPostcode"), Types.INTEGER, null, false);
        tdPostcode.addAttribute(quotedIDFactory.createAttributeID("postcode"), Types.VARCHAR, null, false);
        tdPostcode.addAttribute(quotedIDFactory.createAttributeID("city"), Types.VARCHAR, null, false);
        tdPostcode.addAttribute(quotedIDFactory.createAttributeID("rangeNumbers"), Types.VARCHAR, null, false);

    }





}