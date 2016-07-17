package it.unibz.inf.ontop.api;

import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.ontology.Assertion;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public void MetadataContaisExpectedTable(){
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
        String [] expectedAlias = {"p"};
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
    public void MetadataContaisExpectedTwoTables(){
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
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        assertTrue( p.getRelationAliasMap().size()== expected.length );
        int index = 0;
        Iterator<Map.Entry<List<RelationID>, DatabaseRelationDefinition>> i = p.getRelationAliasMap().entrySet().iterator();
        while (i.hasNext()){
            Map.Entry<List<RelationID>,DatabaseRelationDefinition> entry = i.next();
            String table = expected[index];
            assertTrue( entry.getKey().size() == 1);
            assertEquals(table, entry.getKey().get(0).getTableName());
            assertEquals(table, entry.getValue().getID().getTableName());
            index++;
        }
    }



    @Test
    public void MetadataContaisExpectedThreeTables(){
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
    public void MetadataContaisExpectedTwoTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s natural join %2$s", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }



    @Test
    public void MetadataContaisExpectedTwoAliaTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String [] expectedAlias = {"p", "e"};
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
    public void MetadataContaisExpectedThreeTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s natural join %2$s natural join %3$s", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContaisExpectedTwoTablesInJoinUsingWhere(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s a, %2$s b where a.idPerson=b.idPerson ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }


    @Test
    public void MetadataContaisExpectedTwoTablesInCrossJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s cross join %2$s", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContaisExpectedThreeTablesInCrossJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s cross join %2$s cross join %3$s", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContaisExpectedTwoTablesInnerJoinON(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s a inner join %2$s b on a.idPerson=b.idPerson ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContaisExpectedThreeTablesInnerJoinON(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s a inner join %2$s b on a.idPerson=b.idPerson inner join %3$s c on a.idPerson=c.idPerson", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test(expected = MappingQueryException.class)
    public void MetadataContaisExpectedThreeTablesInnerJoinONMiddleWrong(){
        String [] expected = { "PERSON", "EMAILSSXX", "ADDRESS"};
        String sql = String.format( "select * from %1$s a inner join %2$s b on a.idPerson=b.idPerson inner join %3$s c on a.idPerson=c.idPerson", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContaisExpectedTwoTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s a inner join %2$s b using (idPerson) ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContaisExpectedThreeTablesInnerJoinUsing(){
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
        String [] expectedAlias = {"a", "b", "ADDRESS"};
        String sql = String.format( "select * from %1$s %2$s inner join %3$s %4$s inner join %5$s using (idPerson) ", expected[0], expectedAlias[0], expected[1], expectedAlias[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getRelationAliasMap().size() == expected.length );
        final int[] index = {0};
        p.getRelationAliasMap().entrySet().stream().forEach( entry-> {
            String alias = expectedAlias[index[0]];
            String table = expected[index[0]];
            assertTrue( entry.getKey().size() == 1);
            assertEquals(alias, entry.getKey().get(0).getTableName());
            assertEquals(table, entry.getValue().getID().getTableName());
            index[0]++;
        } );
    }

    @Test(expected= MappingQueryException.class)
    public void MetadataContaisExpectedThreeTablesInnerJoinUsingLastWrong(){
        String [] expected = { "PERSON", "EMAIL", "ADDRE"};
        String sql = String.format( "select * from %1$s a inner join %2$s b inner join %3$s using (idPerson) ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }


    @Test
    public void MetadataContaisExpectedFourTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS", "POSTCODE"};

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
    }

    @Test
    public void MetadataContaisExpectedTwoTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1$s, (select * from %2$s ) c ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test
    public void MetadataContainsExpectedTwoAliasTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String [] expectedAlias = { "a", "b", "c" };

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

        String sql = String.format( "select * from %1$s %2$s, (select * from %3$s %4$s )  ", expected[0], expectedAlias[0], expected[1], expectedAlias[1], expectedAlias[2] );
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }


    @Test // (expected = ParseException.class) // this need to be reviewed
    public void MetadataContaisExpectedThreeTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s, (select * from %2$s, (select * from %3$s ) a ) b ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test // (expected = ParseException.class) // this need to be reviewed
    public void MetadataContaisExpectedFourTablesSubSelectJoin(){
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
        List<RelationID> key = getKeyFromStringArray( new String[]{"a"});
        assertEquals( expected[0]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (b,a   -> EMAIL)
        key = getKeyFromStringArray( new String[]{"b","a"});
        assertEquals( expected[1]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (b,g,a -> ADDRESS)
        key = getKeyFromStringArray( new String[]{"b", "g", "a"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[2]  );


        // (b,g,b -> EMAIL)
        key = getKeyFromStringArray( new String[]{"b", "g", "b"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[1]  );


        //(c     -> EMAIL)
        key = getKeyFromStringArray( new String[]{"c"});
        assertEquals( expected[1]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (d,a   -> EMAIL)
        key = getKeyFromStringArray( new String[]{"d","a"});
        assertEquals( expected[1]  , p.getRelationAliasMap().get(key).getID().getTableName()  );

        // (d,g,a -> PERSON)
        key = getKeyFromStringArray( new String[]{"d", "g", "a"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[0]  );

        // (d,g,b -> EMAIL)
        key = getKeyFromStringArray( new String[]{"d", "g", "b"});
        assertEquals( p.getRelationAliasMap().get(key).getID().getTableName() , expected[1]  );

        //(e     -> ADDRESS)
        key = getKeyFromStringArray( new String[]{"e"});
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