package it.unibz.inf.ontop.api;

import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.sql.DBMetadata;
import it.unibz.inf.ontop.sql.DBMetadataExtractor;
import it.unibz.inf.ontop.sql.DatabaseRelationDefinition;
import it.unibz.inf.ontop.sql.QuotedIDFactory;
import it.unibz.inf.ontop.sql.api.ParsedSqlQueryVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

import static org.junit.Assert.*;


public class ParsedSqlQueryVisitorTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static DBMetadata dbMetadata;


    @BeforeClass
    public static void  Before(){
        createDatabaseRelationDefinition();
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
    public void MetadataContaisExpectedThreeTables(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = "select * from " + String.join(",", (CharSequence[]) expected) ;
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
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
        String sql = String.format( "select * from %1$s, (select * from %2$s ) ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
    }

    @Test // (expected = ParseException.class) // this need to be reviewed
    public void MetadataContaisExpectedThreeTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1$s, (select * from %2$s, (select * from %3$s ) ) ", expected[0], expected[1], expected[2]);
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
                        "(select * from  %3$s a inner join %2$s b using(personId)))", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (final String table : expected)
            assertTrue(p.getTables().stream().anyMatch(q -> q.getTableName().toUpperCase().equals(table)));
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