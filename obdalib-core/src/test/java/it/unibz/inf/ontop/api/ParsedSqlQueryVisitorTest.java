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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

import static org.junit.Assert.*;

/**
 * Created by salvo on 20/06/2016.
 */
public class ParsedSqlQueryVisitorTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private DBMetadata dbMetadata;
    private QuotedIDFactory idfac;


    public ParsedSqlQueryVisitorTest(){
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
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
    }

    @Test
    public void MetadataContaisExpectedTable(){
        String expected = "PERSON";
        String sql = "select * from " + expected + " p";
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);

        assertFalse(  p.getTables().isEmpty() );
        assertEquals(expected, p.getTables().get(0).getTableName().toUpperCase());
    }

    @Test
    public void MetadataContaisExpectedTwoTables(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = "select * from " + String.join(",", expected) ;
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedThreeTables(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = "select * from " + String.join(",", expected) ;
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedTwoTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1s natural join %2s", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedThreeTablesInNaturalJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1s natural join %2s natural join %3s", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedTwoTablesInJoinUsingWhere(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1s a, %2s b where a.idPerson=b.idPerson ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }


    @Test
    public void MetadataContaisExpectedTwoTablesInCrossJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1s cross join %2s", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedThreeTablesInCrossJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1s cross join %2s cross join %3s", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedTwoTablesInnerJoinON(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1s a inner join %2s b on a.idPerson=b.idPerson ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedThreeTablesInnerJoinON(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1s a inner join %2s b on a.idPerson=b.idPerson inner join %3s c on a.idPerson=c.idPerson", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test(expected = MappingQueryException.class)
    public void MetadataContaisExpectedThreeTablesInnerJoinONMiddleWrong(){
        String [] expected = { "PERSON", "EMAILSSXX", "ADDRESS"};
        String sql = String.format( "select * from %1s a inner join %2s b on a.idPerson=b.idPerson inner join %3s c on a.idPerson=c.idPerson", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedTwoTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1s a inner join %2s b using (idPerson) ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedThreeTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1s a inner join %2s b inner join %3s using (idPerson) ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test(expected= MappingQueryException.class)
    public void MetadataContaisExpectedThreeTablesInnerJoinUsingLastWrong(){
        String [] expected = { "PERSON", "EMAIL", "ADDRE"};
        String sql = String.format( "select * from %1s a inner join %2s b inner join %3s using (idPerson) ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }


    @Test
    public void MetadataContaisExpectedFourTablesInnerJoinUsing(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS", "POSTCODE"};

        String sql = String.format(
                "select a.name as fname, " +
                        "b.email as emailAddress, " +
                        "c.address as personAddress, " +
                        "d.postcode as postc " +
                "from %1s a " +
                "inner join %2s b on a.personId = b.personId " +
                        "inner join %3s c on a.personId = c.personId " +
                        "inner join %4s d on d.idPostcode = c.idPostcode;",
                expected[0], expected[1], expected[2], expected[3]);

        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test
    public void MetadataContaisExpectedTwoTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL"};
        String sql = String.format( "select * from %1s, (select * from %2s ) ", expected[0], expected[1]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }

    @Test(expected = ParseException.class) // this need to be reviewed
    public void MetadataContaisExpectedThreeTablesSubSelectJoin(){
        String [] expected = { "PERSON", "EMAIL", "ADDRESS"};
        String sql = String.format( "select * from %1s, (select * from %2s, (select * from %3s ) ) ", expected[0], expected[1], expected[2]);
        ParsedSqlQueryVisitor p = new ParsedSqlQueryVisitor( (Select) getStatementFromUnquotedSQL(sql), dbMetadata);
        logger.info(String.format( "expected.length: %d, p.getTables().size(): %d ",  expected.length, p.getTables().size() ));
        assertTrue(  p.getTables().size() == expected.length );
        for (int i = 0; i<expected.length; i++) {
            assertEquals(expected[i], p.getTables().get(i).getTableName().toUpperCase());
        }
    }


    private Statement getStatementFromUnquotedSQL(String input) {
        try {
            Statement st = CCJSqlParserUtil.parse(input);
            if (!(st instanceof Select))
                throw new JSQLParserException("The inserted query is not a SELECT statement");
            return st;
        }catch (Exception e) {
            System.out.println( e ) ;
            return null;
        }
    }


    private void createDatabaseRelationDefinition(){

        this.dbMetadata = DBMetadataExtractor.createDummyMetadata();
        this.idfac = dbMetadata.getQuotedIDFactory();

        DatabaseRelationDefinition tdPerson = dbMetadata.createDatabaseRelation(idfac.createRelationID(null, "PERSON"));
        tdPerson.addAttribute(idfac.createAttributeID("idPerson"), Types.INTEGER, null, false);
        tdPerson.addAttribute(idfac.createAttributeID("name"), Types.VARCHAR, null, false);

        DatabaseRelationDefinition tdEmail = dbMetadata.createDatabaseRelation(idfac.createRelationID(null, "EMAIL"));
        tdEmail.addAttribute(idfac.createAttributeID("idEmail"), Types.INTEGER, null, false);
        tdEmail.addAttribute(idfac.createAttributeID("idPerson"), Types.INTEGER, null, false);
        tdEmail.addAttribute(idfac.createAttributeID("email"), Types.VARCHAR, null, false);
        tdEmail.addAttribute(idfac.createAttributeID("active"), Types.BIT, null, false);

        DatabaseRelationDefinition tdAddress = dbMetadata.createDatabaseRelation( idfac.createRelationID(null, "ADDRESS") );
        tdAddress.addAttribute(idfac.createAttributeID("idAddress"), Types.INTEGER, null, false);
        tdAddress.addAttribute(idfac.createAttributeID("idPerson"), Types.INTEGER, null, false);
        tdAddress.addAttribute(idfac.createAttributeID("idPostcode"), Types.INTEGER, null, false);
        tdAddress.addAttribute(idfac.createAttributeID("address"), Types.VARCHAR, null, false);

        DatabaseRelationDefinition tdPostcode = dbMetadata.createDatabaseRelation( idfac.createRelationID(null, "POSTCODE") );
        tdPostcode.addAttribute(idfac.createAttributeID("idPostcode"), Types.INTEGER, null, false);
        tdPostcode.addAttribute(idfac.createAttributeID("postcode"), Types.VARCHAR, null, false);
        tdPostcode.addAttribute(idfac.createAttributeID("city"), Types.VARCHAR, null, false);
        tdPostcode.addAttribute(idfac.createAttributeID("rangeNumbers"), Types.VARCHAR, null, false);

    }





}