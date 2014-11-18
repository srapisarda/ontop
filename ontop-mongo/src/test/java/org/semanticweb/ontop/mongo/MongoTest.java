package org.semanticweb.ontop.mongo;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.QueryParserUtil;
import org.semanticweb.ontop.exception.InvalidMappingExceptionWithIndicator;
import org.semanticweb.ontop.exception.InvalidPredicateDeclarationException;
import org.semanticweb.ontop.io.ModelIOManager;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.OBDADataFactory;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.model.OBDAMappingAxiom;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.SQLOBDAModel;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.Predicate.COL_TYPE;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;
import org.semanticweb.ontop.owlrefplatform.core.QuestConstants;
import org.semanticweb.ontop.owlrefplatform.core.QuestPreferences;
import org.semanticweb.ontop.owlrefplatform.core.translator.SparqlAlgebraToDatalogTranslator;
import org.semanticweb.ontop.owlrefplatform.core.unfolding.DatalogUnfolder;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWL;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLConnection;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLFactory;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLResultSet;
import org.semanticweb.ontop.owlrefplatform.owlapi3.QuestOWLStatement;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLException;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;

import com.google.gson.JsonObject;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class MongoTest {

	final String owlfile = "src/test/resources/student.owl";
	final String obdafile = "src/test/resources/student.obda";
	final String mongofile = "/simpleMapping.json";

	QuestOWLStatement statement;
	MongoQueryGenerator queryGenerator;
	
//	@Test
	public void connectToMongoDB() throws UnknownHostException {
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

		DB db = mongoClient.getDB( "db" );
		
		Set<String> colls = db.getCollectionNames();

		for (String s : colls) {
		    System.out.println(s);
		}
		
		
		DBCollection coll = db.getCollection("students");

		//coll.remove(new BasicDBObject());
/*	
		BasicDBObject doc = new BasicDBObject("type", 1)
	    .append("name", "UGStudent1")
	    .append("code", "001")
	    .append("nicknames", "[\"n1_1\",\"n1_2\"]")
	    .append("contact", new BasicDBObject("email", "UGStudent1@ex.org"))
	    .append("age", 8);
		coll.insert(doc);

		doc = new BasicDBObject("type", 1)
	    .append("name", "UGStudent2")
	    .append("code", "002")
	    .append("nicknames", "[\"n2_1\"]")
	    .append("contact", new BasicDBObject("email", "UGStudent2@ex.org"))
	    .append("age", 12);
		coll.insert(doc);
		
		doc = new BasicDBObject("type", 2)
	    .append("name", "GStudent1")
	    .append("code", "003")
	    .append("nicknames", "[]")
	    .append("contact", new BasicDBObject("email", "GStudent1@ex.org"))
	    .append("age", 18);
		coll.insert(doc);

		doc = new BasicDBObject("type", 2)
	    .append("name", "GStudent2")
	    .append("code", "004")
	    .append("contact", new BasicDBObject("email", "GStudent2@ex.org"))
	    .append("age", 20);
		coll.insert(doc);

		doc = new BasicDBObject("type", 1)
	    .append("name", "UGStudent3")
	    .append("code", "005")
	    .append("nicknames", "[\"n5_1\"]")
	    .append("contact", new BasicDBObject())
	    .append("age", 30);
		coll.insert(doc);
*/
		
		DBCursor cursor = coll.find();
		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			System.out.println(obj);
		}
	}
	
/*	@Before
	public void setUpOBDA() throws InvalidMappingExceptionWithIndicator, IOException, InvalidPredicateDeclarationException, OBDAException, OWLException, InvalidMongoMappingException {
		OBDADataFactory dataFactory = OBDADataFactoryImpl.getInstance();
		SQLOBDAModel obdaModel = dataFactory.getOBDAModel();

		QuestOWLFactory factory = new QuestOWLFactory();
		factory.setOBDAController(obdaModel);
		factory.setPreferenceHolder(new QuestPreferences());

		ModelIOManager ioManager = new ModelIOManager(obdaModel);
		ioManager.load(obdafile);
		
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument((new File(owlfile)));
		QuestOWL reasoner = (QuestOWL) factory.createReasoner(ontology, new SimpleConfiguration());

		
		QuestOWLConnection conn = reasoner.getConnection();
		statement = conn.createStatement();

		
		setUpMongo();
	}

	@Before
	public void setUpMongo() throws IOException, InvalidMongoMappingException {
        InputStream stream = MongoMappingParserTest.class.getResourceAsStream(mongofile);
        MongoMappingParser parser = new MongoMappingParser(new InputStreamReader(stream));
        List<OBDAMappingAxiom> mappings =  parser.parse();

        DBMetadata metadata = (new MongoSchemaExtractor()).extractCollectionDefinition(mappings);
		queryGenerator = new MongoQueryGenerator(metadata);
	}

	private void testAndAssertQuery(String sparqlQuery, int expectedResult) throws OWLException, OBDAException, UnknownHostException {
		statement.executeTuple(sparqlQuery);
		
		DatalogProgram programAfterUnfolding = statement.getQuestStatement().getProgramAfterUnfolding();
		List<String> signature = null;
		String mongoQuery = queryGenerator.generateSourceQuery(programAfterUnfolding, signature);
		
		Assert.assertEquals(expectedResult, runQuery(mongoQuery));
	}

	private int runQuery(String mongoQuery) throws UnknownHostException {
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

		DB db = mongoClient.getDB( "db" );
		DBCollection coll = db.getCollection("students");
		DBObject query = (DBObject)JSON.parse(mongoQuery);
		
		DBCursor cursor = coll.find(query);
		int count = 0;
		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			System.out.println(obj);
			count++;
		}
		return count;
	}
	
	@Test
	public void test1() throws OBDAException, OWLException, InvalidMappingExceptionWithIndicator, IOException, InvalidPredicateDeclarationException, InvalidMongoMappingException {
		
		String sparqlQuery = 
				"PREFIX : <http://ex.org/> \n" +
				"SELECT DISTINCT ?x \n" +
				"WHERE { ?x a :Student . }";

		int expectedResult = 5;
		testAndAssertQuery(sparqlQuery, expectedResult);
	}

	@Test
	public void test2() throws OBDAException, OWLException, InvalidMappingExceptionWithIndicator, IOException, InvalidPredicateDeclarationException, InvalidMongoMappingException {
		
		String sparqlQuery = 
				"PREFIX : <http://ex.org/> \n" +
				"SELECT DISTINCT ?x \n" +
				"WHERE { ?x a :GraduateStudent . }";

		int expectedResult = 2;
		testAndAssertQuery(sparqlQuery, expectedResult);
	}

	@Test
	public void test3() throws OBDAException, OWLException, InvalidMappingExceptionWithIndicator, IOException, InvalidPredicateDeclarationException, InvalidMongoMappingException {
		
		String sparqlQuery = 
				"PREFIX : <http://ex.org/> \n" +
				"SELECT DISTINCT ?x ?name \n" +
				"WHERE { ?x a :GraduateStudent ; :name ?name }";

		int expectedResult = 2;
		testAndAssertQuery(sparqlQuery, expectedResult);
	}

*/
}
