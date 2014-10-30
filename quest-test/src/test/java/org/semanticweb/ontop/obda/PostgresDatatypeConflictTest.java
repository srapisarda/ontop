package org.semanticweb.ontop.obda;

/**
 * Test the case for  SQL cast in WHERE Clause for Postgres database, that does not convert automatically integer in char
 */

import org.junit.Before;
import org.junit.Test;
import org.semanticweb.ontop.io.ModelIOManager;
import org.semanticweb.ontop.io.QueryIOManager;
import org.semanticweb.ontop.model.OBDADataFactory;
import org.semanticweb.ontop.model.OBDAModel;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;
import org.semanticweb.ontop.owlrefplatform.core.QuestPreferences;
import org.semanticweb.ontop.owlrefplatform.owlapi3.*;
import org.semanticweb.ontop.querymanager.QueryController;
import org.semanticweb.ontop.querymanager.QueryControllerGroup;
import org.semanticweb.ontop.querymanager.QueryControllerQuery;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class PostgresDatatypeConflictTest {
    private OBDADataFactory fac;

    Logger log = LoggerFactory.getLogger(this.getClass());
    private OBDAModel obdaModel;
    private OWLOntology ontology;

    final String owlFile = "src/test/resources/datatype/person.owl";
    final String obdaFile = "src/test/resources/datatype/person-datatype-pgsql.obda";

    @Before
    public void setUp() throws Exception {

        fac = OBDADataFactoryImpl.getInstance();

        // Loading the OWL file
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        ontology = manager.loadOntologyFromOntologyDocument((new File(owlFile)));

        // Loading the OBDA data
        obdaModel = fac.getOBDAModel();

        ModelIOManager ioManager = new ModelIOManager(obdaModel);
        ioManager.load(obdaFile);

    }



    @Test
    public void testOneQuery() throws Exception {

        QuestPreferences p = new QuestPreferences();

        String query =  "PREFIX : <http://www.example.org/test#> SELECT ?x\n" +
                "WHERE {\n" +
                "   ?x a :Person; :name ?y ;\n" +
                "  :age ?w .\n" +
                "}";


        runTestQuery(p, query);
    }

    private void runTestQuery(Properties p, String query) throws Exception {

        // Creating a new instance of the reasoner
        QuestOWLFactory factory = new QuestOWLFactory();
        factory.setOBDAController(obdaModel);

        factory.setPreferenceHolder(p);

        QuestOWL reasoner = (QuestOWL) factory.createReasoner(ontology, new SimpleConfiguration());

        // Now we are ready for querying
        QuestOWLConnection conn = reasoner.getConnection();
        QuestOWLStatement st = conn.createStatement();


                log.debug("Executing query: ");
                log.debug("Query: \n{}", query);

                long start = System.nanoTime();
                QuestOWLResultSet res = st.executeTuple(query);
                long end = System.nanoTime();

                double time = (end - start) / 1000;

                int count = 0;
                while (res.nextRow()) {
                    count += 1;
                }
                log.debug("Total result: {}", count);

                assertTrue(count == 0);

                log.debug("Elapsed time: {} ms", time);



    }
}

