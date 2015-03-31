package org.semanticweb.ontop.quest.sparql;

import org.semanticweb.ontop.io.ModelIOManager;
import org.semanticweb.ontop.model.OBDADataFactory;
import org.semanticweb.ontop.model.OBDAModel;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;
import org.semanticweb.ontop.owlrefplatform.core.QuestConstants;
import org.semanticweb.ontop.owlrefplatform.core.QuestPreferences;
import org.semanticweb.ontop.owlrefplatform.owlapi3.*;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;

import java.io.File;

/**
 * Created by elem on 31/03/15.
 */
public class SparqlToDatalogTests {

    /*
	 * Use the sample database using H2 from
	 * https://github.com/ontop/ontop/wiki/InstallingTutorialDatabases
	 *
	 * Please use the pre-bundled H2 server from the above link
	 *
	 * Test with not latin Character
	 *
	 */
    final String owlfile = "src/main/resources/exampleBooks/exampleBooksNotLatin.owl";
    final String obdafile = "src/main/resources/exampleBooks/exampleBooksNotLatin.obda";

    public void runQuery() throws Exception {

		/*
		 * Load the ontology from an external .owl file.
		 */
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new File(owlfile));

		/*
		 * Load the OBDA model from an external .obda file
		 */
        OBDADataFactory fac = OBDADataFactoryImpl.getInstance();
        OBDAModel obdaModel = fac.getOBDAModel();
        ModelIOManager ioManager = new ModelIOManager(obdaModel);
        ioManager.load(obdafile);

		/*
		 * Prepare the configuration for the Quest instance. The example below shows the setup for
		 * "Virtual ABox" mode
		 */
        QuestPreferences preference = new QuestPreferences();
        preference.setCurrentValueOf(QuestPreferences.ABOX_MODE, QuestConstants.VIRTUAL);

		/*
		 * Create the instance of Quest OWL reasoner.
		 */
        QuestOWLFactory factory = new QuestOWLFactory();
        factory.setOBDAController(obdaModel);
        factory.setPreferenceHolder(preference);
        QuestOWL reasoner = (QuestOWL) factory.createReasoner(ontology, new SimpleConfiguration());

		/*
		 * Prepare the data connection for querying.
		 */
        QuestOWLConnection conn = reasoner.getConnection();
        QuestOWLStatement st = conn.createStatement();

		/*
		 * Get the book information that is stored in the database
		 */
        String sparqlQuery =
                "PREFIX : <http://meraka/moss/exampleBooks.owl#> \n" +
                        "SELECT DISTINCT ?x ?title ?author ?y ?genre ?edition \n" +
                        "WHERE { ?x a :книга; :title ?title; :النوع ?genre; :writtenBy ?y; :hasÉdition ?z. \n" +
                        "		 ?y a :作者; :name ?author. \n" +
                        "?z a :Édition; :editionNumber ?edition. \n" +
                        "}" +
                        "ORDER BY (?edition)";

        try {
            long t1 = System.currentTimeMillis();
            QuestOWLResultSet rs = st.executeTuple(sparqlQuery);
            int columnSize = rs.getColumnCount();
            while (rs.nextRow()) {
                for (int idx = 1; idx <= columnSize; idx++) {
                    OWLObject binding = rs.getOWLObject(idx);
                    System.out.print(binding.toString() + ", ");
                }
                System.out.print("\n");
            }
            rs.close();
            long t2 = System.currentTimeMillis();

			/*
			 * Print the query summary
			 */
            QuestOWLStatement qst = (QuestOWLStatement) st;
            String sqlQuery = qst.getUnfolding(sparqlQuery);

            System.out.println();
            System.out.println("The input SPARQL query:");
            System.out.println("=======================");
            System.out.println(sparqlQuery);
            System.out.println();

            System.out.println("The output SQL query:");
            System.out.println("=====================");
            System.out.println(sqlQuery);

            System.out.println("Query Execution Time:");
            System.out.println("=====================");
            System.out.println((t2-t1) + "ms");

        } finally {

			/*
			 * Close connection and resources
			 */
            if (st != null && !st.isClosed()) {
                st.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            reasoner.dispose();
        }
    }

    /**
     * Main client program
     */
    public static void main(String[] args) {
        try {
            SparqlToDatalogTests example = new SparqlToDatalogTests();
            example.runQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
