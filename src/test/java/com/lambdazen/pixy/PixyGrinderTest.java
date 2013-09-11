package com.lambdazen.pixy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.pixy.PixyGrinder;
import com.lambdazen.pixy.PixyPipe;
import com.lambdazen.pixy.PixyTheory;
import com.lambdazen.pixy.gremlin.GremlinPipelineExt;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.structures.Row;

import junit.framework.TestCase;

public class PixyGrinderTest extends TestCase {
	public String[][] KENNEDY_FAMILY_TREE = new String[][] {
			new String[] {"Joseph Patrick Kennedy, Sr.", 
					"male", "1888", "1969", "Joe",
					null, null, "Rose Elizabeth Fitzgerald"},
			new String[] {"Rose Elizabeth Fitzgerald",
					"female", "1890", "1995", null,
					null, null, null},
			new String[] {"Joseph Patrick Kennedy, Jr.",
					"male", "1915", "1944", "Joe Jr.",
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					null},
			new String[] {"John Fitzgerald Kennedy, Sr.",
					"male", "1917", "1963", "Jack",
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					"Jacqueline Lee Bouvier"},
			new String[] {"John Vernou Bouvier III", 
					"male", "1891", "1957", "Black Jack",
					null, null, "Janet Lee Bouvier"},
			new String[] {"Janet Lee Bouvier",
					"female", "1907", "1989", null,
					null, null, null},
			new String[] {"Jacqueline Lee Bouvier",
					"female", "1929", "1994", "Jackie",
					"John Vernou Bouvier III", "Janet Lee Bouvier",
					null},
			new String[] {"Rose Marie Kennedy", 
					"female", "1918", "2005", "Rosemary",
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					null},
			new String[] {"Kathleen Agnes Kennedy",
					"female", "1920", "1948", "Kick",
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					null},
			new String[] {"William John Robert Cavendish",
					"male", "1917", "1944", "Billy",
					"Edward William Spencer Cavendish", "Mary Cavendish",
					"Kathleen Agnes Kennedy"},
			new String[] {"Edward William Spencer Cavendish",
					"male", "1895", "1950", null,
					null, null, "Mary Cavendish"},
			new String[] {"Mary Cavendish", 
					"female", "1895", "1988", null,
					null, null, null},
			new String[] {"Eunice Mary Kennedy", 
					"female", "1921", "2009", null,
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					null},
			new String[] {"Robert Sargent Shriver, Jr.",
					"male", "1915", "2011", "Sarge",
					"Robert Sargent Shriver, Sr.", "Hilda Shriver",
					"Eunice Mary Kennedy"},
			new String[] {"Robert Sargent Shriver, Sr.",
					"male", "1878", "1942", null,
					null, null, "Hilda Shriver"},
			new String[] {"Hilda Shriver",
					"female", "1883", "1977", null,
					null, null, null},
			new String[] {"Patricia Helen Kennedy",
					"female", "1924", "2006", "Pat",
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					null},
			new String[] {"Peter Sydney Ernest Lawford",
					"male", "1923", "1984", null,
					"Sydney Turing Barlow Lawford", "May Sommerville Bunny",
					"Patricia Helen Kennedy"},
			new String[] {"Sydney Turing Barlow Lawford",
					"male", "1865", "1953", null, 
					null, null, "May Sommerville Bunny"},
			new String[] {"May Sommerville Bunny", 
					"female", "1883", "1972", null,
					null, null, null},
			new String[] {"Robert Francis Kennedy, Sr.",
					"male", "1925", "1968", "Bobby",
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					"Ethel Skakel Kennedy"},
			new String[] {"Ethel Skakel Kennedy", 
					"female", "1928", null, null,
					"George Skakel", "Ann Brannack",
					null},
			new String[] {"George Skakel",
					"male", "1892", "1955", null,
					null, null, "Ann Brannack"},
			new String[] {"Ann Brannack",
					"female", "1892", "1955", null,
					null, null, null},
			new String[] {"Jean Ann Kennedy",
					"female", "1928", null, null,
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					null},
			new String[] {"Stephen Edward Smith Sr.",
					"male", "1927", "1990", null,
					null, null,
					"Jean Ann Kennedy"},
			new String[] {"Edward Moore Kennedy, Sr.",
					"male", "1932", "2009", "Ted",
					"Joseph Patrick Kennedy, Sr.", "Rose Elizabeth Fitzgerald",
					"Virginia Joan Kennedy"},
			new String[] {"Virginia Joan Kennedy",
					"female", "1936", null, "Joan",
					"Henry Wiggin Bennett, Jr.", "Virginia Joan Bennet",
					null},
			new String[] {"Henry Wiggin Bennett, Jr.",
					"male", "1895", "1981", null,
					null, null, "Virginia Joan Bennet"},
			new String[] {"Virginia Joan Bennet",
					"female", "1900", "1975", null,
					null, null, null},
			new String[] {"Caroline Bouvier Kennedy",
					"female", "1957", null, null,
					"John Fitzgerald Kennedy, Sr.", "Jacqueline Lee Bouvier",
					null},
			new String[] {"Edwin Arthur Schlossberg",
					"male", "1945", null, null,
					null, null,
					"Caroline Bouvier Kennedy"},
			new String[] {"John Fitzgerald Kennedy, Jr.",
					"male", "1960", "1999", "John-John",
					"John Fitzgerald Kennedy, Sr.", "Jacqueline Lee Bouvier",
					"Carolyn Jeanne Bessette"},
			new String[] {"Carolyn Jeanne Bessette", 
					"female", "1966", "1999", null,
					null, null, null},
			new String[] {"Robert Sargent Shriver III",
					"male", "1954", null, "Bobby",
					"Robert Sargent Shriver, Jr.", "Eunice Mary Kennedy",
					"Malissa Feruzzi"},
			new String[] {"Malissa Feruzzi",
					"female", "1963", null, "Mary Elizabeth",
					null, null, null},
			new String[] {"Maria Owings Shriver",
					"female", "1955", null, null,
					"Robert Sargent Shriver, Jr.", "Eunice Mary Kennedy",
					null},
			new String[] {"Arnold Schwarzenegger",
					"male", "1947", null, null,
					null, null,
					"Maria Owings Shriver"},
			new String[] {"Timothy Perry Shriver",
					"male", "1959", null, null,
					"Robert Sargent Shriver, Jr.", "Eunice Mary Kennedy",
					"Linda Sophia Potter"},
			new String[] {"Linda Sophia Potter", 
					"female", "1956", null, null,
					null, null, null},
			new String[] {"Mark Kennedy Shriver",
					"male", "1964", null, null,
					"Robert Sargent Shriver, Jr.", "Eunice Mary Kennedy",
					"Jeanne Eileen Ripp"},
			new String[] {"Jeanne Eileen Ripp", 
					"female", "1965", null, "Jeannie",
					null, null, null},
			new String[] {"Anthony Paul Kennedy Shriver",
					"male", "1965", null, null,
					"Robert Sargent Shriver, Jr.", "Eunice Mary Kennedy",
					"Alina Mojica"},
			new String[] {"Alina Mojica",
					"female", "1965", null, null,
					null, null, null},
			new String[] {"Christopher Kennedy Lawford",
					"male", "1955", null, null,
					"Peter Sydney Ernest Lawford", "Patricia Helen Kennedy",
					null},
			new String[] {"Sydney Maleia Kennedy Lawford",
					"female", "1956", null, null,
					"Peter Sydney Ernest Lawford", "Patricia Helen Kennedy",
					null},
			new String[] {"James Peter McKelvy, Sr",
					"male", "1955", null, null,
					null, null,
					"Sydney Maleia Kennedy Lawford"},
			new String[] {"Victoria Francis Lawford",
					"female", "1958", null, null,
					"Peter Sydney Ernest Lawford", "Patricia Helen Kennedy",
					null},
			new String[] {"Robert Beebe Pender, Jr.",
					"male", "1953", null, null,
					null, null,
					"Victoria Francis Lawford"},
			new String[] {"Robin Elizabeth Lawford",
					"female", "1961", null, null,
					"Peter Sydney Ernest Lawford", "Patricia Helen Kennedy",
					null},
			new String[] {"Stephen Edward Smith, Jr.",
					"male", "1957", null, null,
					"Stephen Edward Smith Sr.", "Jean Ann Kennedy",
					null},
			new String[] {"William Kennedy Smith", 
					"male", "1960", null, null,
					"Peter Sydney Ernest Lawford", "Patricia Helen Kennedy",
					"Anne Henry"},
			new String[] {"Anne Henry",
					"female", "1975", null, null,
					null, null, null},
			new String[] {"Kara Anne Kennedy",
					"female", "1960", null, null,
					"Edward Moore Kennedy, Sr.", "Virginia Joan Kennedy",
					null},
			new String[] {"Edward Moore Kennedy, Jr.",
					"male", "1961", null, "Ted",
					"Edward Moore Kennedy, Sr.", "Virginia Joan Kennedy",
					"Katherine Anne Gershman"},
			new String[] {"Katherine Anne Gershman",
					"female", "1959", null, "Kiki",
					null, null, null},
			new String[] {"Patrick Joseph Kennedy II",
					"male", "1967", null, null,
					"Edward Moore Kennedy, Sr.", "Virginia Joan Kennedy",
					"Amy Savell"},
			new String[] {"Amy Savell",
					"female", "1979", null, null,
					null, null, null}
	};
	
	BitsyGraph graph;
	
	public PixyGrinderTest() {
		
	}
	
	@Override
	public void setUp() {
		//graph = new BitsyGraph(Paths.get("C:/sridhar/temp/kennedy"));
		graph = new BitsyGraph();
		graph.createKeyIndex("name", Vertex.class);
		
		loadGraph();
	}
	
	@Override
	public void tearDown() {
		if (graph != null) {
			graph.shutdown();
		}
		graph = null;
	}
	
	public void loadGraph() {
		for (String[] kennedy : KENNEDY_FAMILY_TREE) {
			Vertex v = graph.addVertex(null);
			
			v.setProperty("name", kennedy[0]);
			v.setProperty("sex", kennedy[1]);
			v.setProperty("born", new Integer(kennedy[2]));
			
			if (kennedy[3] != null) {
				v.setProperty("died", new Integer(kennedy[3]));
			}
			
			if (kennedy[4] != null) {
				v.setProperty("nickname", kennedy[4]);
			}
		}
		
		for (String[] kennedy : KENNEDY_FAMILY_TREE) {
			Vertex v = graph.getVertices("name", kennedy[0]).iterator().next();
			
			assertTrue(v != null);
			
			addRelationship(v, "father", kennedy[5]);
			addRelationship(v, "mother", kennedy[6]);
			addRelationship(v, "wife", kennedy[7]);
		}
		
		graph.commit();		
	}

	private void addRelationship(Vertex v, String label, String name) {
		if (name != null) {
			try {
				Vertex other = graph.getVertices("name", name).iterator().next();			
				assertTrue(other != null);
			
				if (label.equals("father")) {
					assertEquals("for " + name, "male", other.getProperty("sex"));
				} else if (label.equals("mother")) {
					assertEquals("for " + name, "female", other.getProperty("sex"));
				} else if (label.equals("wife")) {
					assertEquals("for " + name + " with " + other.getProperty("sex").getClass(), "female", other.getProperty("sex"));
				}
	
				v.addEdge(label, other);
			} catch (NoSuchElementException e) {
				throw new RuntimeException("Unable to add " + label + " link to " + name, e);
			}
		}
	}
	
	public void testRelationships() {
		testRelationships(true);
	}
	
	public void testRelationshipsComplex() {
		testRelationships(false);
	}
	
	public void testRelationships(boolean shortRules) {
		// Go through various conditions
		
		PixyTheory pt = new PixyTheory(
				"father(Child, Father) :- out(Child, 'father', Father). " +
				"mother(Child, Mother) :- out(Child, 'mother', Mother). " +
				"wife(Wife, Husband) :- out(Husband, 'wife', Wife). " +
				"husband(Husband, Wife) :- wife(Wife, Husband).");
		
		PixyGrinder pg = new PixyGrinder(pt);
		
		PixyPipe pp = pg.convertQueryToPipe("father($c, &f)");

		// Father
		GremlinPipeline pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.").as("c");
		pipeline = pp.pixyStep(pipeline);
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		Vertex v = (Vertex)pipeline.next();
		assertEquals("Joseph Patrick Kennedy, Sr.", v.getProperty("name"));

		// Mother -- reverse
		pipeline = new GremlinPipeline(graph).V("name", "Rose Elizabeth Fitzgerald");
		pp = pt.makePipe("mother(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		assertEquals("Edward Moore Kennedy, Sr.", pipeline.next());
		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Jean Ann Kennedy", pipeline.next());
		assertEquals("John Fitzgerald Kennedy, Sr.", pipeline.next());
		assertEquals("Joseph Patrick Kennedy, Jr.", pipeline.next());
		assertEquals("Kathleen Agnes Kennedy", pipeline.next());
		assertEquals("Patricia Helen Kennedy", pipeline.next());
		assertEquals("Robert Francis Kennedy, Sr.", pipeline.next());
		assertEquals("Rose Marie Kennedy", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Husband -- reverse
		pipeline = new GremlinPipeline(graph).V("name", "Jacqueline Lee Bouvier");
		pp = pt.makePipe("husband(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertEquals("John Fitzgerald Kennedy, Sr.", pipeline.next());
		assertFalse(pipeline.hasNext());
		
		// Extend to spouse and parent (OR clauses)
		pt = pt.extend("spouse(Spouse1, Spouse2) :- wife(Spouse2, Spouse1)." +
				"spouse(Spouse1, Spouse2) :- wife(Spouse1, Spouse2). " +
				"parent(Child, Parent) :- father(Child, Parent). " + 
				"parent(Child, Parent) :- mother(Child, Parent). "); 

		runSpouseTests(pt);
		runParentTests(pt);
		
		// Re-defined spouse and parent from scratch
		if (shortRules) {
			pt = pt.remove("spouse/2", "parent/2").extend(
					"spouse(Spouse1, Spouse2) :- both(Spouse1, 'wife', Spouse2)." +
					"parent(Child, Parent) :- out(Child, ['father', 'mother'], Parent).");
		}
		
		runSpouseTests(pt);
		runParentTests(pt);
		
		pipeline = new GremlinPipeline(graph).V("name", "Jacqueline Lee Bouvier");
		pp = pt.makePipe("parent(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertEquals("Caroline Bouvier Kennedy", pipeline.next());
		assertEquals("John Fitzgerald Kennedy, Jr.", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Extend theory to include grandparent -- AND of OR conditions
		pt = pt.extend("grandparent(Child, GrandParent) :- parent(Child, Father), parent(Father, GrandParent).");

		// Grandparents of JFK Jr.
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Jr.");
		pp = pt.makePipe("grandparent($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertEquals("Janet Lee Bouvier", pipeline.next());
		assertEquals("John Vernou Bouvier III", pipeline.next());
		assertEquals("Joseph Patrick Kennedy, Sr.", pipeline.next());
		assertEquals("Rose Elizabeth Fitzgerald", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Grandchildren of Rose Elizabeth Fitzgerald 
		pipeline = new GremlinPipeline(graph).V("name", "Rose Elizabeth Fitzgerald");
		pp = pt.makePipe("grandparent(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Anthony Paul Kennedy Shriver", pipeline.next());
		assertEquals("Caroline Bouvier Kennedy", pipeline.next());
		assertEquals("Christopher Kennedy Lawford", pipeline.next());
		assertEquals("Edward Moore Kennedy, Jr.", pipeline.next());
		assertEquals("John Fitzgerald Kennedy, Jr.", pipeline.next());
		assertEquals("Kara Anne Kennedy", pipeline.next());
		assertEquals("Maria Owings Shriver", pipeline.next());
		assertEquals("Mark Kennedy Shriver", pipeline.next());
		assertEquals("Patrick Joseph Kennedy II", pipeline.next());
		assertEquals("Robert Sargent Shriver III", pipeline.next());
		assertEquals("Robin Elizabeth Lawford", pipeline.next());
		assertEquals("Stephen Edward Smith, Jr.", pipeline.next());
		assertEquals("Sydney Maleia Kennedy Lawford", pipeline.next());
		assertEquals("Timothy Perry Shriver", pipeline.next());
		assertEquals("Victoria Francis Lawford", pipeline.next());
		assertEquals("William Kennedy Smith", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Extend theory to include son and daughter (expression matches)
		pt = pt.extend("daughter(Parent, Child) :- parent(Child, Parent), property(Child, 'sex', 'female')." +
				"son(Parent, Child) :- parent(Child, Parent), property(Child, 'sex', 'male').");
		
		// Daughters of Rose
		pipeline = new GremlinPipeline(graph).V("name", "Rose Elizabeth Fitzgerald");
		pp = pt.makePipe("daughter($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Jean Ann Kennedy", pipeline.next());
		assertEquals("Kathleen Agnes Kennedy", pipeline.next());
		assertEquals("Patricia Helen Kennedy", pipeline.next());
		assertEquals("Rose Marie Kennedy", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Father and mother of JFK Sr.
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("son(&p, $)");
		pipeline = pp.pixyStep(pipeline);
		pipeline = GremlinPipelineExt.coalesce(pipeline, "p").property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Joseph Patrick Kennedy, Sr.", pipeline.next());
		assertEquals("Rose Elizabeth Fitzgerald", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Sons of JFK Sr.
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.").as("p");
		pp = pt.makePipe("son($p, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("John Fitzgerald Kennedy, Jr.", pipeline.next());
		assertFalse(pipeline.hasNext());
		
		// Siblings of JFK Sr.
		pt = pt.extend("sibling(Person1, Person2) :- father(Person1, Parent), father(Person2, Parent), Person1 \\= Person2. ");
		
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("sibling($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		System.out.println("test1");
		assertEquals("Edward Moore Kennedy, Sr.", pipeline.next());
		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Jean Ann Kennedy", pipeline.next());
		assertEquals("Joseph Patrick Kennedy, Jr.", pipeline.next());
		assertEquals("Kathleen Agnes Kennedy", pipeline.next());
		assertEquals("Patricia Helen Kennedy", pipeline.next());
		assertEquals("Robert Francis Kennedy, Sr.", pipeline.next());
		assertEquals("Rose Marie Kennedy", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Brothers and sisters
		pt = pt.extend(
				"brother(Person1, Person2) :- sibling(Person1, Person2), property(Person2, 'sex', 'male')."
              + "sister(Person1, Person2) :- sibling(Person1, Person2), property(Person2, 'sex', 'female').");

		// People to whom JFK Sr. is a brother 
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("brother(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Edward Moore Kennedy, Sr.", pipeline.next());
		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Jean Ann Kennedy", pipeline.next());
		assertEquals("Joseph Patrick Kennedy, Jr.", pipeline.next());
		assertEquals("Kathleen Agnes Kennedy", pipeline.next());
		assertEquals("Patricia Helen Kennedy", pipeline.next());
		assertEquals("Robert Francis Kennedy, Sr.", pipeline.next());
		assertEquals("Rose Marie Kennedy", pipeline.next());
		assertFalse(pipeline.hasNext());

		// JFK Sr's brothers
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("brother($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Edward Moore Kennedy, Sr.", pipeline.next());
		assertEquals("Joseph Patrick Kennedy, Jr.", pipeline.next());
		assertEquals("Robert Francis Kennedy, Sr.", pipeline.next());
		assertFalse(pipeline.hasNext());

		// RFK Sr's sisters
		pipeline = new GremlinPipeline(graph).V("name", "Robert Francis Kennedy, Sr.");
		pp = pt.makePipe("sister($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Jean Ann Kennedy", pipeline.next());
		assertEquals("Kathleen Agnes Kennedy", pipeline.next());
		assertEquals("Patricia Helen Kennedy", pipeline.next());
		assertEquals("Rose Marie Kennedy", pipeline.next());
		assertFalse(pipeline.hasNext());

		// JFK Sr.'s younger brothers 
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.extend("youngerBrother(X, Y) :- brother(X, Y), property(X, 'born', BX), property(Y, 'born', BY), BY > BX. ")
				.makePipe("youngerBrother($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Edward Moore Kennedy, Sr.", pipeline.next());
		assertEquals("Robert Francis Kennedy, Sr.", pipeline.next());
		assertFalse(pipeline.hasNext());

		// RFK Sr.'s older sisters 
		pipeline = new GremlinPipeline(graph).V("name", "Robert Francis Kennedy, Sr.");

		// duplicating property(X, 'born', BX) to test matching of value in property 
		pp = pt.extend("olderSister(X, Y) :- sister(X, Y), property(X, 'born', BX), property(X, 'born', BX), property(Y, 'born', BY), BY < BX. ")
				.makePipe("olderSister($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Kathleen Agnes Kennedy", pipeline.next());
		assertEquals("Patricia Helen Kennedy", pipeline.next());
		assertEquals("Rose Marie Kennedy", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Brother in law and sister in law
		pt = pt.extend(
				"brotherInLaw(Person, BIL) :- spouse(Person, Spouse), brother(Spouse, BIL). "
			+	"brotherInLaw(Person, BIL) :- sibling(Person, Sister), husband(Sister, BIL). "
			+	"sisterInLaw(Person, SIL) :- spouse(Person, Spouse), sister(Spouse, SIL). "
			+	"sisterInLaw(Person, SIL) :- sibling(Person, Sister), wife(Sister, SIL). ");
		
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.extend("bOrSIL(X, Y) :- brotherInLaw(X, Y). bOrSIL(X, Y) :- sisterInLaw(X, Y). ")
				.makePipe("bOrSIL($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Ethel Skakel Kennedy", pipeline.next());
		assertEquals("Peter Sydney Ernest Lawford", pipeline.next());
		assertEquals("Robert Sargent Shriver, Jr.", pipeline.next());
		assertEquals("Stephen Edward Smith Sr.", pipeline.next());
		assertEquals("Virginia Joan Kennedy", pipeline.next());
		assertEquals("William John Robert Cavendish", pipeline.next());
		assertFalse(pipeline.hasNext());
		
		// Aunt and uncle
		pt = pt.extend("aunt(Person, Aunt) :- parent(Person, Parent), sister(Parent, Aunt). "
				+ "uncle(Person, Aunt) :- parent(Person, Parent), brother(Parent, Aunt)."
				+ "niece(Person, Niece) :- sibling(Person, Sibling), daughter(Sibling, Niece). " 
				+ "nephew(Person, Nephew) :- sibling(Person, Sibling), son(Sibling, Nephew)."); 	

		// Dead nephews of Teddy Kennedy
		pipeline = new GremlinPipeline(graph).V("name", "Edward Moore Kennedy, Sr.");
		pp = pt.extend("deadNephew(Person, Nephew) :- nephew(Person, Nephew), property(Nephew, 'died', _), property(Person, 'died', _).").makePipe("deadNephew($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("John Fitzgerald Kennedy, Jr.", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Nieces of JFK Sr.
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("niece($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Kara Anne Kennedy", pipeline.next());
		assertEquals("Maria Owings Shriver", pipeline.next());
		assertEquals("Robin Elizabeth Lawford", pipeline.next());
		assertEquals("Sydney Maleia Kennedy Lawford", pipeline.next());
		assertEquals("Victoria Francis Lawford", pipeline.next());
		assertFalse(pipeline.hasNext());

		// People to whom JFK Sr. is an aunt
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("aunt(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertFalse(pipeline.hasNext());

		// People to whom JFK Sr. is an uncle
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("uncle(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Anthony Paul Kennedy Shriver", pipeline.next());
		assertEquals("Christopher Kennedy Lawford", pipeline.next());
		assertEquals("Edward Moore Kennedy, Jr.", pipeline.next());
		assertEquals("Kara Anne Kennedy", pipeline.next());
		assertEquals("Maria Owings Shriver", pipeline.next());
		assertEquals("Mark Kennedy Shriver", pipeline.next());
		assertEquals("Patrick Joseph Kennedy II", pipeline.next());
		assertEquals("Robert Sargent Shriver III", pipeline.next());
		assertEquals("Robin Elizabeth Lawford", pipeline.next());
		assertEquals("Stephen Edward Smith, Jr.", pipeline.next());
		assertEquals("Sydney Maleia Kennedy Lawford", pipeline.next());
		assertEquals("Timothy Perry Shriver", pipeline.next());
		assertEquals("Victoria Francis Lawford", pipeline.next());
		assertEquals("William Kennedy Smith", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Same as before, but with a cut
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.extend("funcle(X, Y) :- uncle(Z, Y), !, uncle(X, Y). ").makePipe("funcle(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Anthony Paul Kennedy Shriver", pipeline.next());
		assertEquals("Christopher Kennedy Lawford", pipeline.next());
		assertEquals("Edward Moore Kennedy, Jr.", pipeline.next());
		assertEquals("Kara Anne Kennedy", pipeline.next());
		assertEquals("Maria Owings Shriver", pipeline.next());
		assertEquals("Mark Kennedy Shriver", pipeline.next());
		assertEquals("Patrick Joseph Kennedy II", pipeline.next());
		assertEquals("Robert Sargent Shriver III", pipeline.next());
		assertEquals("Robin Elizabeth Lawford", pipeline.next());
		assertEquals("Stephen Edward Smith, Jr.", pipeline.next());
		assertEquals("Sydney Maleia Kennedy Lawford", pipeline.next());
		assertEquals("Timothy Perry Shriver", pipeline.next());
		assertEquals("Victoria Francis Lawford", pipeline.next());
		assertEquals("William Kennedy Smith", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Same as before, but with an expanded defn of uncle
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.extend("funcle(X, Y) :- uncle(Z, Y), !, parent(X, T), brother(T, Y). ").makePipe("funcle(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		assertEquals("Anthony Paul Kennedy Shriver", pipeline.next());
		assertEquals("Christopher Kennedy Lawford", pipeline.next());
		assertEquals("Edward Moore Kennedy, Jr.", pipeline.next());
		assertEquals("Kara Anne Kennedy", pipeline.next());
		assertEquals("Maria Owings Shriver", pipeline.next());
		assertEquals("Mark Kennedy Shriver", pipeline.next());
		assertEquals("Patrick Joseph Kennedy II", pipeline.next());
		assertEquals("Robert Sargent Shriver III", pipeline.next());
		assertEquals("Robin Elizabeth Lawford", pipeline.next());
		assertEquals("Stephen Edward Smith, Jr.", pipeline.next());
		assertEquals("Sydney Maleia Kennedy Lawford", pipeline.next());
		assertEquals("Timothy Perry Shriver", pipeline.next());
		assertEquals("Victoria Francis Lawford", pipeline.next());
		assertEquals("William Kennedy Smith", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Cuts and not conditions
		pt = pt.extend(
				//"childless(Person) :- parent(Child, Person), !, fail."
				"childless(Person) :- not(parent(Child, Person))."
//			  + "childless(Person)."
			  + "living(Person) :- not(property(Person, 'died', _))."
//			  + "living(Person) :- property(Person, 'died', Year), !, fail."
//			  + "living(Person)."
			  + "livingAsOf(Person, Year) :- property(Person, 'born', Year1), Year1 =< Year, livingAsOfSub(Person, Year)." 
//			  + "livingAsOfSub(Person, Year) :- property(Person, 'died', Year2), Year2 >= Year."
//			  + "livingAsOfSub(Person, Year) :- not(property(Person, 'died', _)). "
			  + "livingAsOfSub(Person, Year) :- property(Person, 'died', Year2), !, Year2 >= Year."
			  + "livingAsOfSub(Person, Year)."
              );
		
		// Is Rose Marie Kennedy child less?
		pipeline = new GremlinPipeline(graph).V("name", "Rose Marie Kennedy");
		pp = pt.makePipe("childless($)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		// Yes
		assertTrue(pipeline.hasNext());
		pipeline.next();
		assertFalse(pipeline.hasNext());
		
		// Childless aunts of JFK Jr
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Jr.");
		pp = pt.extend("clAunt(X, Y) :- aunt(X, Y), childless(Y). ").makePipe("clAunt($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		// RFK's children aren't in the database
		assertEquals("Kathleen Agnes Kennedy", pipeline.next());
		assertEquals("Rose Marie Kennedy", pipeline.next());
		assertFalse(pipeline.hasNext());
		
		// Living nephews of Teddy Kennedy
		pipeline = new GremlinPipeline(graph).V("name", "Edward Moore Kennedy, Sr.");
		pp = pt.extend("livingNephew(Person, Nephew) :- nephew(Person, Nephew), living(Nephew).").makePipe("livingNephew($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		assertEquals("Anthony Paul Kennedy Shriver", pipeline.next());
		assertEquals("Christopher Kennedy Lawford", pipeline.next());
		assertEquals("Mark Kennedy Shriver", pipeline.next());
		assertEquals("Robert Sargent Shriver III", pipeline.next());
		assertEquals("Stephen Edward Smith, Jr.", pipeline.next());
		assertEquals("Timothy Perry Shriver", pipeline.next());
		assertEquals("William Kennedy Smith", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Survivors of a person
		pt = pt.extend(
				"survivor(Person, Survivor) :- property(Person, 'died', Year), survivorSub(Person, Survivor), livingAsOf(Survivor, Year)."
			+   "survivorSub(Person, Survivor) :- spouse(Person, Survivor). "
			+   "survivorSub(Person, Survivor) :- parent(Survivor, Person). "
			+   "survivorSub(Person, Survivor) :- parent(Child, Person), spouse(Child, Survivor). "
			+   "survivorSub(Person, Survivor) :- grandparent(Survivor, Person). ");

		// Survivors of JFK Sr
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("survivor($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
			 
		assertEquals("Caroline Bouvier Kennedy", pipeline.next());
		assertEquals("Edwin Arthur Schlossberg", pipeline.next());
		assertEquals("Jacqueline Lee Bouvier", pipeline.next());
		assertEquals("John Fitzgerald Kennedy, Jr.", pipeline.next());
		assertFalse(pipeline.hasNext());

		// Survivors of Joseph Patrick Kennedy, Jr.
		pipeline = new GremlinPipeline(graph).V("name", "Joseph Patrick Kennedy, Jr.");
		pp = pt.makePipe("survivor($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		assertFalse(pipeline.hasNext());

		// Recursion test
		pipeline = new GremlinPipeline(graph).V("name", "Joseph Patrick Kennedy, Jr.");

		try {
			pp = pt.extend("one(Y, X) :- two(X, Y). two(X, Y) :- two(Y). two(Y) :- one(X, Y). ").makePipe("two($, &)");
			fail("Recursion not detected");
		} catch (Exception e) {
			// OK
		}

		// Descendents of Hilda Shriver
		pt = pt.extend("descendant(Person, Descendant) :- inLoop(Person, ['father', 'mother'], Descendant).");
		pipeline = new GremlinPipeline(graph).V("name", "Hilda Shriver");
		pp = pt.makePipe("descendant($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);		

		assertEquals("Anthony Paul Kennedy Shriver", pipeline.next());
		assertEquals("Maria Owings Shriver", pipeline.next());
		assertEquals("Mark Kennedy Shriver", pipeline.next());
		assertEquals("Robert Sargent Shriver III", pipeline.next());
		assertEquals("Robert Sargent Shriver, Jr.", pipeline.next());
		assertEquals("Timothy Perry Shriver", pipeline.next());		
		assertFalse(pipeline.hasNext());

		// People to whom Timothy Perry Shriver is a descendant
		pipeline = new GremlinPipeline(graph).V("name", "Timothy Perry Shriver");
		pp = pt.makePipe("descendant(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);		

		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Hilda Shriver", pipeline.next());
		assertEquals("Joseph Patrick Kennedy, Sr.", pipeline.next());
		assertEquals("Robert Sargent Shriver, Jr.", pipeline.next());
		assertEquals("Robert Sargent Shriver, Sr.", pipeline.next());
		assertEquals("Rose Elizabeth Fitzgerald", pipeline.next());

		// Run the same query using ancestor
		pt = pt.extend("ancestor(Person, Descendant) :- outLoop(Person, ['father', 'mother'], Descendant).");
		pipeline = new GremlinPipeline(graph).V("name", "Timothy Perry Shriver");
		pp = pt.makePipe("ancestor($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);		

		assertEquals("Eunice Mary Kennedy", pipeline.next());
		assertEquals("Hilda Shriver", pipeline.next());
		assertEquals("Joseph Patrick Kennedy, Sr.", pipeline.next());
		assertEquals("Robert Sargent Shriver, Jr.", pipeline.next());
		assertEquals("Robert Sargent Shriver, Sr.", pipeline.next());
		assertEquals("Rose Elizabeth Fitzgerald", pipeline.next());
		
		// Ancestors with min/max loop limit of 2 are the same as grandparents
		PixyTheory ptTemp = pt.extend(
				"ancestor_2(Person, Ancestor) :- outLoop(Person, ['father', 'mother'], Ancestor, [2, 2])." +
				"descendant_2(Person, Descendant) :- inLoop(Person, ['father', 'mother'], Descendant, [2, 2])." +
				"grandparents_not_ancestors(Person, GrandParent) :- grandparent(Person, GrandParent), not(ancestor_2(Person, GrandParent))." + 
				"ancestors_not_grandparents(Person, GrandParent) :- descendant_2(GrandParent, Person), not(grandparent(Person, GrandParent))." +
				"buggy_people(Person, GrandParent) :- grandparents_not_ancestors(Person, GrandParent). " + 
				"buggy_people(Person, GrandParent) :- ancestors_not_grandparents(Person, GrandParent). "
				);
		
		pipeline = new GremlinPipeline(graph).V();
		pp = ptTemp.makePipe("buggy_people($, &)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);		

		assertFalse(pipeline.hasNext());

		// Ancestors with min/max loop limit of 1, 2 are the same as parents, or grandparents
		ptTemp = pt.extend(
				"ancestor_1_2(Person, Ancestor) :- outLoop(Person, ['father', 'mother'], Ancestor, [1, 2])." +
				"descendant_1_2(Person, Descendant) :- inLoop(Person, ['father', 'mother'], Descendant, [1, 2])." +
				"parentOrGrandParent(Person, Pogp) :- parent(Person, Pogp). " +
				"parentOrGrandParent(Person, Pogp) :- grandparent(Person, Pogp). " + 
				"grandparents_not_ancestors(Person, GrandParent) :- parentOrGrandParent(Person, GrandParent), not(ancestor_1_2(Person, GrandParent))." + 
				"ancestors_not_grandparents(Person, GrandParent) :- descendant_1_2(GrandParent, Person), not(parentOrGrandParent(Person, GrandParent))." +
				"buggy_people(Person, GrandParent) :- grandparents_not_ancestors(Person, GrandParent). " + 
				"buggy_people(Person, GrandParent) :- ancestors_not_grandparents(Person, GrandParent). "
				);

		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);		

		pipeline = new GremlinPipeline(graph).V().as("p");
		pp = ptTemp.makePipe("buggy_people($, &g)");
		pipeline = pp.pixyStep(pipeline);
		
		assertFalse(pipeline.hasNext());
	}

	private void runParentTests(PixyTheory pt) {
		PixyPipe pp;
		GremlinPipeline pipeline;
		// Parent tests
		pipeline = new GremlinPipeline(graph).V("name", "Jacqueline Lee Bouvier").as("c");
		pp = pt.makePipe("parent($c, &p)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertEquals("Janet Lee Bouvier", pipeline.next());
		assertEquals("John Vernou Bouvier III", pipeline.next());
		assertFalse(pipeline.hasNext());
	}

	private void runSpouseTests(PixyTheory pt) {
		PixyPipe pp;
		GremlinPipeline pipeline;
		// Spouse tests
		pipeline = new GremlinPipeline(graph).V("name", "Jacqueline Lee Bouvier");
		pp = pt.makePipe("spouse(&, $)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertEquals("John Fitzgerald Kennedy, Sr.", pipeline.next());
		assertFalse(pipeline.hasNext());
		
		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.").as("w");
		pp = pt.makePipe("spouse(&h, $w)");
		pipeline = pp.pixyStep(pipeline).property("name").order();
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertEquals("Jacqueline Lee Bouvier", pipeline.next());
		assertFalse(pipeline.hasNext());
	}
	
	// Get code coverage for various pipe makers
	public void testPipes() {
		// Go over all edges and make sure that the relationships hold		
		PixyTheory pt = new PixyTheory(
				"testIn(Vin, Vout) :- in(Vin, Vout), !, 'foo' = 'bar'." +
				"testIn(Vin, Vout)." +
				"testIn2(Vin, Vout) :- inE(Vin, E), outV(E, Vout), !, fail." +
				"testIn2(Vin, Vout)." +
				"testOut(Vin, Vout) :- out(Vout, Vin), !, 1 = 2." +
				"testOut(Vin, Vout)." +
				"testOut2(Vin, Vout) :- outE(Vout, E), inV(E, Vin), !, fail." +
				"testOut2(Vin, Vout)." +
				"testBoth(Vin, Vout) :- both(Vin, Vout), !, both(Vout, Vin), !, 2 is 1." +
				"testBoth(Vin, Vout)." +
				"testBoth2(Vin, Vout) :- bothE(Vin, E), bothV(E, Vout), !, fail." +
				"testBoth2(Vin, Vout)." +
				"testInOutBoth(Vin, Vout) :- testIn(Vin, Vout)." +
				"testInOutBoth(Vin, Vout) :- testIn2(Vin, Vout)." +
				"testInOutBoth(Vin, Vout) :- testOut(Vin, Vout)." +
				"testInOutBoth(Vin, Vout) :- testOut2(Vin, Vout)." +
				"testInOutBoth(Vin, Vout) :- testBoth(Vin, Vout)." +
				"testInOutBoth(Vin, Vout) :- testBoth2(Vin, Vout)."
				);
		
		GremlinPipeline pipeline = new GremlinPipeline(graph).E()
			.as("e").inV().as("xvin");
		pipeline = GremlinPipelineExt.coalesce(pipeline, "e").outV().as("xvout");
		PixyPipe pp = pt.makePipe("testInOutBoth($xvin, $xvout)");
		pipeline = pp.pixyStep(pipeline);
		
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertFalse(pipeline.hasNext());

		// Go over all edges and make sure that the relationships hold		
		pt = new PixyTheory(
				"testIn(Vin, Label, Vout) :- in(Vin, Label, Vout), 'father' = Label, !, fail." +
				"testIn(Vin, Label, Vout)." +
				"testIn2(Vin, Label, Vout) :- inE(Vin, Label, E), label(E, Label), outV(E, Vout), !, fail." +
				"testIn2(Vin, Label, Vout)." +
				"testOut(Vin, Label, Vout) :- out(Vout, Label, Vin), !, fail." +
				"testOut(Vin, Label, Vout)." +
				"testOut2(Vin, Label, Vout) :- outE(Vout, Label, E), label(E, ActLabel), ActLabel = Label, inV(E, Vin), !, '1' is '2'." +
				"testOut2(Vin, Label, Vout)." +
				"testBoth(Vin, Label, Vout) :- both(Vin, Label, Vout), !, both(Vout, Label, Vin), !, fail." +
				"testBoth(Vin, Label, Vout)." +
				"testBoth2(Vin, Label, Vout) :- bothE(Vin, Label, E), label(E, ActLabel), Label = ActLabel, bothV(E, Vout), !, ActLabel = 'foo'." +
				"testBoth2(Vin, Label, Vout)." +
				"testInOutBoth(Vin, Label, Vout) :- testIn(Vin, Label, Vout)." +
				"testInOutBoth(Vin, Label, Vout) :- testIn2(Vin, Label, Vout)." +
				"testInOutBoth(Vin, Label, Vout) :- testOut(Vin, Label, Vout)." +
				"testInOutBoth(Vin, Label, Vout) :- testOut2(Vin, Label, Vout)." +
				"testInOutBoth(Vin, Label, Vout) :- testBoth(Vin, Label, Vout)." +
				"testInOutBoth(Vin, Label, Vout) :- testBoth2(Vin, Label, Vout)."
				);
		
		pipeline = new GremlinPipeline(graph).E().filter(new PipeFunction<Edge, Boolean>() {
			@Override
			public Boolean compute(Edge e) {
				return e.getLabel().equals("father");
			}
		}).as("e").inV().as("vin");
		pipeline = GremlinPipelineExt.coalesce(pipeline, "e").outV().as("vout");
		pp = pt.makePipe("testInOutBoth($vin, ?, $vout)", "father");
		pipeline = pp.pixyStep(pipeline);
		
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertFalse(pipeline.hasNext());

		pt = new PixyTheory(
				"testEquals(V1, V2) :- V1 = V2, !, fail." +
				"testEquals(V1, V2)." +
				"testEquals2(V1, V2) :- V1 == V2, !, fail." +
				"testEquals2(V1, V2)." +
				"testEquals3(V1, V2) :- 2 == 2, 1 = 1, 'foo' = 'foo', !, fail." +
				"testEquals3(V1, V2)." +
				"testEquals4(V1, V2) :- V1 == V1, V2 = V2, !, fail." +
				"testEquals4(V1, V2)." +
				"testEquals5(V1, V2) :- V1 is V1, Z = 'foo', Z is 'foo', 'bar' is 'ba' + 'r', 5 is 2+3, !, fail." +
				"testEquals5(V1, V2)." +
				"testNotEquals(V1, V2) :- V1 \\= V2. " +
				"testEqAndNotEqA(V1, V2) :- testEquals(V1, V2). " +
				"testEqAndNotEqA(V1, V2) :- testEquals2(V1, V2)." +
				"testEqAndNotEqA(V1, V2) :- testEquals3(V1, V2)." +
				"testEqAndNotEqB(V1, V2) :- testEquals4(V1, V2)." +
				"testEqAndNotEqB(V1, V2) :- testEquals5(V1, V2)." +
				"testEqAndNotEqB(V1, V2) :- testNotEquals(V1, V2)." +
				"testEqAndNotEq(V1, V2) :- testEqAndNotEqA(V1, V2)." +
				"testEqAndNotEq(V1, V2) :- testEqAndNotEqB(V1, V2)."
				);
		
		pipeline = new GremlinPipeline(graph).V().as("v");
		pp = pt.makePipe("testEqAndNotEq($, $v)");
		pipeline = pp.pixyStep(pipeline);
		
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		assertFalse(pipeline.hasNext());

		pt = new PixyTheory(
				"function(A, B) :- B is -((-1 + ((A - 2) rem 12) + ((A * 5 + 7) / 2) mod 13) // 5)." +
				"fpFunction(A, B) :- B is -((-1 + ((A - 2) / 12) + ((A * 5 + 7) / 2) * 13) / 5)." +
				"overlap(A, B, C) :- (B =< C, A < C, A >= B) ; (B > C, A < B, A >= C, true). " +
				// Demorgan
				"overlapStr(A, B, C, Astr) :- Astr is ('' + A), \\+ ((B @> C ; Astr @>= C ; Astr @< B ; false) , (B @=< C ; Astr @>= B ; Astr @< C ; fail)). ");
		
		List<Object> numbers = new ArrayList<Object>();
		List<Object> fpNumbers = new ArrayList<Object>();
		List<Integer> intNumbers = new ArrayList<Integer>();
		for (int i=0; i < 100; i++) {
			intNumbers.add(new Integer(i));
			numbers.add(new Integer(i));
			numbers.add(new Long(i));
			numbers.add(new Short((short)i));
			numbers.add(new Byte((byte)i));
			numbers.add(new BigInteger("" + i));
			
			fpNumbers.add(new Float((float)i));
			fpNumbers.add(new Double((double)i));
			fpNumbers.add(new BigDecimal(i));
		}

		// Test int functions
		pipeline = new GremlinPipeline(numbers)._().scatter();
		pp = pt.makePipe("function($, &)");
		pipeline = pp.pixyStep(pipeline);
		
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		for (Integer num : intNumbers) {
			int exp = (-1 + ((num - 2) % 12) + ((num * 5 + 7) / 2) % 13) / 5;
			
			// Check 5 times for each type
			assertEquals(new BigDecimal(-exp), pipeline.next());
			assertEquals(new BigDecimal(-exp), pipeline.next());
			assertEquals(new BigDecimal(-exp), pipeline.next());
			assertEquals(new BigDecimal(-exp), pipeline.next());
			assertEquals(new BigDecimal(-exp), pipeline.next());
		}
		assertFalse(pipeline.hasNext());
		
		// Test FP functions
		pipeline = new GremlinPipeline(fpNumbers)._().scatter();
		pp = pt.makePipe("fpFunction($, &)");
		pipeline = pp.pixyStep(pipeline);
		
		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);
		
		for (Integer numInt : intNumbers) {
			double num = (double) numInt;
			double exp = (-1 + ((num - 2) / 12) + ((num * 5 + 7) / 2) * 13) / 5;
			
			// Check 3 times for each type
			assertTrue(new BigDecimal(-exp).subtract((BigDecimal)pipeline.next()).abs().doubleValue() < 0.0000001);
			assertTrue(new BigDecimal(-exp).subtract((BigDecimal)pipeline.next()).abs().doubleValue() < 0.0000001);
			assertTrue(new BigDecimal(-exp).subtract((BigDecimal)pipeline.next()).abs().doubleValue() < 0.0000001);
		}
		assertFalse(pipeline.hasNext());

		// Overlap tests for boolean expressions
		pipeline = new GremlinPipeline(intNumbers)._().transform(new PipeFunction() {
			@Override
			public Object compute(Object x) {
				return new Long(((Integer)x).intValue());
			}
		}).scatter().as("n");
		pp = pt.makePipe("overlap($, ?, ?)", 10, 50);
		pipeline = GremlinPipelineExt.coalesce(pp.pixyStep(pipeline), "n");

		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		for (int exp = 10; exp < 50; exp++) {
			assertEquals(new Long(exp), pipeline.next());
		}
		assertFalse(pipeline.hasNext());
		
		pipeline = new GremlinPipeline(intNumbers)._().scatter().as("n");
		pp = pt.makePipe("overlap($, ?, ?)", 70, 0);
		pipeline = GremlinPipelineExt.coalesce(pp.pixyStep(pipeline), "n");

		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		for (int exp = 0; exp < 70; exp++) {
			assertEquals(new Integer(exp), pipeline.next());
		}
		assertFalse(pipeline.hasNext());

		pipeline = new GremlinPipeline(intNumbers)._().scatter().as("n");
		pp = pt.makePipe("overlapStr($, ?, ?, _)", 10, 50);
		pipeline = GremlinPipelineExt.coalesce(pp.pixyStep(pipeline), "n");
		for (int exp = 2; exp <= 5; exp++) {
			assertEquals(new Integer(exp), pipeline.next());
		}
		for (int exp = 10; exp < 50; exp++) {
			assertEquals(new Integer(exp), pipeline.next());
		}
		assertFalse(pipeline.hasNext());
		
		pipeline = new GremlinPipeline(intNumbers)._().scatter().as("n");
		pp = pt.makePipe("overlapStr($, ?, ?, _)", 70, 57);
		pipeline = GremlinPipelineExt.coalesce(pp.pixyStep(pipeline), "n");

		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		assertEquals(new Integer(6), pipeline.next());
		assertEquals(new Integer(7), pipeline.next());
		for (int exp = 57; exp < 70; exp++) {
			assertEquals(new Integer(exp), pipeline.next());
		}
		assertFalse(pipeline.hasNext());

		// Test property/3 and property/4
		pt = pt.extend(
				"person(V, Sex, Born, Died, Nickname) :- " +
				"    Sex2 = Sex, property(V, 'sex', Sex2)," + 
				"    property(V, 'born', Born)," + 
				"    property(V, 'died', Died)," + 
				"    property(V, 'nickname', Nickname).");

		pipeline = new GremlinPipeline(graph).V("name", "John Fitzgerald Kennedy, Sr.");
		pp = pt.makePipe("person($, &sex, &born, &died, &nickname)");
		pipeline = pp.pixyStep(pipeline).select(Arrays.asList(new String[] {"sex", "born", "died", "nickname"}));

		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		Row row = (Row)pipeline.next();
		assertEquals("Jack", row.getColumn("nickname"));
		assertEquals(new Integer(1917), row.getColumn("born"));
		assertEquals(new Integer(1963), row.getColumn("died"));
		assertEquals("male", row.getColumn("sex"));

		assertFalse(pipeline.hasNext());
		
		// Try with Carolyn Jeanne Bessette -- no nickname
		pipeline = new GremlinPipeline(graph).V("name", "Carolyn Jeanne Bessette");
		pp = pt.makePipe("person($, &sex, &born, &died, &nickname)");
		pipeline = pp.pixyStep(pipeline).select(Arrays.asList(new String[] {"sex", "born", "died", "nickname"}));

		assertFalse(pipeline.hasNext());
		
		// Redefine property to return '0' for died and '' for nickname
		pt = pt.remove("person/5").extend(
				"person(V, Sex, Born, Died, Nickname) :- " +
				"    property(V, 'sex', Sex)," + 
				"    property(V, 'born', Born)," + 
				"    property(V, 'died', Died, 0)," + 
				"    property(V, 'nickname', Nickname, '').");

		// Try again for Carolyn
		pipeline = new GremlinPipeline(graph).V("name", "Carolyn Jeanne Bessette");
		pp = pt.makePipe("person($, &sex, &born, &died, &nickname)");
		pipeline = pp.pixyStep(pipeline).select(Arrays.asList(new String[] {"sex", "born", "died", "nickname"}));

		row = (Row)pipeline.next();
		assertEquals("", row.getColumn("nickname"));
		assertEquals(new Integer(1966), row.getColumn("born"));
		assertEquals(new Integer(1999), row.getColumn("died"));
		assertEquals("female", row.getColumn("sex"));

		assertFalse(pipeline.hasNext());

		// Try again for Timothy Perry Shriver
		pipeline = new GremlinPipeline(graph).V("name", "Timothy Perry Shriver");
		pp = pt.makePipe("person($, &sex, &born, &died, &nickname)");
		pipeline = pp.pixyStep(pipeline).select(Arrays.asList(new String[] {"sex", "born", "died", "nickname"}));

		row = (Row)pipeline.next();
		assertEquals("", row.getColumn("nickname"));
		assertEquals(new Integer(1959), row.getColumn("born"));
		// TODO: Allow non-BigDecimal but numeric, defaults
		assertEquals(new BigDecimal(0), row.getColumn("died"));
		assertEquals("male", row.getColumn("sex"));

		assertFalse(pipeline.hasNext());		

		// Test label and property
		pt = pt.extend(
				"invalidPerson(Person) :- " +
				"    property(Father, 'sex', 'female')," + 
				"    label(E, 'father'), true, outE(Person, E), inV(E, Father). " + 
				"invalidPerson(Person) :- " +
				"    out(Person, ['mother'], Mother), " + 
				"    'male' is Sex, property(Mother, 'sex', Sex). " +
				"invalidPerson(Person) :- " +
				"    in(Wife, ['wife'], Person), " + 
				"    property(Person, 'sex', 'female'). " +
				"invalidPerson(Person) :- " +
				"    inE(Person, E), " +
				"    outV(E, Husband), " +
				"    label(E, 'wife'), " +
				"    property(Person, 'sex', 'male'). " +
				"invalidPerson(Person) :- " +
				"    Born > Died, " + 
				"    property(Person, 'born', Born), " + 
				"    property(Person, 'died', Died). ");

		pipeline = new GremlinPipeline(graph).V();
		pp = pt.makePipe("invalidPerson($)");
		pipeline = pp.pixyStep(pipeline);

		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		assertFalse(pipeline.hasNext());

		// Test loops
		pt = pt.extend(
				// If there is a way out, there is a way in
				"outInTest(Person) :- " +
				"    outLoop(Person, AnotherPerson, 3), " + 
				"    not(inLoop(AnotherPerson, Person, 3)). " +
				"outInTest(Person) :- " +
				"    inLoop(Person, AnotherPerson, 3)," + 
				"    not(outLoop(AnotherPerson, Person, 4)). " +

				// Compare loop of 2 by hand vs in/outLoop
				"loopByHand(Person, AnotherPerson) :- " +
				"    out(Person, X), out(X, AnotherPerson). " +
				"byHandTest(Person) :- loopByHand(Person, X), not(outLoop(Person, X)). " +
				"byHandTest(Person) :- loopByHand(Person, X), not(bothLoop(Person, X)). " +
				"byHandTest(Person) :- outLoop(Person, X, [2, 2]), not(loopByHand(Person, X)). " +
				"byHandTest(Person) :- outLoop(Person, X, [2, 2]), not(loopByHand(Person, X)). " +
				
				// Check queries w/labels is a subset of w/o labels
				"labelTest(Person) :- inLoop(Person, 'father', Child, 1), not(inLoop(Person, Child)). " +
				"labelTest(Person) :- inLoop(Person, 'father', Child, 1), not(bothLoop(Person, Child)). " +
				"labelTest(Person) :- outLoop(Person, 'father', Ancestor), not(outLoop(Person, Ancestor)). " +
				"labelTest(Person) :- outLoop(Person, 'father', Ancestor), not(bothLoop(Person, Ancestor)). " +
				"labelTest(Person) :- inLoop(Person, 'father', Descendant, [1, 2]), not(inLoop(Person, Descendant)). " +
				
				// Check that bothLoop is a superset of in/outLoop
				"bothByHand(Person, X, Limits) :- inLoop(Person, X, Limits). " +
				"bothByHand(Person, X, Limits) :- outLoop(Person, X, Limits). " +
				"bothTest(Person) :- " +
				"    outLoop(Person, 'father', X), " + 
				"    not(bothLoop(Person, X)). " +
				"bothTest(Person) :- " +
				"    bothByHand(Person, 'father', X), " + 
				"    not(bothLoop(Person, X, [0, 0])). " +
				"bothTest(Person) :- " +
				"    inLoop(Person, 'father', X, [2, 3]), " + 
				"    not(bothLoop(Person, ['father', 'mother'], X)). " +
				"bothTest(Person) :- " +
				"    outLoop(Person, 'father', X, [2, 3]), " + 
				"    not(bothLoop(Person, 'father', X, [2, 3])). " +
				"bothTest(Person) :- " +
				"    bothByHand(Person, X, [1, 3]), " + 
				"    not(bothLoop(Person, X, [1, 3])). " +
				"bothTest(Person) :- " +
				"    bothByHand(Person, X, 3), " + 
				"    not(bothLoop(Person, X, [2, 3])). " +
				
				// All tests in one mega-pipe
				"testLoops(Person) :- byHandTest(Person). " +
				"testLoops(Person) :- outInTest(Person). " +
				"testLoops(Person) :- labelTest(Person). " +
				"testLoops(Person) :- bothTest(Person). "
				);

		pipeline = new GremlinPipeline(graph).V();
		pp = pt.makePipe("testLoops($)");
		pipeline = pp.pixyStep(pipeline);

		System.out.println("Pixy pipe: " + pp);
		System.out.println("Pipeline: " + pipeline);

		assertFalse(pipeline.hasNext());
	}
}
