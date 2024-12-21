package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.impl.ConstantImpl;
import fr.boreal.model.logicalElements.impl.VariableImpl;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.BeforeEach;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.storage.RDFHexaStore;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour la classe {@link RDFHexaStore}.
 */
public class RDFHexaStoreTest {
    private static final Literal<String> SUBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("subject1");
    private static final Literal<String> PREDICATE_1 = SameObjectTermFactory.instance().createOrGetLiteral("predicate1");
    private static final Literal<String> OBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("object1");
    private static final Literal<String> SUBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("subject2");
    private static final Literal<String> PREDICATE_2 = SameObjectTermFactory.instance().createOrGetLiteral("predicate2");
    private static final Literal<String> OBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("object2");
    private static final Literal<String> OBJECT_3 = SameObjectTermFactory.instance().createOrGetLiteral("object3");
    private static final Literal<String> OBJECT_4 = SameObjectTermFactory.instance().createOrGetLiteral("object4");
    private static final Variable VAR_X = SameObjectTermFactory.instance().createOrGetVariable("?x");
    private static final Variable VAR_Y = SameObjectTermFactory.instance().createOrGetVariable("?y");


    @Test
    public void testAddAllRDFAtoms() {
        RDFHexaStore store = new RDFHexaStore();

        // Version stream
        // Ajouter plusieurs RDFAtom
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        Set<RDFAtom> rdfAtoms = Set.of(rdfAtom1, rdfAtom2);

        assertTrue(store.addAll(rdfAtoms.stream()), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        Collection<Atom> atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");

        // Version collection
        store = new RDFHexaStore();
        assertTrue(store.addAll(rdfAtoms), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");
    }


    @Test
    public void testAddRDFAtom() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);

        assertTrue(store.add(rdfAtom), "Le RDFAtom devrait être ajouté avec succès.");
        assertTrue(store.getAtoms().contains(rdfAtom), "Le RDFAtom devrait être présent dans la base.");
    }

    @Test
    public void testAddDuplicateAtom() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);

        store.add(rdfAtom);
        assertFalse(store.add(rdfAtom), "Le RDFAtom ne devrait pas être ajouté à nouveau.");
    }

    @Test
    public void testSize() {
        RDFHexaStore store = new RDFHexaStore();
        assertEquals(0, store.size(), "La taille initiale devrait être 0.");

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        assertEquals(1, store.size(), "La taille devrait être 1 après un ajout.");
    }

    @Test
    public void testMatchAtom() {
        RDFHexaStore store = new RDFHexaStore();
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1)); // RDFAtom(subject1, triple, object1)
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2)); // RDFAtom(subject2, triple, object2)
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3)); // RDFAtom(subject1, triple, object3)

        // Case 1
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, VAR_X); // RDFAtom(subject1, predicate1, X)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, OBJECT_1);
        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, OBJECT_3);

        assertEquals(2, matchedList.size(), "There should be two matched RDFAtoms");
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);

        // Other cases
        //throw new NotImplementedException("This test must be completed");
    }


    @Test
    void testMatch_StarQuery() {
        RDFHexaStore rdfHexaStore = new RDFHexaStore();
        // Ajouter 20 RDFAtoms au RDFHexaStore
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Alice"), new ConstantImpl("knows"), new ConstantImpl("Bob")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Alice"), new ConstantImpl("likes"), new ConstantImpl("Pizza")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Bob"), new ConstantImpl("works_for"), new ConstantImpl("Nasa")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Alice"), new ConstantImpl("works_for"), new ConstantImpl("Domino")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Alice"), new ConstantImpl("knows"), new ConstantImpl("Eve")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Charlie"), new ConstantImpl("works_for"), new ConstantImpl("Microsoft")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Ruby"), new ConstantImpl("knows"), new ConstantImpl("Bob")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Bob"), new ConstantImpl("works_for"), new ConstantImpl("Tesla")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Alice"), new ConstantImpl("likes"), new ConstantImpl("Sushi")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Charlie"), new ConstantImpl("friends_with"), new ConstantImpl("Alice")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Bob"), new ConstantImpl("likes"), new ConstantImpl("Panini")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Charlie"), new ConstantImpl("knows"), new ConstantImpl("Alice")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Thomas"), new ConstantImpl("works_for"), new ConstantImpl("Google")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Alice"), new ConstantImpl("friends_with"), new ConstantImpl("Eve")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Ruby"), new ConstantImpl("likes"), new ConstantImpl("Chocolate")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Thomas"), new ConstantImpl("knows"), new ConstantImpl("Charlie")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Ruby"), new ConstantImpl("works_for"), new ConstantImpl("Facebook")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Alice"), new ConstantImpl("likes"), new ConstantImpl("Cake")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Bob"), new ConstantImpl("friends_with"), new ConstantImpl("Eve")));
        rdfHexaStore.add(new RDFAtom(new ConstantImpl("Thomas"), new ConstantImpl("likes"), new ConstantImpl("Pasta")));

        // Définir 8 requêtes de type StarQuery

        //Requete 01
        List<RDFAtom> rdfAtoms1 = Arrays.asList(
                new RDFAtom(new VariableImpl("?x"), new ConstantImpl("knows"), new ConstantImpl("Bob"))
        );
        StarQuery query1 = new StarQuery("Query1", rdfAtoms1, Set.of(new VariableImpl("?x")));
        //Requete 02
        List<RDFAtom> rdfAtoms2 = Arrays.asList(
                new RDFAtom(new VariableImpl("?x"), new ConstantImpl("likes"), new ConstantImpl("Pizza")),
                new RDFAtom(new VariableImpl("?x"), new VariableImpl("?y"), new ConstantImpl("Facebook"))
        );
        StarQuery query2 = new StarQuery("Query2", rdfAtoms2, Set.of(new VariableImpl("?x"), new VariableImpl("?y")));
        //Requete 03
        List<RDFAtom> rdfAtoms3 = Arrays.asList(
                new RDFAtom(new VariableImpl("?x"), new ConstantImpl("works_for"), new ConstantImpl("Tesla"))
        );
        StarQuery query3 = new StarQuery("Query3", rdfAtoms3, Set.of(new VariableImpl("?x")));

        //Requete 04
        List<RDFAtom> rdfAtoms4 = Arrays.asList(
                new RDFAtom(new ConstantImpl("Eve"), new ConstantImpl("likes"), new VariableImpl("?z")),
                new RDFAtom(new ConstantImpl("Charlie"), new VariableImpl("?y"), new VariableImpl("?z"))
        );
        StarQuery query4 = new StarQuery("Query4", rdfAtoms4, Set.of(new VariableImpl("?z")));
        //Requete 05
        List<RDFAtom> rdfAtoms5 = Arrays.asList(
                new RDFAtom(new VariableImpl("?x"), new ConstantImpl("likes"), new VariableImpl("?y")),
                new RDFAtom(new ConstantImpl("Charlie"), new VariableImpl("?z"), new VariableImpl("?x"))
        );
        StarQuery query5 = new StarQuery("Query5", rdfAtoms5, Set.of(new VariableImpl("?x"),new VariableImpl("?y"),new VariableImpl("?z")));

        List<RDFAtom> rdfAtoms6 = Arrays.asList(
                new RDFAtom(new VariableImpl("?x"), new ConstantImpl("knows"), new ConstantImpl("Bob")),
                new RDFAtom(new VariableImpl("?x"), new ConstantImpl("likes"), new ConstantImpl("Pizza")),
                new RDFAtom(new VariableImpl("?x"), new ConstantImpl("works_for"), new ConstantImpl("Domino"))
        );
        StarQuery query6 = new StarQuery("Query5", rdfAtoms6, Set.of(new VariableImpl("?x")));
        //requete 06



        // Lancer les requêtes et vérifier les résultats
        //requete 01
        Iterator<Substitution> iterateur;
        iterateur = rdfHexaStore.match(query1);
        assertEquals("{?x:Alice}",iterateur.next().toString());
        assertEquals("{?x:Ruby}",iterateur.next().toString());
        assertFalse(iterateur.hasNext());

        //requetes 02
        iterateur= rdfHexaStore.match(query2);
        assertFalse(iterateur.hasNext());
        // Assert pour requête 3
        iterateur = rdfHexaStore.match(query3);
        assertEquals("{?x:Bob}", iterateur.next().toString());
        assertFalse(iterateur.hasNext());
        //requete 04
        iterateur= rdfHexaStore.match(query4);
        assertFalse(iterateur.hasNext());

        //requete 05
        iterateur= rdfHexaStore.match(query5);
        //assertEquals("{?x:Alice, ?y:Pizza, ?z:knows}",iterateur.next().toString());
        //assertEquals("{?x:Alice, ?y:Pizza, ?z:friends_with}",iterateur.next().toString());
        //assertFalse(iterateur.hasNext());

        //requete 06
        iterateur = rdfHexaStore.match(query6);
        assertEquals("{?x:Alice}",iterateur.next().toString());
        assertFalse(iterateur.hasNext());
    }


}
