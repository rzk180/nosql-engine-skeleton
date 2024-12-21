package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    private final Dictionary dictionary; // Dictionnaire pour encoder/décoder les termes RDF
    private final Index index; // Index HexaStore pour stocker les triplets
    private long size = 0; // Nombre de triplets stockés

    /**
     * Constructeur initialisant le dictionnaire et l'index.
     */
    public RDFHexaStore() {
        this.dictionary = new Dictionary(); // Initialise le dictionnaire
        this.index = new Index(); // Initialise l'index
    }

    /**
     * Ajoute un RDFAtom dans le store en l'encodant et le stockant dans les index.
     */
    @Override
    public boolean add(RDFAtom atom) {
        // Encode les termes du triplet RDF (sujet, prédicat, objet)
        int subjectId = dictionary.encode(atom.getTripleSubject().toString());
        int predicateId = dictionary.encode(atom.getTriplePredicate().toString());
        int objectId = dictionary.encode(atom.getTripleObject().toString());

        // Vérifie si le triplet est déjà présent
        List<int[]> matches = index.findMatches(subjectId, predicateId, objectId);
        if (!matches.isEmpty()) {
            return false; // Retourne false si le triplet existe déjà
        }


        // Ajoute le triplet encodé dans les six index
        index.addTriple(subjectId, predicateId, objectId);

        size++; // Incrémente le compteur de triplets
        return true; // Retourne true après ajout
    }

    /**
     * Retourne le nombre total de triplets stockés dans l'HexaStore.
     */
    @Override
    public long size() {
        return size; // Retourne la taille actuelle
    }

    /**
     * Recherche les correspondances pour un RDFAtom donné.
     */
    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        // Récupère les termes du triplet
        Term subjectTerm = atom.getTripleSubject();
        Term predicateTerm = atom.getTriplePredicate();
        Term objectTerm = atom.getTripleObject();

        // Encode les termes en entiers, ou -1 pour les variables
        int subjectId = (subjectTerm instanceof Variable) ? -1 : dictionary.encode(subjectTerm.toString());
        int predicateId = (predicateTerm instanceof Variable) ? -1 : dictionary.encode(predicateTerm.toString());
        int objectId = (objectTerm instanceof Variable) ? -1 : dictionary.encode(objectTerm.toString());

        // Trouve les triplets correspondants dans les index
        List<int[]> matches = index.findMatches(subjectId, predicateId, objectId);

        // Liste pour stocker les substitutions finales
        List<Substitution> results = new ArrayList<>();

        // Parcourt les triplets correspondants
        for (int[] triple : matches) {
            Map<Variable, Term> substitutionMap = new HashMap<>();

            // Ajoute les substitutions pour les variables dans le triplet
            if (subjectTerm instanceof Variable) {
                Term subject = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(triple[0]));
                substitutionMap.put((Variable) subjectTerm, subject);
            }
            if (predicateTerm instanceof Variable) {
                Term predicate = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(triple[1]));
                substitutionMap.put((Variable) predicateTerm, predicate);
            }
            if (objectTerm instanceof Variable) {
                Term object = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(triple[2]));
                substitutionMap.put((Variable) objectTerm, object);
            }

            // Crée une substitution et l'ajoute aux résultats
            Substitution substitution = new SubstitutionImpl(substitutionMap);
            results.add(substitution);
        }
        return results.iterator(); // Retourne un itérateur sur les substitutions
    }


    
    @Override
    public Collection<Atom> getAtoms() {
        // Liste pour stocker les atomes RDF décodés
        List<Atom> atoms = new ArrayList<>();

        // Récupérer tous les triplets encodés depuis l'index
        List<int[]> allTriples = index.getAllTriples();

        // Décoder chaque triplet pour recréer les RDFAtom
        for (int[] triple : allTriples) {
            // Décodage des termes RDF
            Term subject = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(triple[0]));
            Term predicate = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(triple[1]));
            Term object = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(triple[2]));

            // Créer un RDFAtom à partir des termes décodés
            Atom atom = new RDFAtom(subject, predicate, object);

            // Ajouter l'atome à la liste
            atoms.add(atom);
        }

        // Retourner la collection d'atomes
        return atoms;
    }

    @Override
    public Iterator<Substitution> match(StarQuery query) {
        if (query.getRdfAtoms().isEmpty()) {
            return Collections.emptyIterator();
        }

        List<RDFAtom> atoms = query.getRdfAtoms();
        List<Substitution> combinedResults = new ArrayList<>();

        // Traiter chaque atome de la requête
        for (RDFAtom atom : atoms) {
            System.out.println("Matching atom: " + atom);
            Iterator<Substitution> atomMatches = match(atom);

            if (!atomMatches.hasNext()) {
                // Si aucun résultat pour cet atome, retourner un itérateur vide
                System.out.println("No matches for atom: " + atom);
                return Collections.emptyIterator();
            }

            // Initialiser les résultats combinés avec les premiers atomes
            if (combinedResults.isEmpty()) {
                atomMatches.forEachRemaining(combinedResults::add);
                System.out.println("Initial matches: " + combinedResults);
                continue;
            }

            List<Substitution> newResults = new ArrayList<>();
            while (atomMatches.hasNext()) {
                Substitution currentSubstitution = atomMatches.next();

                // Fusionner les substitutions avec celles existantes
                for (Substitution existingSubstitution : combinedResults) {
                    // Fusionner seulement les substitutions compatibles
                    Optional<Substitution> merged = currentSubstitution.merged(existingSubstitution);

                    // Ajouter la substitution fusionnée si elle est valide
                    merged.ifPresent(newResults::add);
                }
            }

            // Remplacer les résultats combinés avec les nouveaux résultats
            combinedResults = newResults;
            System.out.println("Combined results: " + combinedResults);

            // Si les résultats sont vides après la fusion, on peut arrêter
            if (combinedResults.isEmpty()) {
                break;
            }
        }

        System.out.println("Final results: " + combinedResults);
        return combinedResults.iterator();
    }


}
