package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectPredicateFactory;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.factory.api.TermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.Spliterator;
import java.util.Spliterators;


/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */

public class RDFHexaStore implements RDFStorage {

    // Dictionnaire pour l'encodage/décodage
    private final Dictionary dictionary;

    // Six index pour les triplets RDF
    private final Map<Integer, Map<Integer, Set<Integer>>> spoIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> sopIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> psoIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> posIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> ospIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> opsIndex = new HashMap<>();

    public RDFHexaStore() {
        this.dictionary = new Dictionary(); // Initialise le dictionnaire RDF
    }


    @Override
    public boolean add(RDFAtom atom) {
        int s = dictionary.encode(atom.getTripleSubject().toString());
        int p = dictionary.encode(atom.getPredicate().toString());
        int o = dictionary.encode(atom.getTripleObject().toString());

        // Ajouter le triplet dans les six index
        addToIndex(spoIndex, s, p, o);
        addToIndex(sopIndex, s, o, p);
        addToIndex(psoIndex, p, s, o);
        addToIndex(posIndex, p, o, s);
        addToIndex(ospIndex, o, s, p);
        addToIndex(opsIndex, o, p, s);

        return true; // Retourne true si l'ajout a réussi
    }

    private void addToIndex(Map<Integer, Map<Integer, Set<Integer>>> index, int a, int b, int c) {
        index.computeIfAbsent(a, k -> new HashMap<>())
                .computeIfAbsent(b, k -> new HashSet<>())
                .add(c);
    }

    @Override
    public long size() {
        // Le nombre de triplets correspond au nombre d'éléments dans SPO
        return spoIndex.values().stream()
                .mapToLong(map -> map.values().stream()
                        .mapToLong(Set::size).sum())
                .sum();
    }

    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        Integer s = atom.getTripleSubject() != null ? dictionary.encode(atom.getTripleSubject().toString()) : null;
        Integer p = atom.getPredicate() != null ? dictionary.encode(atom.getPredicate().toString()) : null;
        Integer o = atom.getTripleObject() != null ? dictionary.encode(atom.getTripleObject().toString()) : null;

        Stream<Substitution> results;

        if (s != null && p != null && o != null) {
            results = matchExact(spoIndex, s, p, o);
        } else if (s != null && p != null) {
            results = matchPartial(spoIndex, s, p);
        } else if (s != null && o != null) {
            results = matchPartial(sopIndex, s, o);
        } else if (p != null && o != null) {
            results = matchPartial(posIndex, p, o);
        } else if (s != null) {
            results = matchSingle(spoIndex, s);
        } else if (p != null) {
            results = matchSingle(psoIndex, p);
        } else if (o != null) {
            results = matchSingle(ospIndex, o);
        } else {
            results = Stream.empty();
        }

        return results.iterator();
    }

    private Stream<Substitution> matchExact(
            Map<Integer, Map<Integer, Set<Integer>>> index, int a, int b, int c) {
        return index.getOrDefault(a, Collections.emptyMap())
                .getOrDefault(b, Collections.emptySet())
                .contains(c)
                ? Stream.of(new SubstitutionImpl())
                : Stream.empty();
    }

    private Stream<Substitution> matchPartial(
            Map<Integer, Map<Integer, Set<Integer>>> index, int a, int b) {
        return index.getOrDefault(a, Collections.emptyMap())
                .getOrDefault(b, Collections.emptySet())
                .stream()
                .map(value -> new SubstitutionImpl());
    }

    private Stream<Substitution> matchSingle(
            Map<Integer, Map<Integer, Set<Integer>>> index, int a) {
        return index.getOrDefault(a, Collections.emptyMap())
                .values().stream()
                .flatMap(Set::stream)
                .map(value -> new SubstitutionImpl());
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        List<RDFAtom> atoms = q.getRdfAtoms();

        // Évaluer les résultats pour chaque triplet
        List<Iterator<Substitution>> results = atoms.stream()
                .map(this::match)
                .toList();

        // Fusionner les résultats
        return results.stream().flatMap(it -> StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false))
                .iterator();
    }

    @Override
    public Collection<Atom> getAtoms() {
        List<Atom> atoms = new ArrayList<>();

        spoIndex.forEach((sID, pMap) ->
                pMap.forEach((pID, oSet) ->
                        oSet.forEach(oID -> {
                            // Utilisation de SameObjectPredicateFactory pour créer les Term
                            Term s = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(sID));
                            Term p = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(pID));
                            Term o = SameObjectTermFactory.instance().createOrGetLiteral(dictionary.decode(oID));

                            // Ajouter le RDFAtom à la liste
                            atoms.add(new RDFAtom(s, p, o));
                        })
                )
        );
        return atoms;
    }

}
