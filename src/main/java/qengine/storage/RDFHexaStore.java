package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
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

    // Dictionnaire pour l'encodage/décodage
    private final Dictionary dictionary;

    // Six index pour les triplets RDF
    private final Map<Integer, Map<Integer, Set<Integer>>> spoIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> sopIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> psoIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> posIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> ospIndex = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> opsIndex = new HashMap<>();

    public RDFHexaStore(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public boolean add(RDFAtom atom) {
        int s = dictionary.encode(atom.getSubject());
        int p = dictionary.encode(atom.getPredicate());
        int o = dictionary.encode(atom.getObject());

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
        Integer s = atom.getSubject() != null ? dictionary.encode(atom.getSubject()) : null;
        Integer p = atom.getPredicate() != null ? dictionary.encode(atom.getPredicate()) : null;
        Integer o = atom.getObject() != null ? dictionary.encode(atom.getObject()) : null;

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
        List<RDFAtom> atoms = q.getAtoms();

        // Évaluer les résultats pour chaque triplet
        List<Iterator<Substitution>> results = atoms.stream()
                .map(this::match)
                .collect(Collectors.toList());

        // Fusionner les résultats
        return results.stream().flatMap(it -> StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false))
                .iterator();
    }

    @Override
    public Collection<Atom> getAtoms() {
        List<Atom> atoms = new ArrayList<>();

        spoIndex.forEach((s, pMap) ->
                pMap.forEach((p, oSet) ->
                        oSet.forEach(o ->
                                atoms.add(new RDFAtom(dictionary.decode(s),
                                        dictionary.decode(p), dictionary.decode(o))))));
        return atoms;
    }
}
