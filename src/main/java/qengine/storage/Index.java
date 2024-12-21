package qengine.storage;

import java.util.*;

public class Index {

    // Six indices pour permettre différentes combinaisons de recherche
    private final Map<Integer, Map<Integer, Set<Integer>>> spo = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> sop = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> pso = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> pos = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> osp = new HashMap<>();
    private final Map<Integer, Map<Integer, Set<Integer>>> ops = new HashMap<>();

    // Ajoute un triplet à tous les indices
    public void addTriple(int subject, int predicate, int object) {
        addToIndex(spo, subject, predicate, object);
        addToIndex(sop, subject, object, predicate);
        addToIndex(pso, predicate, subject, object);
        addToIndex(pos, predicate, object, subject);
        addToIndex(osp, object, subject, predicate);
        addToIndex(ops, object, predicate, subject);
    }

    // Méthode utilitaire pour ajouter des valeurs dans un index
    private void addToIndex(Map<Integer, Map<Integer, Set<Integer>>> index, int first, int second, int third) {
        index.computeIfAbsent(first, k -> new HashMap<>())
                .computeIfAbsent(second, k -> new HashSet<>())
                .add(third);
    }

    // Recherche des triplets correspondant aux critères donnés
    public List<int[]> findMatches(int subject, int predicate, int object) {
        List<int[]> results = new ArrayList<>();

        // Cas 1 : Tous les paramètres sont spécifiés
        if (subject != -1 && predicate != -1 && object != -1) {
            // Recherche directe dans sp_o
            Map<Integer, Set<Integer>> secondMap = spo.get(subject);
            if (secondMap != null) {
                Set<Integer> thirdSet = secondMap.get(predicate);
                if (thirdSet != null && thirdSet.contains(object)) {
                    results.add(new int[]{subject, predicate, object});
                }
            }

            // Cas 2 : Sujet et prédicat spécifiés
        } else if (subject != -1 && predicate != -1) {
            // Recherche dans sp_o
            Map<Integer, Set<Integer>> secondMap = spo.get(subject);
            if (secondMap != null) {
                Set<Integer> thirdSet = secondMap.get(predicate);
                if (thirdSet != null) {
                    for (int obj : thirdSet) {
                        results.add(new int[]{subject, predicate, obj});
                    }
                }
            }

            // Cas 3 : Prédicat et objet spécifiés
        } else if (predicate != -1 && object != -1) {
            // Recherche dans po_s
            Map<Integer, Set<Integer>> secondMap = pos.get(predicate);
            if (secondMap != null) {
                Set<Integer> thirdSet = secondMap.get(object);
                if (thirdSet != null) {
                    for (int subj : thirdSet) {
                        results.add(new int[]{subj, predicate, object});
                    }
                }
            }

            // Cas 4 : Sujet et objet spécifiés
        } else if (subject != -1 && object != -1) {
            // Recherche dans so_p
            Map<Integer, Set<Integer>> secondMap = sop.get(subject);
            if (secondMap != null) {
                Set<Integer> thirdSet = secondMap.get(object);
                if (thirdSet != null) {
                    for (int pred : thirdSet) {
                        results.add(new int[]{subject, pred, object});
                    }
                }
            }



            // Cas 5 : Prédicat spécifié uniquement
        } else if (predicate != -1) {
            // Recherche dans ps_o
            Map<Integer, Set<Integer>> secondMap = pso.get(predicate);
            if (secondMap != null) {
                for (Map.Entry<Integer, Set<Integer>> entry : secondMap.entrySet()) {
                    int subjectValue = entry.getKey();
                    for (int objectValue : entry.getValue()) {
                        results.add(new int[]{subjectValue, predicate, objectValue});
                    }
                }
            }

            // Cas 6 : Objet spécifié uniquement
        } else if (object != -1) {
            // Recherche dans os_p
            Map<Integer, Set<Integer>> secondMap = osp.get(object);
            if (secondMap != null) {
                for (Map.Entry<Integer, Set<Integer>> entry : secondMap.entrySet()) {
                    int subjectValue = entry.getKey();
                    for (int predicateValue : entry.getValue()) {
                        results.add(new int[]{subjectValue, predicateValue, object});
                    }
                }
            }

            // Cas 7 : Aucun paramètre spécifié
        } else {
            // Parcours complet de sp_o (ou de tout autre index)
            for (Map.Entry<Integer, Map<Integer, Set<Integer>>> entry1 : spo.entrySet()) {
                int subjectValue = entry1.getKey();
                for (Map.Entry<Integer, Set<Integer>> entry2 : entry1.getValue().entrySet()) {
                    int predicateValue = entry2.getKey();
                    for (int objectValue : entry2.getValue()) {
                        results.add(new int[]{subjectValue, predicateValue, objectValue});
                    }
                }
            }
        }

        return results;
    }

    public List<int[]> getAllTriples() {
        List<int[]> allTriples = new ArrayList<>();
        // Traverse the sp_o index (or any other index) to gather all triples
        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> entry1 : spo.entrySet()) {
            int subjectValue = entry1.getKey();
            for (Map.Entry<Integer, Set<Integer>> entry2 : entry1.getValue().entrySet()) {
                int predicateValue = entry2.getKey();
                for (int objectValue : entry2.getValue()) {
                    allTriples.add(new int[]{subjectValue, predicateValue, objectValue});
                }
            }
        }
        return allTriples;
    }

}
