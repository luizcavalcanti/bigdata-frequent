package br.edu.ufam.icomp.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import br.edu.ufam.icomp.model.HashTreeNode;
import br.edu.ufam.icomp.model.Registro;

/**
 * Utility class that builds a hash tree for fast searching for itemset patterns
 * in a transaction.
 */
public class HashTreeUtils {
    /*
     * Builds hashtree from the candidate itemsets.
     */
    public static HashTreeNode buildHashTree(List<Registro> candidateItemsets, int itemsetSize) {
        HashTreeNode hashTreeRoot = new HashTreeNode();

        HashTreeNode parentNode = null;
        HashTreeNode currNode = null;
        for (Registro currItemset : candidateItemsets) {
            parentNode = null;
            currNode = hashTreeRoot;
            for (int i = 0; i < itemsetSize; i++) {
                String item = currItemset.getItens().get(i);
                Map<String, HashTreeNode> mapAtNode = currNode.getMapAtNode();
                parentNode = currNode;

                if (mapAtNode.containsKey(item)) {
                    currNode = mapAtNode.get(item);
                } else {
                    currNode = new HashTreeNode();
                    mapAtNode.put(item, currNode);
                }

                parentNode.setMapAtNode(mapAtNode);
            }

            currNode.setLeafNode(true);
            List<Registro> itemsets = currNode.getItemsets();
            itemsets.add(currItemset);
            currNode.setItemsets(itemsets);
        }

        return hashTreeRoot;
    }

    /*
     * Returns the set of itemsets in a transaction from the set of candidate
     * itemsets. Used hash tree data structure for fast generation of matching
     * itemsets.
     */
    public static List<Registro> findItemsets(HashTreeNode hashTreeRoot, Registro reg, int startIndex) {
        if (hashTreeRoot.isLeafNode()) {
            return hashTreeRoot.getItemsets();
        }

        List<Registro> matchedItemsets = new ArrayList<Registro>();
        for (int i = startIndex; i < reg.getItens().size(); i++) {
            String item = reg.getItens().get(i);
            Map<String, HashTreeNode> mapAtNode = hashTreeRoot.getMapAtNode();

            if (!mapAtNode.containsKey(item)) {
                continue;
            }
            List<Registro> itemset = findItemsets(mapAtNode.get(item), reg, i + 1);
            matchedItemsets.addAll(itemset);
        }

        return matchedItemsets;
    }

    /*
     * Prints the hashtree for debugging purposes.
     */
    public static void printHashTree(HashTreeNode hashTreeRoot) {
        if (hashTreeRoot == null) {
            System.out.println("Hash Tree Empty !!");
            return;
        }

        System.out.println("Node " + hashTreeRoot.toString());
        Map<String, HashTreeNode> mapAtNode = hashTreeRoot.getMapAtNode();
        for (Map.Entry<String, HashTreeNode> entry : mapAtNode.entrySet()) {
            printHashTree(entry.getValue());
        }

    }
}