package br.edu.ufam.icomp.apriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ufam.icomp.model.HashTreeNode;
import br.edu.ufam.icomp.model.Registro;
import br.edu.ufam.icomp.util.HashTreeUtils;

/**
 * Mapper para os passos posteriores ao primeiro no algoritmo Apriori. Se
 * utiliza da saída do passo anterior para formar novos candidatos e depois
 * fazer a contagem de ocorrências.
 */
public class AprioriMapPassoK extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text item = new Text();

    private List<Registro> candidatosPassoAnterior = new ArrayList<Registro>();
    private List<Registro> candidatosAtuais;
    private HashTreeNode raizArvoreHash;

    @Override
    public void setup(Context context) throws IOException {
        // Path[] uris =
        // DistributedCache.getLocalCacheFiles(context.getConfiguration());

        int passo = context.getConfiguration().getInt("passo", 2);
        String outputPath = "/user/luiz/mushroom/output" + (passo - 1) + "/part-r-00000";
        String opFileLastPass = context.getConfiguration().get("fs.default.name") + outputPath;
        System.out.println("Distributed cache file to search " + opFileLastPass);

        try {
            Path pt = new Path(opFileLastPass);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String linha = null;
            while ((linha = fis.readLine()) != null) {
                linha = linha.trim();
                String[] words = linha.split("[\\s\\t]+");
                if (words.length < 2) {
                    continue;
                }

                List<String> items = new ArrayList<String>();
                for (int k = 0; k < words.length - 1; k++) {
                    String csvItemIds = words[k];
                    String[] itemIds = csvItemIds.split(",");
                    for (String item : itemIds) {
                        items.add(item);
                    }
                }
                String finalWord = words[words.length - 1];
                int contador = Integer.parseInt(finalWord);
                // System.out.println(items + " --> " + supportCount);
                Registro reg = new Registro();
                reg.setItems(items);
                reg.setContador(contador);
                candidatosPassoAnterior.add(reg);
            }
        } catch (Exception e) {
        }
        candidatosAtuais = elegerCandidatos(candidatosPassoAnterior, (passo - 1));
        raizArvoreHash = HashTreeUtils.buildHashTree(candidatosAtuais, passo);
        // This would be changed later
    }

    public void map(Object key, Text registro, Context context) throws IOException, InterruptedException {
        Registro reg = Registro.criar(registro.toString());
        List<Registro> candidatos = HashTreeUtils.findItemsets(raizArvoreHash, reg, 0);
        item.set(reg.getItens().toString());
        for (Registro itemset : candidatos) {
            item.set(itemset.getItens().toString());
            context.write(item, one);
        }
    }

    private static List<Registro> elegerCandidatos(List<Registro> candidatosPassoAnterior, int itemSetSize) {
        List<Registro> candidateItemsets = new ArrayList<Registro>();
        List<String> newItems = null;
        Map<Integer, List<Registro>> mapaDeHash = gerarMapaDeHash(candidatosPassoAnterior);
        Collections.sort(candidatosPassoAnterior);

        for (int i = 0; i < (candidatosPassoAnterior.size() - 1); i++) {
            for (int j = i + 1; j < candidatosPassoAnterior.size(); j++) {
                List<String> outerItems = candidatosPassoAnterior.get(i).getItens();
                List<String> innerItems = candidatosPassoAnterior.get(j).getItens();

                if ((itemSetSize - 1) > 0) {
                    boolean isMatch = true;
                    for (int k = 0; k < (itemSetSize - 1); k++) {
                        if (!outerItems.get(k).equals(innerItems.get(k))) {
                            isMatch = false;
                            break;
                        }
                    }

                    if (isMatch) {
                        newItems = new ArrayList<String>();
                        newItems.addAll(outerItems);
                        newItems.add(innerItems.get(itemSetSize - 1));

                        Registro newItemSet = new Registro(newItems, 0);
                        if (prune(mapaDeHash, newItemSet)) {
                            candidateItemsets.add(newItemSet);
                        }
                    }
                } else {
                    // TODO entender esse trecho
                    // if (outerItems.get(0) < innerItems.get(0)) {
                    newItems = new ArrayList<String>();
                    newItems.add(outerItems.get(0));
                    newItems.add(innerItems.get(0));

                    Registro newItemSet = new Registro(newItems, 0);

                    candidateItemsets.add(newItemSet);
                    // }
                }
            }
        }
        return candidateItemsets;
    }

    public static Map<Integer, List<Registro>> gerarMapaDeHash(List<Registro> registros) {
        Map<Integer, List<Registro>> largeItemsetMap = new HashMap<Integer, List<Registro>>();

        List<Registro> itemsets = null;
        for (Registro reg : registros) {
            int hashCode = reg.hashCode();
            if (largeItemsetMap.containsKey(hashCode)) {
                itemsets = largeItemsetMap.get(hashCode);
            } else {
                itemsets = new ArrayList<Registro>();
            }

            itemsets.add(reg);
            largeItemsetMap.put(hashCode, itemsets);
        }

        return largeItemsetMap;
    }

    private static boolean prune(Map<Integer, List<Registro>> largeItemsetsMap, Registro newItemset) {
        List<Registro> subsets = gerarSubconjuntos(newItemset);
        for (Registro r : subsets) {
            boolean contains = false;
            int hashCodeToSearch = r.hashCode();
            if (largeItemsetsMap.containsKey(hashCodeToSearch)) {
                List<Registro> candidateItemsets = largeItemsetsMap.get(hashCodeToSearch);
                for (Registro itemset : candidateItemsets) {
                    if (itemset.equals(r)) {
                        contains = true;
                        break;
                    }
                }
            }

            if (!contains)
                return false;
        }

        return true;
    }

    /**
     * Gera todos os subconjuntos possiveis do passo anterior (k-1), preservando
     * a ordem
     * 
     * @param reg
     *            Conjunto atual
     * @return Lista de subconjuntos
     */
    private static List<Registro> gerarSubconjuntos(Registro reg) {
        List<Registro> subsets = new ArrayList<Registro>();
        List<String> itens = reg.getItens();
        for (int i = 0; i < itens.size(); i++) {
            List<String> currItems = new ArrayList<String>(itens);
            // remove o item da vez
            currItems.remove(itens.size() - 1 - i);
            // cria conjunto com todos os itens, menos o que acabou de ser
            // removido
            subsets.add(new Registro(currItems, 0));
        }
        return subsets;
    }
}