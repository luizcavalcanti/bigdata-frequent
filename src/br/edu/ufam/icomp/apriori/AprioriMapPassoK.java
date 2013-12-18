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

import br.edu.ufam.icomp.model.NodoArvoreHash;
import br.edu.ufam.icomp.model.Registro;

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
    private NodoArvoreHash raizArvoreHash;

    @Override
    public void setup(Context context) throws IOException {
        // Path[] uris =
        // DistributedCache.getLocalCacheFiles(context.getConfiguration());

        // Encontra caminho para arquivo de saída do passo anterior
        int passo = context.getConfiguration().getInt("passo", 2);
        // TODO passar caminho por parametro de config
        String arquivoDeSaida = "/user/luiz/mushroom/output" + (passo - 1) + "/part-r-00000";
        String caminhoCompleto = context.getConfiguration().get("fs.default.name") + arquivoDeSaida;

        extrairDadosPassoAnterior(context, caminhoCompleto);
        // Carrega lista de novos candidatos (tamanho k)
        candidatosAtuais = elegerCandidatos(candidatosPassoAnterior, (passo - 1));
        // Adiciona
        raizArvoreHash = constroiArvoreHash(candidatosAtuais, passo);
    }

    /**
     * Lê arquivo da execução anterior e carrega a lista de candidatos
     * (anteriores) com os dados desse mesmo arquivo
     */
    private void extrairDadosPassoAnterior(Context contexto, String caminhoCompleto) {
        try {
            // Carrega arquivo e lê linha a linha
            Path pt = new Path(caminhoCompleto);
            FileSystem fs = FileSystem.get(contexto.getConfiguration());
            BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String linha = null;
            // Para cada linha (registro)...
            while ((linha = fis.readLine()) != null) {
                linha = linha.trim();
                // Separa os itens da contagem
                String[] words = linha.split("[\\s\\t]+");
                if (words.length < 2) {
                    continue;
                }
                // Cria uma lista a partir dos itens encontrados
                List<String> items = new ArrayList<String>();
                for (int k = 0; k < words.length - 1; k++) {
                    String csvItemIds = words[k];
                    String[] itemIds = csvItemIds.split(",");
                    for (String item : itemIds) {
                        items.add(item);
                    }
                }
                // Recupera a contagem do registro
                String finalWord = words[words.length - 1];
                int contador = Integer.parseInt(finalWord);

                // Cria registro com as informações recuperadas
                Registro reg = new Registro();
                reg.setItems(items);
                reg.setContador(contador);
                candidatosPassoAnterior.add(reg);
            }
        } catch (Exception e) {
        }
    }

    public void map(Object key, Text registro, Context context) throws IOException, InterruptedException {
        Registro reg = Registro.criar(registro.toString());
        // Procura na árvore de hash pelo candidato
        List<Registro> candidatos = buscaRegistroArvoreHash(raizArvoreHash, reg, 0);
        item.set(reg.getItens().toString());
        // Para cada candidato encontrado..
        for (Registro itemset : candidatos) {
            // Adiciona <lista-de-itens, 1> na saída do Map
            item.set(itemset.getItens().toString());
            context.write(item, one);
        }
    }

    /**
     * Escolhe a lista de candidatos para o passo k baseando-se na saída do
     * passo k-1
     */
    private static List<Registro> elegerCandidatos(List<Registro> candidatosPassoAnterior, int itensPassoAnterior) {
        List<Registro> novosCandidatos = new ArrayList<Registro>();
        List<String> novosItens = null;
        // Gera hashmap dos candidatos do passo anterior
        Map<Integer, List<Registro>> hashMapCandidatosPassoAnterior = gerarHashMap(candidatosPassoAnterior);
        // Ordena candidatos para facilitar busca
        Collections.sort(candidatosPassoAnterior);

        // Gera todos os subconjuntos possíveis para os candidatos
        for (int i = 0; i < (candidatosPassoAnterior.size() - 1); i++) {
            for (int j = i + 1; j < candidatosPassoAnterior.size(); j++) {
                List<String> itensExcluidos = candidatosPassoAnterior.get(i).getItens();
                List<String> itensIncluidos = candidatosPassoAnterior.get(j).getItens();

                if ((itensPassoAnterior - 1) > 0) {
                    boolean repetido = true;
                    for (int k = 0; k < (itensPassoAnterior - 1); k++) {
                        if (!itensExcluidos.get(k).equals(itensIncluidos.get(k))) {
                            repetido = false;
                            break;
                        }
                    }

                    if (repetido) {
                        novosItens = new ArrayList<String>();
                        novosItens.addAll(itensExcluidos);
                        novosItens.add(itensIncluidos.get(itensPassoAnterior - 1));

                        Registro novoItem = new Registro(novosItens, 0);
                        if (prune(hashMapCandidatosPassoAnterior, novoItem)) {
                            novosCandidatos.add(novoItem);
                        }
                    }
                } else {
                    if (!itensExcluidos.get(0).equals(itensIncluidos.get(0))) {
                        novosItens = new ArrayList<String>();
                        novosItens.add(itensExcluidos.get(0));
                        novosItens.add(itensIncluidos.get(0));
                        Registro newItemSet = new Registro(novosItens, 0);
                        novosCandidatos.add(newItemSet);
                    }
                }
            }
        }
        return novosCandidatos;
    }

    /**
     * Criar hashmap de registros utilizando a função hash da classe Registro
     * como chave. Agrupa os registros em 'buckets' de acordo com o resultado de
     * sua função hash
     */
    public static Map<Integer, List<Registro>> gerarHashMap(List<Registro> registros) {
        Map<Integer, List<Registro>> retorno = new HashMap<Integer, List<Registro>>();

        List<Registro> bucket = null;
        for (Registro reg : registros) {
            int hashCode = reg.hashCode();
            if (retorno.containsKey(hashCode)) {
                bucket = retorno.get(hashCode);
            } else {
                bucket = new ArrayList<Registro>();
            }

            bucket.add(reg);
            retorno.put(hashCode, bucket);
        }

        return retorno;
    }

    /**
     * Procura por subconjuntos do registro solicitado no hashmap de candidatos
     */
    private static boolean prune(Map<Integer, List<Registro>> hashMap, Registro novoRegistro) {
        List<Registro> subconjuntos = gerarSubconjuntos(novoRegistro);
        for (Registro r : subconjuntos) {
            boolean contains = false;
            int hashCodeToSearch = r.hashCode();
            if (hashMap.containsKey(hashCodeToSearch)) {
                List<Registro> candidateItemsets = hashMap.get(hashCodeToSearch);
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
        List<Registro> subconjuntos = new ArrayList<Registro>();
        List<String> itens = reg.getItens();
        for (int i = 0; i < itens.size(); i++) {
            List<String> currItems = new ArrayList<String>(itens);
            // remove o item da vez
            currItems.remove(itens.size() - 1 - i);
            // cria conjunto com todos os itens, menos o que acabou de ser
            // removido
            subconjuntos.add(new Registro(currItems, 0));
        }
        return subconjuntos;
    }

    /**
     * Constrói uma árvore de hash a partir da lista de candidatos
     */
    public static NodoArvoreHash constroiArvoreHash(List<Registro> candidatos, int tamanhoRegistro) {
        NodoArvoreHash raiz = new NodoArvoreHash();

        NodoArvoreHash pai = null;
        NodoArvoreHash nodoAtual = null;
        for (Registro reg : candidatos) {
            pai = null;
            nodoAtual = raiz;
            for (int i = 0; i < tamanhoRegistro; i++) {
                String item = reg.getItens().get(i);
                Map<String, NodoArvoreHash> mapa = nodoAtual.getMapa();
                pai = nodoAtual;

                if (mapa.containsKey(item)) {
                    nodoAtual = mapa.get(item);
                } else {
                    nodoAtual = new NodoArvoreHash();
                    mapa.put(item, nodoAtual);
                }
                pai.setMapa(mapa);
            }

            nodoAtual.setNodoFolha(true);
            List<Registro> registros = nodoAtual.getRegistros();
            registros.add(reg);
            nodoAtual.setRegistros(registros);
        }

        return raiz;
    }

    /**
     * Retorna um conjunto de registros de uma árvore de hash de candidatos.
     */
    public static List<Registro> buscaRegistroArvoreHash(NodoArvoreHash nodoRaiz, Registro reg, int indice) {
        if (nodoRaiz.isNodoFolha()) {
            return nodoRaiz.getRegistros();
        }

        List<Registro> registrosEncontrados = new ArrayList<Registro>();
        for (int i = indice; i < reg.getItens().size(); i++) {
            String item = reg.getItens().get(i);
            Map<String, NodoArvoreHash> mapa = nodoRaiz.getMapa();

            if (!mapa.containsKey(item)) {
                continue;
            }
            List<Registro> itemset = buscaRegistroArvoreHash(mapa.get(item), reg, i + 1);
            registrosEncontrados.addAll(itemset);
        }

        return registrosEncontrados;
    }

}