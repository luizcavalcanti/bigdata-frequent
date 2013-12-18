package br.edu.ufam.icomp.apriori;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Implementação do Reduce para MapReduce Apriori
 */
public class AprioriReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text conjunto, Iterable<IntWritable> valores, Context contexto) throws IOException,
            InterruptedException {
        int contador = 0;
        // Itera em todas as ocorrências e soma os valores
        for (IntWritable valor : valores) {
            contador += valor.get();
        }
        // Removendo formatação de arraylist padrão do java
        String itemsetIds = conjunto.toString();
        itemsetIds = itemsetIds.replace("[", "");
        itemsetIds = itemsetIds.replace("]", "");
        itemsetIds = itemsetIds.replace(" ", "");
        float suporte = Float.parseFloat(contexto.getConfiguration().get("suporteMinimo"));
        int registros = contexto.getConfiguration().getInt("numeroRegistros", 2);
        // Se houver suporte mínimo, escreve registro e contador na saída
        if (possuiSuporteMinimo(suporte, registros, contador)) {
            contexto.write(new Text(itemsetIds), new IntWritable(contador));
        }
    }

    /**
     * Verifica se um determinado registro ocorre um número de vezes que
     * satisfaça o suporte mínimo informado pelo usuário.
     */
    private static boolean possuiSuporteMinimo(float suporteMinimo, int totalRegistros, int contador) {
        int suporte = (int) Math.ceil(suporteMinimo * totalRegistros);
        if (contador >= suporte) {
            return true;
        }
        return false;
    }
}