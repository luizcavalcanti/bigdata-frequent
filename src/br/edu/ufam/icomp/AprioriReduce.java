package br.edu.ufam.icomp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AprioriReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text conjunto, Iterable<IntWritable> valores, Context contexto) throws IOException,
            InterruptedException {
        int contador = 0;
        for (IntWritable valor : valores) {
            contador += valor.get();
        }
        // TODO : This can be improved. Creating too many strings.
         String itemsetIds = conjunto.toString();
         itemsetIds = itemsetIds.replace("[", "");
         itemsetIds = itemsetIds.replace("]", "");
         itemsetIds = itemsetIds.replace(" ", "");
        float suporte = Float.parseFloat(contexto.getConfiguration().get("suporteMinimo"));
        int registros = contexto.getConfiguration().getInt("numeroRegistros", 2);
        if (possuiSuporteMinimo(suporte, registros, contador)) {
            contexto.write(new Text(itemsetIds), new IntWritable(contador));
        }
    }
    
    private static boolean possuiSuporteMinimo(float suporteMinimo, int numeroRegistros, int contador) {
        int suporte = (int) (suporteMinimo * numeroRegistros);
        if (contador >= suporte) {
            return true;
        }
        return false;
    }
}