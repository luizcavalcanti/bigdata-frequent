package br.edu.ufam.icomp.apriori;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ufam.icomp.model.Registro;

/**
 * Mapper simples que apenas conta todos os itens de todos os registros.
 * Utilizado no primeiro passo do Apriori (k=1)
 */
public class AprioriMapPasso1 extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable um = new IntWritable(1);
    private Text item = new Text();

    public void map(Object key, Text registro, Context contexto) throws IOException, InterruptedException {
        // Cria objeto de registro a partir da linha de dados
        Registro reg = Registro.criar(registro.toString());
        // Para cada item, adiciona registro <item, 1> na saída do mapper
        for (String s : reg.getItens()) {
            item.set(s);
            contexto.write(item, um);
        }
    }
}