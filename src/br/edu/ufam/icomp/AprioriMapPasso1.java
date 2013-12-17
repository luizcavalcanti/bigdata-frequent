package br.edu.ufam.icomp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.edu.ufam.icomp.model.Registro;

public class AprioriMapPasso1 extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable um = new IntWritable(1);
    private Text item = new Text();

    public void map(Object key, Text registro, Context contexto) throws IOException, InterruptedException {
        Registro reg = Registro.criar(registro.toString());
        for (String s : reg.getItens()) {
            item.set(s);
            contexto.write(item, um);
        }
    }
}