package br.edu.ufam.icomp.apriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import br.edu.ufam.icomp.MushroomCount;

/**
 * Classe responsável pela configuração, execução e controle dos K passos
 * solicitados para a implementação apriori mapreduce
 */
public class AprioriRunner extends Configured implements Tool {

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 5) {
            System.err.println("Parâmetros: dir-entrada dir-saida passos suporte-minimo total-registros");
            return -1;
        }

        // Armazena todas as variáveis passadas como parâmetros no console
        String entrada = args[0];
        String saida = args[1];
        int maximoPassos = Integer.parseInt(args[2]);
        float suporteMinimo = Float.parseFloat(args[3]);
        int numeroReg = Integer.parseInt(args[4]);

        // Cria variáveis para controle do tempo de execução de cada passo
        long inicio = System.currentTimeMillis();
        long fim = System.currentTimeMillis();
        // Para cada passo previsto...
        for (int numPasso = 1; numPasso <= maximoPassos; numPasso++) {
            fim = System.currentTimeMillis();
            // ...executa o job do haddop, com seus parâmetros
            boolean jobTerminado = executarJob(entrada, saida, numPasso, suporteMinimo, numeroReg);
            if (!jobTerminado) { // Se houve um problema na execução do passo
                System.err.println("Job falhou. Execução abortada...");
                return -1;
            }
            System.out.println("Passo " + numPasso + ": " + (System.currentTimeMillis() - fim));
        }
        fim = System.currentTimeMillis();
        System.out.println("Tempo total: " + (fim - inicio));
        return 0;
    }

    /**
     * Método responsável por configurar o job do hadoop e dispara sua execução.
     * 
     * @return resultado da execução (apenas se foi um sucesso ou não)
     */
    private static boolean executarJob(String entrada, String saida, int passo, float suporteMinimo, int numeroReg)
            throws IOException, InterruptedException, ClassNotFoundException {
        boolean successful = false;

        // Adiciona os parâmetros da execução do job, que posteriormente pode
        // ser recuperado para ser utilizado tanto no Mao quanto no Reduce
        JobConf configuracao = new JobConf();
        configuracao.setInt("passo", passo);
        configuracao.setFloat("suporteMinimo", suporteMinimo);
        configuracao.setInt("numeroRegistros", numeroReg);

        FileInputFormat.addInputPath(configuracao, new Path(entrada));
        FileOutputFormat.setOutputPath(configuracao, new Path(saida + passo));

        Job job = new Job(configuracao, "passo_" + passo);
        // O problema exige que o primeiro passo tenha um código para Map
        // diferente das demais execuções
        if (passo == 1) {
            job.setMapperClass(AprioriMapPasso1.class);
        } else {
            job.setMapperClass(AprioriMapPassoK.class);
        }
        job.setJarByClass(MushroomCount.class);
        job.setReducerClass(AprioriReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        successful = (job.waitForCompletion(true) ? true : false);
        System.out.println("Passo " + passo + " terminado");
        return successful;
    }

}
