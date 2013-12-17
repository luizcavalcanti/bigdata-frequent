package br.edu.ufam.icomp;

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

public class AprioriRunner extends Configured implements Tool {

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 5) {
            System.err
                    .println("Parametros: dir-de-entrada dir-de-saida max-de-passos percentual-de-suporte numero-de-registros");
            return -1;
        }

        String entrada = args[0];
        String saida = args[1];
        int maximoPassos = Integer.parseInt(args[2]);
        float suporteMinimo = Float.parseFloat(args[3]);
        int numeroReg = Integer.parseInt(args[4]);
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        for (int numPasso = 1; numPasso <= maximoPassos; numPasso++) {
            endTime = System.currentTimeMillis();
            boolean jobTerminado = executarJob(entrada, saida, numPasso, suporteMinimo, numeroReg);
            if (!jobTerminado) {
                System.err.println("Job falhou. Saindo...");
                return -1;
            }
            System.out.println("Passo " + numPasso + ": " + (System.currentTimeMillis() - endTime));
        }
        endTime = System.currentTimeMillis();
        System.out.println("Tempo total: " + (endTime - startTime));
        return 0;
    }

    private static boolean executarJob(String entrada, String saida, int passo, float suporteMinimo, int numeroReg)
            throws IOException, InterruptedException, ClassNotFoundException {
        boolean successful = false;

        JobConf jobConfig = new JobConf();
        jobConfig.setInt("passo", passo);
        jobConfig.setFloat("suporteMinimo", suporteMinimo);
        jobConfig.setInt("numeroRegistros", numeroReg);

        FileInputFormat.addInputPath(jobConfig, new Path(entrada));
        FileOutputFormat.setOutputPath(jobConfig, new Path(saida + passo));

        // TODO : Should I scan all files in this output directory?
        /*
         * The large itemsets of pass (K-1) are used to derive the candidate
         * itemsets of pass K. So, storing the large itemsets of pass (K-1) in
         * distributed cache, so that it is accessible to pass K MR job.
         */
        /*
         * if (passo > 1) {
         * DistributedCache.addCacheFile(URI.create("hdfs://127.0.0.1:54310" +
         * saida + (passo - 1) + "/part-r-00000"), jobConfig);
         * System.out.println
         * ("Adicionado o resultado do passo 1 ao cache distribuido."); }
         */

        Job job = new Job(jobConfig, "passo_" + passo);
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
