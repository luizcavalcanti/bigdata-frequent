package br.edu.ufam.icomp;

import org.apache.hadoop.util.ToolRunner;

import br.edu.ufam.icomp.apriori.AprioriRunner;

/**
 * Ponto de entrada da execução do algoritmo apriori mapreduce. Dispara a classe
 * responsável pelo gestão da execução dos K passos
 */
public class MushroomCount {

    public static void main(String[] args) throws Exception {
        int resultado = ToolRunner.run(new AprioriRunner(), args);
        System.exit(resultado);
    }

}