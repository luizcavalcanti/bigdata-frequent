package br.edu.ufam.icomp;

import org.apache.hadoop.util.ToolRunner;

import br.edu.ufam.icomp.apriori.AprioriRunner;

public class MushroomCount {

    public static void main(String[] args) throws Exception {
        int resultado = ToolRunner.run(new AprioriRunner(), args);
        System.exit(resultado);
    }

}