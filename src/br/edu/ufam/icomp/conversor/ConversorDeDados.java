package br.edu.ufam.icomp.conversor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ConversorDeDados {

    private static File entrada;
    private static File saida;

    public static void main(String[] args) {
        if (args.length != 2)
            abortarExecucao("Parametros: arquivo-entrada arquivo-saida");

        carregarParametros(args);

        if (!entrada.exists())
            abortarExecucao("Arquivo de entrada não encontrado");

        converterArquivo();
    }

    private static void carregarParametros(String[] args) {
        entrada = new File(args[0]);
        saida = new File(args[1]);
    }

    private static void abortarExecucao(String mensagem) {
        System.err.println(mensagem);
        System.exit(-1);
    }

    private static void converterArquivo() {
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            br = new BufferedReader(new FileReader(entrada));
            bw = new BufferedWriter(new FileWriter(saida));
            String linha = null;
            while ((linha = br.readLine()) != null) {
                linha = linha.trim();
                String convertido = converterRegistro(linha);
                bw.write(convertido);
                bw.write('\n');
            }
            bw.flush();
            bw.close();
            br.close();
        } catch (IOException e) {
            abortarExecucao(e.getMessage());
        }
    }

    private static String converterRegistro(String linha) {
        String[] valores = linha.split("[,]");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < valores.length; i++) {
            if (i != 0)
                sb.append(',');
            if (i < 10)
                sb.append('0');
            sb.append(i).append(':').append(valores[i]);
        }
        return sb.toString();
    }

}
