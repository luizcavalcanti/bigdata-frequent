package br.edu.ufam.icomp.conversor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Classe utilitária para conversão dos dados de sua forma original para um
 * formato mais adequado ao uso do Apriori MapReduce.
 */
public class ConversorDeDados {

    private static File entrada;
    private static File saida;

    public static void main(String[] args) {
        if (args.length != 2) // Checagem de parâmetros
            abortarExecucao("Parametros: arquivo-entrada arquivo-saida");

        carregarParametros(args);

        // Se não houver arquivo válido de entrada, abortar a execução
        if (!entrada.exists())
            abortarExecucao("Arquivo de entrada não encontrado");

        converterArquivo();
    }

    /**
     * Carrega parametros necessários para conversão dos dados
     * 
     * @param args
     *            argumentos de linha de comando
     */
    private static void carregarParametros(String[] args) {
        entrada = new File(args[0]);
        saida = new File(args[1]);
    }

    /**
     * Método utilitário que imprime uma mensagem e aborta a execução da
     * aplicação
     * 
     * @param mensagem
     */
    private static void abortarExecucao(String mensagem) {
        System.err.println(mensagem);
        System.exit(-1);
    }

    /**
     * Método principal da classe utilitária, converte o arquivo para um formato
     * mais adequado ao MapReduce Apriori
     */
    private static void converterArquivo() {
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            br = new BufferedReader(new FileReader(entrada));
            bw = new BufferedWriter(new FileWriter(saida));
            String linha = null;

            // Lê todo o conteúdo do arquivo, linha a linha (registro a
            // registro),
            // convertendo as mesmas e escrevendo no arquivo de saída
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

    /**
     * Método que converte um registro do arquivo original em um registro mais
     * adequado ao mapreduce
     * 
     * @param linha
     * @return linha convertida
     */
    private static String converterRegistro(String linha) {
        String[] valores = linha.split("[,]");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < valores.length; i++) { // Para cada coluna
            // Se for o primeiro atributo, dispensa a vírgula
            if (i != 0)
                sb.append(',');
            // Se o índice do atributo for maior que 10, adiciona um
            // 0 para ajudar na ordenação de strings adequada, antes
            // da fase de reduce
            if (i < 10)
                sb.append('0');
            // Adiciona índice da coluna, separador e valor da coluna no arquivo
            // original
            sb.append(i).append(':').append(valores[i]);
        }
        return sb.toString();
    }

}
