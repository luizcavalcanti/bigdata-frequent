package br.edu.ufam.icomp.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Nodo de uma árvore de hash, utilizada para busca eficiente no conjunto de
 * dados.
 */
public class NodoArvoreHash {

    private Map<String, NodoArvoreHash> mapa;
    private boolean nodoFolha;
    private List<Registro> registros;

    public NodoArvoreHash() {
        mapa = new HashMap<String, NodoArvoreHash>();
        nodoFolha = false;
        registros = new ArrayList<Registro>();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("folha : ").append(Boolean.toString(nodoFolha)).append("\t");
        builder.append("chaves :").append(mapa.keySet().toString()).append("\t");
        builder.append("registros : ").append(registros.toString());
        return builder.toString();
    }

    public Map<String, NodoArvoreHash> getMapa() {
        return mapa;
    }

    public void setMapa(Map<String, NodoArvoreHash> mapa) {
        this.mapa = mapa;
    }

    public boolean isNodoFolha() {
        return nodoFolha;
    }

    public void setNodoFolha(boolean nodoFolha) {
        this.nodoFolha = nodoFolha;
    }

    public List<Registro> getRegistros() {
        return registros;
    }

    public void setRegistros(List<Registro> registros) {
        this.registros = registros;
    }

}