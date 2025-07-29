import processador

def main():
    caminho = "./dados_recebidos/info_transportes.csv"

    dados = processador.processar_arquivo(caminho)
    dados.write.option("header", "true").option("sep", ";").csv("./dados_processados/info_corridas_dia")


if __name__ == "__main__":
    main()










