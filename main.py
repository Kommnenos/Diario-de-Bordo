import os
import shutil
import glob
import processador

def main():
    caminho = "./dados_recebidos/info_transportes.csv"
    diretorio_saida = "./dados_processados/info_corridas_dia_temp"
    caminho_resultado_final = "./dados_processados/info_corridas_dia.csv"

    dados = processador.processar_arquivo(caminho)
    dados.coalesce(1).write.option("header", "true").option("sep", ";").mode("overwrite").csv(diretorio_saida)

    ## Logica para batch ou trabalhar com amostras
    organizar_arquivo(caminho_resultado_final, diretorio_saida)


def organizar_arquivo(final_csv_path, output_dir):
    ## Encontra o arquivo particionado
    part_file = glob.glob(os.path.join(output_dir, "part-*.csv"))[0]
    ## Move e renomeia
    shutil.move(part_file, final_csv_path)
    ## Limpa saida original
    shutil.rmtree(output_dir)
    print(f"Arquivo final salvo como: {final_csv_path}")


if __name__ == "__main__":
    main()









