import streamlit as st
import time
from pipeline_01 import processar_arquivos_novos, baixar_pasta_google_drive

def main():
    url_pasta = 'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    diretorio_local = './pasta_gdown'
    arquivos_ja_processados = set()

    st.write("Verificando novos arquivos...")
    baixar_pasta_google_drive(url_pasta, diretorio_local)
    dataframes_novos, arquivos_ja_processados = processar_arquivos_novos(diretorio_local, arquivos_ja_processados)
    
    if not dataframes_novos:
        st.write("Nenhum arquivo novo encontrado.")
    else:
        st.write(f"{len(dataframes_novos)} novo(s) arquivo(s) processado(s).")
        # Aqui você pode adicionar mais lógica para exibir os novos dataframes

if __name__ == '__main__':
    main()
    time.sleep(30)
    st.experimental_rerun()
