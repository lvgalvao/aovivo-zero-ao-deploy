import streamlit as st
from pipeline_02 import conectar_banco, processar_arquivos_novos, baixar_pasta_google_drive, inicializar_tabela

def main():
    st.title('Verificação de Novos Arquivos CSV')

    url_pasta = 'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    diretorio_local = './pasta_gdown'

    # Conecta ao banco de dados e inicializa a tabela
    con = conectar_banco()
    inicializar_tabela(con)

    if st.button('Processar novos arquivos'):
        # Informa ao usuário que o processo começou
        with st.spinner('Baixando e processando novos arquivos...'):
            baixar_pasta_google_drive(url_pasta, diretorio_local)
            dataframes_novos = processar_arquivos_novos(diretorio_local, con)

            if not dataframes_novos:
                st.success("Nenhum arquivo novo encontrado.")
            else:
                st.success(f"{len(dataframes_novos)} novo(s) arquivo(s) processado(s).")

        # Exibe o histórico de arquivos processados
        st.write("Histórico de arquivos processados:")
        historico = con.execute("SELECT nome_arquivo, horario_processamento FROM historico_arquivos ORDER BY horario_processamento DESC").fetchall()
        if historico:
            st.table(historico)
        else:
            st.write("Nenhum arquivo foi processado ainda.")

if __name__ == '__main__':
    main()
