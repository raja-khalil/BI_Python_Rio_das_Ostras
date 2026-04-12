"""Entrypoint de deploy para Streamlit Cloud.

Executa o painel principal garantindo contexto de import no diretório do projeto.
"""

from app.main import main


if __name__ == "__main__":
    main()
