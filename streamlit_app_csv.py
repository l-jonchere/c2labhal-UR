import streamlit as st
import pandas as pd
import io
from utils import (
    get_scopus_data, get_openalex_data, get_pubmed_data, convert_to_dataframe,
    clean_doi, HalCollImporter, merge_rows_with_sources, get_authors_from_crossref,
    check_df, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo,
    normalise, normalize_name, get_initial_form
)

class CSVApp:
    def __init__(self):
        self.prefix = "app2_" 
        self.uploaded_file = None
        self.collection_a_chercher = ""
        self.processed_df = pd.DataFrame()

    def run(self):
        st.title("🥎 c2LabHAL - Version import csv")
        st.subheader("Comparez les publications contenues dans un fichier .csv avec une collection HAL")

        self.uploaded_file = st.file_uploader("Téléversez un fichier CSV (ce fichier doit contenir une colonne 'doi' et une colonne 'Title')", type="csv", key="file_uploader_app2")
        self.collection_a_chercher = st.text_input("Nom de la collection HAL à comparer :", "", key="collection_hal_app2")

        if self.uploaded_file and self.collection_a_chercher:
            with st.spinner("Traitement du fichier CSV..."):
                self.processed_df = self.process_csv(self.uploaded_file, self.collection_a_chercher)

            if self.processed_df is not None:  # Vérifie que process_csv n'a pas retourné None (erreur)
                st.dataframe(self.processed_df)

                # Génère un nom de fichier unique
                filename = f"publications_verifiees_{self.collection_a_chercher}.csv"

                # Convertit le DataFrame en CSV
                csv = self.processed_df.to_csv(index=False, encoding='utf-8')

                # Propose le téléchargement
                st.download_button(
                    label="Télécharger le CSV enrichi",
                    data=csv.encode('utf-8'),
                    file_name=filename,
                    mime="text/csv",
                    key="download_button_app2"
                )
            else:
                st.error("Veuillez fournir un fichier CSV valide avec les colonnes 'doi' et 'Title', et un nom de collection HAL.")