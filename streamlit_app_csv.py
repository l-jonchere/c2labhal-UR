import streamlit as st
import pandas as pd
import io
from streamlit_app_Scopus_OpenAlex_Pubmed import (
    check_df,
    enrich_w_upw_parallel,
    add_permissions_parallel,
    deduce_todo,
    normalise,
    statut_doi,
    HalCollImporter
)

class CSVApp:
    def __init__(self):
        self.uploaded_file = None
        self.collection_a_chercher = ""
        self.processed_df = pd.DataFrame()


    def process_csv(self, uploaded_file, collection_a_chercher):
        """
        Traite le fichier CSV uploadé, effectue les vérifications HAL et Unpaywall,
        et retourne le DataFrame enrichi.
        """
        df = pd.read_csv(uploaded_file)

        # Assure que les colonnes 'doi' et 'Title' existent
        if 'doi' not in df.columns or 'Title' not in df.columns:
            st.error("Le fichier CSV doit contenir les colonnes 'doi' et 'Title'.")
            return None

        # Normalise les DOIs
        df['doi'] = df['doi'].apply(lambda x: x.lower() if isinstance(x, str) else x)

        # Récupère les données de la collection HAL
        coll = HalCollImporter(collection_a_chercher)
        coll_df = coll.import_data()
        coll_df['nti'] = coll_df['Titres'].apply(lambda x: normalise(x).strip())

        # Effectue les vérifications HAL
        df_checked = check_df(df, coll_df)

        # Enrichit avec les données Unpaywall
        df_enriched = enrich_w_upw_parallel(df_checked)

        # Ajoute les permissions de dépôt
        df_final = add_permissions_parallel(df_enriched)

        # Déduit les actions à effectuer
        df_final['Action'] = df_final.apply(deduce_todo, axis=1)

        return df_final


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

if __name__ == "__main__":
    main()
