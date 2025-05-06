
import streamlit as st
import pandas as pd
import io
import requests
from streamlit_app import (
    HalCollImporter, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo, normalise, normalize_name, get_initial_form
)

def main():
    st.title("Enrichissement des publications")
    st.subheader("Enrichissez les publications avec HAL, Unpaywall et OA.works")

    # Saisie du nom de la collection HAL
    collection_a_chercher = st.text_input(
        "Collection HAL",
        value="",
        key="collection_hal",
        help="Saisissez le nom de la collection HAL du laboratoire, par exemple MIP"
    )

    # Téléversement du fichier CSV
    uploaded_file = st.file_uploader("Téléversez un fichier CSV avec les colonnes 'Title' et 'doi'", type=["csv"])

    if uploaded_file is not None:
        # Lire le fichier CSV
        csv_data = pd.read_csv(uploaded_file)

        # Vérifier la présence des colonnes nécessaires
        if 'Title' in csv_data.columns and 'doi' in csv_data.columns:
            # Initialiser la barre de progression
            progress_bar = st.progress(0)
            progress_text = st.empty()

            # Récupérer les informations HAL
            progress_text.text("Récupération des informations HAL...")
            progress_bar.progress(20)
            coll = HalCollImporter(collection_a_chercher, 2020, 2025)  # Exemple d'années
            coll_df = coll.import_data()
            coll_df['nti'] = coll_df['Titres'].apply(lambda x: normalise(x).strip())

            # Enrichir avec Unpaywall
            progress_text.text("Enrichissement avec Unpaywall...")
            progress_bar.progress(50)
            csv_data = enrich_w_upw_parallel(csv_data)

            # Enrichir avec OA.works
            progress_text.text("Enrichissement avec OA.works...")
            progress_bar.progress(80)
            csv_data = add_permissions_parallel(csv_data)

            # Déduire les actions à entreprendre
            csv_data['Action'] = csv_data.apply(deduce_todo, axis=1)

            # Exporter le CSV enrichi
            if not csv_data.empty:
                enriched_csv = csv_data.to_csv(index=False)
                csv_bytes = io.BytesIO()
                csv_bytes.write(enriched_csv.encode('utf-8'))
                csv_bytes.seek(0)

                st.download_button(
                    label="📥 Télécharger le CSV enrichi",
                    data=csv_bytes,
                    file_name=f"{collection_a_chercher}_enriched.csv",
                    mime="text/csv"
                )

                progress_bar.progress(100)
                progress_text.text("Terminé ✅")
            else:
                st.error("Aucune donnée à exporter.")
        else:
            st.error("Le fichier CSV doit contenir les colonnes 'Title' et 'doi'.")
    else:
        st.info("Veuillez téléverser un fichier CSV.")

if __name__ == "__main__":
    main()
