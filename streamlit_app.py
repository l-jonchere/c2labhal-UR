import streamlit as st
import pandas as pd
import io

def generate_csv(scopus_api_key, scopus_lab_id, openalex_institution_id, pubmed_id, start_year, end_year):
    # Exemple de données fictives pour les champs spécifiés
    data = {
        'Data source': ['Scopus', 'PubMed', 'OpenAlex'],
        'Title': ['Title 1', 'Title 2', 'Title 3'],
        'doi': ['10.1000/xyz123', '10.1000/abc456', '10.1000/def789'],
        'id': [scopus_lab_id, pubmed_id, openalex_institution_id],
        'Source title': ['Source 1', 'Source 2', 'Source 3'],
        'Date': [f'{start_year}-01-01', f'{end_year}-01-01', f'{start_year}-06-01']
    }
    df = pd.DataFrame(data)

    # Générer le CSV à partir du DataFrame
    csv = df.to_csv(index=False)
    return csv

def main():
    st.title("Générateur de CSV")

    # Saisie des paramètres
    scopus_api_key = st.text_input("Scopus API Key")
    scopus_lab_id = st.text_input("Scopus Lab ID")
    openalex_institution_id = st.text_input("OpenAlex Institution ID")
    pubmed_id = st.text_input("PubMed ID")
    start_year = st.number_input("Start Year", min_value=1900, max_value=2100, value=2000)
    end_year = st.number_input("End Year", min_value=1900, max_value=2100, value=2023)

    if st.button("Télécharger le CSV"):
        # Générer le CSV
        csv = generate_csv(scopus_api_key, scopus_lab_id, openalex_institution_id, pubmed_id, start_year, end_year)

        # Créer un objet BytesIO pour stocker le CSV
        csv_bytes = io.BytesIO()
        csv_bytes.write(csv.encode('utf-8'))
        csv_bytes.seek(0)

        # Proposer le téléchargement du CSV
        st.download_button(
            label="Télécharger le CSV",
            data=csv_bytes,
            file_name="parametres.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
