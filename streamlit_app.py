import streamlit as st
import pandas as pd
from metapub import PubMedFetcher
import requests

# Fonctions pour récupérer les données
def get_pubmed_data(query, max_items=50):
    fetch = PubMedFetcher()
    pmids = fetch.pmids_for_query(query, retmax=max_items)
    data = []

    for pmid in pmids:
        article = fetch.article_by_pmid(pmid)
        pub_date = article.history.get('pubmed', 'N/A')
        if pub_date != 'N/A':
            pub_date = pub_date.date().isoformat()
        data.append({
            'Data source': 'pubmed',
            'Title': article.title,
            'DOI': article.doi,
            'ID': pmid,
            'Source Title': article.journal,
            'Date': pub_date
        })
    return pd.DataFrame(data)

# Interface Streamlit
st.title('PubMed Data Extractor')

# Champs de saisie
pubmed_id = st.text_input('PubMed ID (Affiliation)', 'incit')
start_year = st.number_input('Start Year', min_value=1900, max_value=2100, value=2020)
end_year = st.number_input('End Year', min_value=1900, max_value=2100, value=2021)
max_items = st.slider('Max Articles', 1, 100, 50)

# Bouton de lancement
if st.button('Fetch Data'):
    pubmed_query = f"{pubmed_id}[Affiliation] AND {start_year}/01/01:{end_year}/12/31[Date - Publication]"
    df = get_pubmed_data(pubmed_query, max_items)
    st.write(df)
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button('Download CSV', csv, 'pubmed_results.csv', 'text/csv')

st.write('Made with ❤️ using Streamlit')
