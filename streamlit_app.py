# -*- coding: utf-8 -*-
"""
Created on Tue Mar 18 15:41:22 2025

@author: godet-g
"""

import streamlit as st
import pandas as pd
import io
import requests
import json
import gzip
import csv
import os
from metapub import PubMedFetcher

# Fonction pour récupérer les données de Scopus
def get_scopus_data(api_key, query, max_items=2000):
    found_items_num = 1
    start_item = 0
    items_per_query = 25
    JSON = []

    while found_items_num > 0:
        resp = requests.get(
            'https://api.elsevier.com/content/search/scopus',
            headers={'Accept': 'application/json', 'X-ELS-APIKey': api_key},
            params={'query': query, 'count': items_per_query, 'start': start_item}
        )

        if resp.status_code != 200:
            raise Exception(f'Scopus API error {resp.status_code}, JSON dump: {resp.json()}')

        if found_items_num == 1:
            found_items_num = int(resp.json().get('search-results').get('opensearch:totalResults'))

        if found_items_num == 0:
            break

        JSON += resp.json()['search-results']['entry']

        start_item += items_per_query
        found_items_num -= items_per_query

        if start_item >= max_items:
            break

    return JSON

# Fonction pour récupérer les données d'OpenAlex
def get_openalex_data(query, max_items=2000):
    url = 'https://api.openalex.org/works'
    params = {'filter': query, 'per-page': 200}
    JSON = []
    next_cursor = None

    while True:
        if next_cursor:
            params['cursor'] = next_cursor

        resp = requests.get(url, params=params)

        if resp.status_code != 200:
            raise Exception(f'OpenAlex API error {resp.status_code}, JSON dump: {resp.json()}')

        data = resp.json()
        JSON += data['results']
        next_cursor = data['meta'].get('next_cursor')

        if not next_cursor or len(JSON) >= max_items:
            break

    return JSON

# Fonction pour récupérer les données de PubMed
def get_pubmed_data(query, max_items=50):
    fetch = PubMedFetcher()
    pmids = fetch.pmids_for_query(query, retmax=max_items)
    data = []

    for pmid in pmids:
        article = fetch.article_by_pmid(pmid)
        pub_date = article.history.get('pubmed', 'N/A')
        if pub_date != 'N/A':
            pub_date = pub_date.date().isoformat()  # Convertir en format YYYY-MM-DD
        data.append({
            'Data source': 'pubmed',
            'Title': article.title,
            'doi': article.doi,
            'id': pmid,
            'Source title': article.journal,
            'Date': pub_date
        })
    return data

# Fonction pour convertir les données en DataFrame et ajouter la colonne "source"
def convert_to_dataframe(data, source):
    df = pd.DataFrame(data)
    df['source'] = source
    return df

# Fonction pour nettoyer les DOI
def clean_doi(doi):
    if doi and doi.startswith('https://doi.org/'):
        return doi[len('https://doi.org/'):]
    return doi

# Fonction principale
def main():
    st.title("Générateur de CSV")

    # Saisie des paramètres
    scopus_api_key = st.text_input("Scopus API Key")
    scopus_lab_id = st.text_input("Scopus Lab ID")
    openalex_institution_id = st.text_input("OpenAlex Institution ID")
    pubmed_id = st.text_input("PubMed ID")
    start_year = st.number_input("Start Year", min_value=1900, max_value=2100, value=2000)
    end_year = st.number_input("End Year", min_value=1900, max_value=2100, value=2023)

    # Requêtes pour Scopus et OpenAlex
    scopus_query = f"af-ID({scopus_lab_id}) AND PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1}"
    openalex_query = f"institutions.id:{openalex_institution_id},publication_year:{start_year}-{end_year}"
    pubmed_query = f"{pubmed_id}[Affiliation] AND {start_year}/01/01:{end_year}/12/31[Date - Publication]"


    # Récupérer les données de Scopus
    scopus_data = get_scopus_data(scopus_api_key, scopus_query)
    scopus_df = convert_to_dataframe(scopus_data, 'scopus')

    # Récupérer les données d'OpenAlex
    openalex_data = get_openalex_data(openalex_query)
    openalex_df = convert_to_dataframe(openalex_data, 'openalex')

    # Récupérer les données de PubMed
    pubmed_data = get_pubmed_data(pubmed_query)
    pubmed_df = pd.DataFrame(pubmed_data)

    # Sélectionner les colonnes spécifiques pour Scopus
    scopus_df = scopus_df[['source', 'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']]
    scopus_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']

    # Sélectionner les colonnes spécifiques pour OpenAlex
    openalex_df['Source title'] = openalex_df.apply(
        lambda row: row['primary_location']['source']['display_name'] if row['primary_location'] and row['primary_location'].get('source') else None, axis=1
    )
    openalex_df['Date'] = openalex_df['publication_date']
    openalex_df = openalex_df[['source', 'title', 'doi', 'id', 'Source title', 'Date']]
    openalex_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']

    # Nettoyer les DOI pour OpenAlex
    openalex_df['doi'] = openalex_df['doi'].apply(clean_doi)

    # Combiner les DataFrames
    combined_df = pd.concat([scopus_df, openalex_df, pubmed_df], ignore_index=True)def generate_csv(scopus_api_key, scopus_lab_id, openalex_institution_id, pubmed_id, start_year, end_year):
    

    # Générez le CSV à partir du DataFrame
    csv = combined_df.to_csv('results.csv', index=False)
    return csv


if __name__ == "__main__":
    main()
