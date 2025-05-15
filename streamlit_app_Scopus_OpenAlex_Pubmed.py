import os
import streamlit as st
import pandas as pd
import io
import requests
import json
from metapub import PubMedFetcher
import regex as re
from unidecode import unidecode
import unicodedata
from difflib import get_close_matches
from langdetect import detect
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor  
from utils import (
    get_scopus_data, get_openalex_data, get_pubmed_data, convert_to_dataframe,
    clean_doi, HalCollImporter, merge_rows_with_sources, get_authors_from_crossref,
    check_df, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo,
    normalise, normalize_name, get_initial_form
)

# Configurer tqdm pour pandas
tqdm.pandas()

class ScopusOpenAlexPubmedApp:
    def __init__(self):
        self.prefix = "app1_"  # Préfixe unique pour cette application
        self.collection_a_chercher = ""
        self.openalex_institution_id = ""
        self.pubmed_id = ""
        self.pubmed_api_key = ""
        self.scopus_lab_id = ""
        self.scopus_api_key = ""
        self.start_year = 2020
        self.end_year = 2025
        self.fetch_authors = False
        self.compare_authors = False
        self.uploaded_authors_file = None
        self.progress_bar = None
        self.progress_text = None
        self.scopus_df = pd.DataFrame()
        self.openalex_df = pd.DataFrame()
        self.pubmed_df = pd.DataFrame()
        self.combined_df = pd.DataFrame()
        self.merged_data = pd.DataFrame()


    def run(self): # La méthode run est maintenant à l'intérieur de la classe
        st.title("🥎 c2LabHAL")
        st.subheader("Comparez les publications d'un labo dans Scopus, OpenAlex et Pubmed avec sa collection HAL")

        self.collection_a_chercher = st.text_input(
            "Collection HAL",
            value="",
            key=self.prefix + "collection_hal",  # Préfixe ajouté
            help="Saisissez le nom de la collection HAL du laboratoire, par exemple MIP"
        )

        self.openalex_institution_id = st.text_input(
            "Identifiant OpenAlex du labo",
            key=self.prefix + "openalex_id",  # Préfixe ajouté
            help="Saisissez l'identifiant du labo dans OpenAlex, par exemple i4392021216"
        )

        col1, col2 = st.columns(2)
        with col1:
            self.pubmed_id = st.text_input(
                "Requête PubMed",
                key=self.prefix + "pubmed_query",  # Préfixe ajouté
                help="Saisissez la requête Pubmed qui rassemble le mieux les publications du labo..."
            )
        with col2:
            self.pubmed_api_key = st.text_input(
                "Clé API Pubmed",
                key=self.prefix + "pubmed_api_key",  # Préfixe ajouté
                help="Pour obtenir une clé API, connectez vous sur Pubmed..."
            )

        col1, col2 = st.columns(2)
        with col1:
            self.scopus_lab_id = st.text_input(
                "Identifiant Scopus du labo",
                key=self.prefix + "scopus_lab_id",  # Préfixe ajouté
                help="Saisissez le Scopus Affiliation Identifier du laboratoire..."
            )
        with col2:
            self.scopus_api_key = st.text_input(
                "Clé API Scopus",
                key=self.prefix + "scopus_api_key",  # Préfixe ajouté
                help="Pour obtenir une clé API : https://dev.elsevier.com/..."
            )

        col1, col2 = st.columns(2)
        with col1:
            self.start_year = st.number_input(
                "Année de début",
                min_value=1900,
                max_value=2100,
                value=2020,
                key=self.prefix + "start_year"  # Préfixe ajouté
            )
        with col2:
            self.end_year = st.number_input(
                "Année de fin",
                min_value=1900,
                max_value=2100,
                value=2025,
                key=self.prefix + "end_year"  # Préfixe ajouté
            )

        self.fetch_authors = st.checkbox(
            "🧑‍🔬 Récupérer les auteurs sur Crossref",
            key=self.prefix + "fetch_authors"  # Préfixe ajouté
        )

        if self.fetch_authors:
            self.compare_authors = st.checkbox(
                "🔍 Comparer les auteurs avec ma liste de chercheurs",
                key=self.prefix + "compare_authors"  # Préfixe ajouté
            )
            if self.compare_authors:
                self.uploaded_authors_file = st.file_uploader(
                    "📤 Téléversez un fichier CSV avec deux colonnes : 'collection', 'prénom nom'",
                    type=["csv"],
                    key=self.prefix + "authors_file"  # Préfixe ajouté
                )

        self.progress_bar = st.progress(0)  # Préfixe ajouté
        self.progress_text = st.empty()

        if st.button("Rechercher", key=self.prefix + "rechercher"):  
            # Configurer la clé API PubMed si elle est fournie
            if self.pubmed_api_key:
                os.environ['NCBI_API_KEY'] = self.pubmed_api_key

            # Initialiser des DataFrames vides
            self.scopus_df = pd.DataFrame()
            self.openalex_df = pd.DataFrame()
            self.pubmed_df = pd.DataFrame()

            # Étape 1 : Récupération des données OpenAlex
            with st.spinner("OpenAlex"):
                self.progress_text.text("Étape 1 : Récupération des données OpenAlex")
                self.progress_bar.progress(10)
                if self.openalex_institution_id:
                    openalex_query = f"institutions.id:{self.openalex_institution_id},publication_year:{self.start_year}-{self.end_year}"
                    openalex_data = self.get_openalex_data(openalex_query)
                    self.openalex_df = self.convert_to_dataframe(openalex_data, 'openalex')
                    self.openalex_df['Source title'] = self.openalex_df.apply(
                        lambda row: row['primary_location']['source']['display_name'] if row['primary_location'] and row['primary_location'].get('source') else None, axis=1
                    )
                    self.openalex_df['Date'] = self.openalex_df.apply(
                        lambda row: row.get('publication_date', None), axis=1
                    )
                    self.openalex_df['doi'] = self.openalex_df.apply(
                        lambda row: row.get('doi', None), axis=1
                    )
                    self.openalex_df['id'] = self.openalex_df.apply(
                        lambda row: row.get('id', None), axis=1
                    )
                    self.openalex_df['title'] = self.openalex_df.apply(
                        lambda row: row.get('title', None), axis=1
                    )
                    self.openalex_df = self.openalex_df[['source', 'title', 'doi', 'id', 'Source title', 'Date']]
                    self.openalex_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                    self.openalex_df['doi'] = self.openalex_df['doi'].apply(clean_doi)

            # Étape 2 : Récupération des données PubMed
            with st.spinner("Pubmed"):
                self.progress_text.text("Étape 2 : Récupération des données PubMed")
                self.progress_bar.progress(30)
                if self.pubmed_id:
                    pubmed_query = f"{self.pubmed_id} AND {self.start_year}/01/01:{self.end_year}/12/31[Date - Publication]"
                    pubmed_data = self.get_pubmed_data(pubmed_query)
                    self.pubmed_df = pd.DataFrame(pubmed_data)

            # Étape 3 : Récupération des données Scopus
            with st.spinner("Scopus"):
                self.progress_text.text("Étape 3 : Récupération des données Scopus")
                self.progress_bar.progress(50)
                if self.scopus_api_key and self.scopus_lab_id:
                    scopus_query = f"af-ID({self.scopus_lab_id}) AND PUBYEAR > {self.start_year - 1} AND PUBYEAR < {self.end_year + 1}"
                    scopus_data = self.get_scopus_data(self.scopus_api_key, scopus_query)
                    self.scopus_df = self.convert_to_dataframe(scopus_data, 'scopus')

                    self.scopus_df = self.scopus_df[['source', 'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']]
                    self.scopus_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']

            # Étape 4 : Comparaison avec HAL (si le champ "Collection HAL" n'est pas vide)
            if self.collection_a_chercher:
                with st.spinner("HAL"):
                    self.progress_text.text("Étape 4 : Comparaison avec HAL")
                    self.progress_bar.progress(70)
                    # Combiner les DataFrames
                    self.combined_df = pd.concat([self.scopus_df, self.openalex_df, self.pubmed_df], ignore_index=True)

                    # Récupérer les données HAL
                    coll = HalCollImporter(self.collection_a_chercher, self.start_year, self.end_year)
                    coll_df = coll.import_data()
                    coll_df['nti'] = coll_df['Titres'].apply(lambda x: normalise(x).strip())
                    self.combined_df = check_df(self.combined_df, coll_df, progress_bar=self.progress_bar, progress_text=self.progress_text)

                with st.spinner("Unpaywall"):
                    self.progress_text.text("Étape 5 : Récupération des données Unpaywall")
                    self.progress_bar.progress(75)
                    self.combined_df = enrich_w_upw_parallel(self.combined_df)

                with st.spinner("OA.Works"):
                    self.progress_text.text("Étape 6 : Récupération des permissions via OA.Works")
                    self.progress_bar.progress(85)
                    self.combined_df = add_permissions_parallel(self.combined_df)

                self.combined_df['Action'] = self.combined_df.apply(deduce_todo, axis=1)
            else:
                self.combined_df = pd.concat([self.scopus_df, self.openalex_df, self.pubmed_df], ignore_index=True)

             # Étape 7 : Fusion des lignes en double
            with st.spinner("Fusion"):
                self.progress_text.text("Étape 7 : Fusion des lignes en double")
                self.progress_bar.progress(90)
                # Séparer les lignes avec et sans DOI
                with_doi = self.combined_df.dropna(subset=['doi'])
                without_doi = self.combined_df[self.combined_df['doi'].isna()]

                # Fusionner les lignes avec DOI
                merged_with_doi = with_doi.groupby('doi', as_index=False).apply(merge_rows_with_sources)

                # Combiner les lignes fusionnées avec les lignes sans DOI
                self.merged_data = pd.concat([merged_with_doi, without_doi], ignore_index=True)

               # Étape 8 : Ajout des auteurs à partir de Crossref (si la case est cochée)
            if self.fetch_authors:
                with st.spinner("Recherche des auteurs Crossref"):
                    self.progress_text.text("Étape 8 : Recherche des auteurs via Crossref")
                    self.progress_bar.progress(92)
                    self.merged_data['Auteurs'] = self.merged_data['doi'].apply(lambda doi: '; '.join(get_authors_from_crossref(doi)) if doi else '')

                # Étape 9 : Comparaison avec le fichier de chercheurs
                if self.compare_authors and self.uploaded_authors_file and self.collection_a_chercher:
                    with st.spinner("Comparaison des auteurs avec le fichier"):
                        self.progress_text.text("Étape 9 : Comparaison des auteurs")
                        self.progress_bar.progress(95)

                        user_df = pd.read_csv(self.uploaded_authors_file)
                        if "collection" not in user_df.columns or user_df.columns[1] not in user_df.columns:
                            st.error("❌ Le fichier doit contenir une colonne 'collection' et une colonne 'prénom nom'")
                        else:
                            noms_ref = user_df[user_df["collection"].str.lower() == self.collection_a_chercher.lower()].iloc[:, 1].dropna().unique().tolist()
                            chercheur_map = {normalize_name(n): n for n in noms_ref}
                            initial_map = {get_initial_form(normalize_name(n)): n for n in noms_ref}
                            all_forms = {**chercheur_map, **initial_map}

                            def detect_known_authors(auteur_str):
                                if pd.isna(auteur_str):
                                    return ""
                                auteurs = [a.strip() for a in str(auteur_str).split(';') if a.strip()]
                                noms_detectes = []
                                for a in auteurs:
                                    norm = normalize_name(a)
                                    forme = get_initial_form(norm)
                                    match = get_close_matches(norm, all_forms.keys(), n=1, cutoff=0.8) or \
                                            get_close_matches(forme, all_forms.keys(), n=1, cutoff=0.8)
                                    if match:
                                        noms_detectes.append(all_forms[match[0]])
                                return "; ".join(noms_detectes)

                            self.merged_data['Auteurs fichier'] = self.merged_data['Auteurs'].apply(detect_known_authors)

            # Vérifier si merged_data n'est pas vide avant de générer le CSV
            if not self.merged_data.empty:
                # Générer le CSV à partir du DataFrame
                csv = self.merged_data.to_csv(index=False)

                # Créer un objet BytesIO pour stocker le CSV
                csv_bytes = io.BytesIO()
                csv_bytes.write(csv.encode('utf-8'))
                csv_bytes.seek(0)

                # Proposer le téléchargement du CSV
                st.download_button(
                    label="Télécharger le CSV",
                    data=csv_bytes,
                    file_name=f"{self.collection_a_chercher}_c2LabHAL.csv",
                    mime="text/csv"
                )

                # Mettre à jour la barre de progression à 100%
                self.progress_bar.progress(100)
                self.progress_text.text("Terminé !")

        else:
            st.error("Aucune donnée à exporter. Veuillez vérifier les paramètres de recherche.")