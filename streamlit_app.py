import streamlit as st
import pandas as pd
import io
import requests
from metapub import PubMedFetcher
import regex as re
from unidecode import unidecode
from langdetect import detect

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

# Fonction pour récupérer les données HAL
def safe_get_json(url):
    response = requests.get(url)
    if response.status_code == 200:
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"Erreur de décodage JSON pour l'URL : {url}")
            return None
    else:
        print(f"Erreur API : {response.status_code} pour l'URL : {url}")
        return {}

def get_hal_data(collection_a_chercher, start_year, end_year):
    endpoint = "http://api.archives-ouvertes.fr/search/"
    n = safe_get_json(f"{endpoint}{collection_a_chercher}/?q=*&fq=publicationDateY_i:[{start_year} TO {end_year}]&fl=doiId_s,title_s&rows=0&sort=docid asc&wt=json")
    n = n.get('response', {}).get('numFound', 0)
    print(f'publications trouvées : {n}')

    dois_coll = []
    titres_coll = []

    if n > 1000:
        current = 0
        cursor = ""
        next_cursor = "*"
        while cursor != next_cursor:
            print(f"\ren cours : {current}", end="\t")
            cursor = next_cursor
            page = safe_get_json(f"{endpoint}{collection_a_chercher}/?q=*&fq=publicationDateY_i:[{start_year} TO {end_year}]&fl=doiId_s,title_s&rows=1000&cursorMark={cursor}&sort=docid asc&wt=json")
            for d in page.get('response', {}).get('docs', []):
                for t in d.get('title_s', []):
                    titres_coll.append(t)
                dois_coll.append(d.get('doiId_s', '').lower())
            current += 1000
            next_cursor = page.get('nextCursorMark', '')
    else:
        page = safe_get_json(f"{endpoint}{collection_a_chercher}/?q=*&fq=publicationDateY_i:[{start_year} TO {end_year}]&fl=doiId_s,title_s&rows=1000&sort=docid asc&wt=json")
        for d in page.get('response', {}).get('docs', []):
            for t in d.get('title_s', []):
                titres_coll.append(t)
            dois_coll.append(d.get('doiId_s', '').lower())

    print(f"\rterminé : {n} publications chargées", end="\t")
    return dois_coll, titres_coll

def normalise(s):
    return re.sub(' +', ' ', unidecode(re.sub('\W', ' ', s))).lower()

def inex_match(nti, nti_coll):
    for x in nti_coll:
        if len(nti) * 1.1 > len(x) and len(nti) * 0.9 < len(x):
            if re.fullmatch(f"({nti}){{e<={int(len(x) / 20)}}}", x):
                return True
    return False

def statut_titre(ti, nti_coll):
    if isinstance(ti, str) and ti.endswith("]"):
        try:
            if detect(ti[:re.match(".*\\[", ti).span()[1]]) != detect(ti[re.match(".*\\[", ti).span()[1]:]):
                ti = ti[re.match(".*\\[", ti).span()[1]:]
        except:
            pass
    try:
        nti = normalise(ti)
    except TypeError:
        return "titre invalide"
    if nti in nti_coll:
        return "titre trouvé dans la collection : probablement déjà présent"
    elif inex_match(nti, nti_coll):
        return "titre approchant trouvé dans la collection : à vérifier"
    else:
        hal_result = safe_get_json(f"http://api.archives-ouvertes.fr/search/?q=title_t:{nti}&rows=0")
        if hal_result.get('response', {}).get('numFound', 0) > 0:
            return "titre trouvé dans HAL mais hors de la collection : affiliation probablement à corriger"
        else:
            return "hors HAL"

def statut_doi(do, dois_coll):
    if pd.isna(do):
        return "pas de DOI valide"
    ndo = do.replace("https://doi.org/", "").lower()
    if ndo in dois_coll:
        return "Dans la collection"
    else:
        hal_result = safe_get_json(f"http://api.archives-ouvertes.fr/search/?q=doiId_id:{ndo}&rows=0")
        if hal_result.get('response', {}).get('numFound', 0) > 0:
            return "Dans HAL mais hors de la collection"
        else:
            return "hors HAL"

# Fonction pour fusionner les lignes en gardant les valeurs identiques et en concaténant les valeurs différentes
def merge_rows_with_sources(group):
    # Conserver les IDs et les sources séparés par un |, et fusionner les autres champs
    merged_ids = '|'.join(group['id'])
    merged_sources = '|'.join(group['Data source'])

    # Initialiser une nouvelle ligne avec les valeurs de la première ligne
    first_row = group.iloc[0].copy()

    # Pour chaque colonne, vérifier si les valeurs sont identiques ou différentes
    for column in group.columns:
        if column not in ['id', 'Data source']:
            unique_values = group[column].unique()
            if len(unique_values) == 1:
                first_row[column] = unique_values[0]
            else:
                first_row[column] = '|'.join(map(str, unique_values))

    # Mettre à jour les IDs et les sources dans la nouvelle ligne
    first_row['id'] = merged_ids
    first_row['Data source'] = merged_sources

    return first_row

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
    collection_a_chercher = st.text_input("Collection HAL", value="GEPEA")

    if st.button("Télécharger le CSV"):
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
        combined_df = pd.concat([scopus_df, openalex_df, pubmed_df], ignore_index=True)

        # Récupérer les données HAL
        dois_coll, titres_coll = get_hal_data(collection_a_chercher, start_year, end_year)
        nti_coll = [normalise(x).strip() for x in titres_coll]

        # Ajouter la colonne "Statut"
        combined_df['Statut'] = ''

        for i, r in combined_df.iterrows():
            print(f"\rtraité : {i} sur {len(combined_df)}", end="\t\t")
            ret_doi = statut_doi(r.doi, dois_coll)
            if ret_doi in ["pas de DOI valide", "hors HAL"]:
                ret_ti = statut_titre(r.Title, nti_coll)
                combined_df.loc[i, 'Statut'] = ret_ti
            else:
                combined_df.loc[i, 'Statut'] = ret_doi

        # Fusionner les lignes en double
        merged_data = combined_df.groupby('doi', as_index=False).apply(merge_rows_with_sources)

        # Générer le CSV à partir du DataFrame
        csv = merged_data.to_csv(index=False)

        # Créer un objet BytesIO pour stocker le CSV
        csv_bytes = io.BytesIO()
        csv_bytes.write(csv.encode('utf-8'))
        csv_bytes.seek(0)

        # Proposer le téléchargement du CSV
        st.download_button(
            label="Télécharger le CSV",
            data=csv_bytes,
            file_name="results.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
