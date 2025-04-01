import os
import streamlit as st
import pandas as pd
import io
import requests
import json
from metapub import PubMedFetcher
import regex as re
from unidecode import unidecode
from langdetect import detect
from tqdm import tqdm

# Configurer tqdm pour pandas
tqdm.pandas()

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

        if 'entry' in resp.json()['search-results']:
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
    next_cursor = "*"

    while True:
        if next_cursor:
            params['cursor'] = next_cursor

        resp = requests.get(url, params=params)

        if resp.status_code != 200:
            raise Exception(f'OpenAlex API error {resp.status_code}, JSON dump: {resp.json()}')

        data = resp.json()
        JSON += data['results']
        next_cursor = data['meta'].get('next_cursor')

        # Sortir de la boucle si plus de pages ou max_items atteint
        if not next_cursor or len(JSON) >= max_items:
            break

    return JSON

# Fonction pour récupérer les données de PubMed
def get_pubmed_data(query, max_items=1000):
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
            'Auteurs': article.authors,
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
endpoint="http://api.archives-ouvertes.fr/search/"

escapeRules ={'+':r'\+','-':r'\-','&':r'\&','|':r'\|','!':r'\!','(':r'\(',')':r'\)','{':r'\{','}':r'\}','[':r'\[',
              ']':r'\]','^':r'\^','~':r'\~','*':r'\*','?':r'\?',':':r'\:','"':r'\"'}

hal_fl="docid,doiId_s,title_s,submitType_s"

default_start=2018
default_end='*'
def escapedSeq(term):
    """ Yield the next string based on the
    next character (either this char
    or escaped version """
    for char in term:
        if char in escapeRules.keys():
            yield escapeRules[char]
        else:
            yield char

def escapeSolrArg(term):
    """ Apply escaping to the passed in query terms
    escaping special characters like : , etc."""
    term = term.replace('\\',r'\\') # escape \ first
    return"".join([nextStr for nextStr in escapedSeq(term)])

def normalise(s):
    """Takes any string and returns it with only normal characters, single spaces and in lower case."""
    return re.sub(' +', ' ',unidecode(re.sub(r'\W',' ', s))).lower()

def compare_inex(nti,cti):
    """Takes a normalised title from the list to be compared, and compares it with the title from the list extracted from HAL.
    Returns True if the titles have comparable lengths and 90% character similarity (~Lehvenstein distance)"""
    nti=normalise(nti).strip()
    if len(nti)*1.1 > len(cti) > len(nti)*0.9:
        if len(cti) > 50:
            if re.fullmatch("("+nti[:50]+"){e<=5}",cti[:50]):
                return cti if  re.fullmatch("("+nti+"){"+f"e<={int(len(cti)/10)}"+"}",cti) else False
        else:
            return cti if  re.fullmatch("("+nti+"){"+f"e<={int(len(cti)/10)}"+"}",cti) else False
    return False

def ex_in_coll(ti,coll_df):
    """Takes a title from the list to be compared. If it is in the list of titles from the compared HAL collection,
    returns the corresponding HAL reference. Else, returns False."""
    try:
        return ["Titre trouvé dans la collection : probablement déjà présent",
                ti,
                coll_df[coll_df['Titres']==ti].iloc[0,0],
                coll_df[coll_df['Titres']==ti].iloc[0,3]]
    except IndexError:
        return False

def inex_in_coll(nti,coll_df):
    """Takes a title from the list to be compared. If it has at least 90% similarity with one of the titles from the compared HAL collection,
    returns the corresponding HAL reference. Else, returns False."""
    for x in list(coll_df['nti']):
        y = compare_inex(nti,x)
        if y:
            return ["Titre approchant trouvé dans la collection : à vérifier",
                    coll_df[coll_df['nti']==y].iloc[0,2],
                    coll_df[coll_df['nti']==y].iloc[0,0],
                    coll_df[coll_df['nti']==y].iloc[0,3]]
    return False

def in_hal(nti,ti):
    """Tries to find a title in HAL, first with a strict character match then if not found with a loose SolR search"""
    try:
        r_ex=requests.get(f"{endpoint}?q=title_t:{nti}&rows=1&fl={hal_fl}").json()['response']
        if r_ex['numFound'] >0:
            if any(ti==x for x in r_ex['docs'][0]['title_s']):
                return ["Titre trouvé dans HAL mais hors de la collection : affiliation probablement à corriger",
                        r_ex['docs'][0]['title_s'][0],
                        r_ex['docs'][0]['docid'],
                        r_ex['docs'][0]['submitType_s']]
    except KeyError:
        r_inex=requests.get(f"{endpoint}?q=title_t:{ti}&rows=1&fl={hal_fl}").json()['response']
        if r_inex['numFound'] >0:
            return ["Titre approchant trouvé dans HAL mais hors de la collection : vérifier les affiliations",
                    r_inex['response']['docs'][0]['title_s'][0],
                    r_inex['response']['docs'][0]['docid'],
                    r_ex['docs'][0]['submitType_s']] if any(
                        compare_inex(ti,x) for x in [r_inex['response']['docs'][0]['title_s']]
                        ) else ["Hors HAL","","",""]
    return ["Hors HAL","","",""]

def statut_titre(title,coll_df):
    """Applies the matching process to a title, from searching it exactly in the HAL collection to be compared, to searching it loosely in HAL search API."""
    try:
        if title[len(title)-1]=="]" and detect(title[:re.match(r".*\[",ti).span()[1]]) != detect(title[re.match(r".*\[",title).span()[1]:]):
            title=title[re.match(r".*\[",title).span()[1]:]
        elif detect(title[:len(title)/2]) != detect(title[len(title)/2:]):
            title=title[:len(title)/2]
        else: title=title
    except:
        title=title
    try:
        ti='\"'+escapeSolrArg(title)+'\"'
    except TypeError:
        return ["Titre invalide","","",""]
    try:
        c_ex=ex_in_coll(title,coll_df)
        if c_ex:
            return c_ex
        else:
            c_inex = inex_in_coll(title,coll_df)
            if c_inex:
                return c_inex
            else:
                r_ex=in_hal(ti,title)
                return r_ex
    except KeyError:
        return ["Titre incorrect, probablement absent de HAL","","",""]

def statut_doi(do,coll_df):
    """applies the matching process to a DOI, searching it in the collection to be compared then in all of HAL"""
    dois_coll=coll_df['DOIs'].tolist()
    if do and do == do:
        ldo=do.lower()
        ndo=escapeSolrArg(re.sub(r"\[.*\]","",do.replace("https://doi.org/","").lower()))
        if ldo in dois_coll:
            return ["Dans la collection",
                    coll_df[coll_df['DOIs']==ldo].iloc[0,2],
                    coll_df[coll_df['DOIs']==ldo].iloc[0,0],
                    coll_df[coll_df['DOIs']==ldo].iloc[0,3]]
        else:
            r=requests.get(f"{endpoint}?q=doiId_id:{ndo}&rows=1&fl={hal_fl}").json()
            if r['response']['numFound'] >0:
                return ["Dans HAL mais hors de la collection",
                        r['response']['docs'][0]['title_s'][0],
                        r['response']['docs'][0]['docid'],
                        r['response']['docs'][0]['submitType_s']]
            return ["Hors HAL","","",""]
    elif do != do:
        return ["Pas de DOI valide","","",""]

def check_df(df, coll_df):
    """Applies the full process to the dataframe or table given as an input."""
    results = df.progress_apply(
        lambda x: statut_doi(x['doi'], coll_df) if statut_doi(x['doi'], coll_df) and statut_doi(x['doi'], coll_df)[0] in ("Dans la collection", "Dans HAL mais hors de la collection") else statut_titre(x['Title'], coll_df), axis=1
    )
    df['Statut'] = results.apply(lambda x: x[0])
    df['titre_si_trouvé'] = results.apply(lambda x: x[1])
    df['identifiant_hal_si_trouvé'] = results.apply(lambda x: x[2])
    df['statut_dépôt_si_trouvé'] = results.apply(lambda x: x[3])

class HalCollImporter:

    def __init__(self,coll_code:str,start_year=None,end_year=None):
        self.coll_code=coll_code
        self.start_year=start_year if start_year != None else default_start
        self.end_year=end_year if end_year != None else default_end
        self.nbdocs=self.get_nb_docs()

    def get_nb_docs(self):
        n=requests.get(f"{endpoint}{self.coll_code}/?q=*&fq=publicationDateY_i:[{self.start_year} TO {self.end_year}]&fl=docid&rows=0&sort=docid asc&wt=json").json()['response']['numFound']
        return n

    def import_data(self):
        n=self.nbdocs
        docid_coll=list()
        dois_coll=list()
        titres_coll=list()
        submit_types_coll=list()
        if n>1000:
            current=0
            cursor=""
            next_cursor="*"
            while cursor != next_cursor:
                cursor=next_cursor
                page=requests.get(f"{endpoint}{self.coll_code}/?q=*&fq=publicationDateY_i:[{self.start_year} TO {self.end_year}]&fl={hal_fl}&rows=1000&cursorMark={cursor}&sort=docid asc&wt=json").json()
                for d in page['response']['docs']:
                    for t in d['title_s']:
                        titres_coll.append(t)
                        docid_coll.append(d['docid'])
                        try:
                            dois_coll.append(d['doiId_s'].lower())
                        except KeyError:
                            dois_coll.append("")
                        submit_types_coll.append(d['submitType_s'])
                current+=1000
                next_cursor=page['nextCursorMark']
        else:
            for d in requests.get(f"{endpoint}{self.coll_code}/?q=*&fq=publicationDateY_i:[{self.start_year} TO {self.end_year}]&fl={hal_fl}&rows=1000&sort=docid asc&wt=json").json()['response']['docs']:
                for t in d['title_s']:
                    titres_coll.append(t)
                    docid_coll.append(d['docid'])
                    try:
                        dois_coll.append(d['doiId_s'].lower())
                    except KeyError:
                        dois_coll.append("")
                    submit_types_coll.append(d['submitType_s'])
        return pd.DataFrame({'Hal_ids':docid_coll,'DOIs':dois_coll,'Titres':titres_coll, 'Types de dépôts':submit_types_coll})

# Fonction pour fusionner les lignes en gardant les valeurs identiques et en concaténant les valeurs différentes
def merge_rows_with_sources(group):
    # Conserver les IDs et les sources séparés par un |, et fusionner les autres champs
    if 'id' in group.columns:
        merged_ids = '|'.join(map(str, group['id'].dropna()))
    else:
        merged_ids = None

    merged_sources = '|'.join(group['Data source'].dropna())

    # Initialiser une nouvelle ligne avec les valeurs de la première ligne
    first_row = group.iloc[0].copy()

    # Pour chaque colonne, vérifier si les valeurs sont identiques ou différentes
    for column in group.columns:
        if column not in ['id', 'Data source']:
            unique_values = group[column].dropna().apply(lambda x: str(x) if isinstance(x, list) else x).unique()
            if len(unique_values) == 1:
                first_row[column] = unique_values[0]
            else:
                first_row[column] = '|'.join(map(str, unique_values))

    # Mettre à jour les IDs et les sources dans la nouvelle ligne
    first_row['id'] = merged_ids
    first_row['Data source'] = merged_sources

    return first_row

# Fonction pour récupérer les auteurs à partir de Crossref
def get_authors_from_crossref(doi):
    url = f"https://api.crossref.org/works/{doi}"
    response = requests.get(url)

    if response.status_code != 200:
        return []

    data = response.json()
    authors = data.get('message', {}).get('author', [])
    author_names = [author.get('given', '') + ' ' + author.get('family', '') for author in authors]

    return author_names

# Fonction principale
def main():
    st.title("🥎 c2LabHAL")
    st.subheader("Comparez les publications d'un labo dans Scopus, OpenAlex et Pubmed avec sa collection HAL")

    # Saisie des paramètres
    collection_a_chercher = st.text_input(
        "Collection HAL",
        value="",
        key="collection_hal",
        help="Saisissez le nom de la collection HAL du laboratoire, par exemple MIP"
    )

    openalex_institution_id = st.text_input("Identifiant OpenAlex du labo", help="Saisissez l'identifiant du labo dans OpenAlex, par exemple i4392021216")

    col1, col2 = st.columns(2)
    with col1:
        pubmed_id = st.text_input("Requête PubMed", help="Saisissez la requête Pubmed qui rassemble le mieux les publications du labo, par exemple ((MIP[Affiliation]) AND ((mans[Affiliation]) OR (nantes[Affiliation]))) OR (EA 4334[Affiliation]) OR (EA4334[Affiliation]) OR (UR 4334[Affiliation]) OR (UR4334[Affiliation]) OR (Movement Interactions Performance[Affiliation] OR (Motricité Interactions Performance[Affiliation]) OR (mouvement interactions performance[Affiliation])")
    with col2:
        pubmed_api_key = st.text_input("Clé API Pubmed", help="Pour obtenir une clé API, connectez vous sur Pubmed, cliquez sur Account, Account Settings, API Key Management.")

    col1, col2 = st.columns(2)
    with col1:
        scopus_lab_id = st.text_input("Identifiant Scopus du labo", help="Saisissez le Scopus Affiliation Identifier du laboratoire, par exemple 60105638")
    with col2:
        scopus_api_key = st.text_input("Clé API Scopus", help="Pour obtenir une clé API : https://dev.elsevier.com/. Sinon, contactez la personne en charge de la bibliométrie dans votre établissement")

    start_year = st.number_input("Année de début", min_value=1900, max_value=2100, value=2020)
    end_year = st.number_input("Année de fin", min_value=1900, max_value=2100, value=2025)
    fetch_authors = st.checkbox("Récupérer les auteurs sur Crossref", value=True)

    # Initialiser la barre de progression
    progress_bar = st.progress(0)
    progress_text = st.empty()

    if st.button("Rechercher"):
        # Configurer la clé API PubMed si elle est fournie
        if pubmed_api_key:
            os.environ['NCBI_API_KEY'] = pubmed_api_key

        # Initialiser des DataFrames vides
        scopus_df = pd.DataFrame()
        openalex_df = pd.DataFrame()
        pubmed_df = pd.DataFrame()

        # Étape 1 : Récupération des données OpenAlex
        with st.spinner("OpenAlex"):
            progress_text.text("Étape 1 : Récupération des données OpenAlex")
            progress_bar.progress(10)
            if openalex_institution_id:
                openalex_query = f"institutions.id:{openalex_institution_id},publication_year:{start_year}-{end_year}"
                openalex_data = get_openalex_data(openalex_query)
                openalex_df = convert_to_dataframe(openalex_data, 'openalex')
                openalex_df['Source title'] = openalex_df.apply(
                    lambda row: row['primary_location']['source']['display_name'] if row['primary_location'] and row['primary_location'].get('source') else None, axis=1
                )
                openalex_df['Date'] = openalex_df.apply(
                    lambda row: row.get('publication_date', None), axis=1
                )
                openalex_df['doi'] = openalex_df.apply(
                    lambda row: row.get('doi', None), axis=1
                )
                openalex_df['id'] = openalex_df.apply(
                    lambda row: row.get('id', None), axis=1
                )
                openalex_df['title'] = openalex_df.apply(
                    lambda row: row.get('title', None), axis=1
                )
                openalex_df = openalex_df[['source', 'title', 'doi', 'id', 'Source title', 'Date']]
                openalex_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                openalex_df['doi'] = openalex_df['doi'].apply(clean_doi)

        # Étape 2 : Récupération des données PubMed
        with st.spinner("Pubmed"):
            progress_text.text("Étape 2 : Récupération des données PubMed")
            progress_bar.progress(30)
            if pubmed_id:
                pubmed_query = f"{pubmed_id} AND {start_year}/01/01:{end_year}/12/31[Date - Publication]"
                pubmed_data = get_pubmed_data(pubmed_query)
                pubmed_df = pd.DataFrame(pubmed_data)

        # Étape 3 : Récupération des données Scopus
        with st.spinner("Scopus"):
            progress_text.text("Étape 3 : Récupération des données Scopus")
            progress_bar.progress(50)
            if scopus_api_key and scopus_lab_id:
                scopus_query = f"af-ID({scopus_lab_id}) AND PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1}"
                scopus_data = get_scopus_data(scopus_api_key, scopus_query)
                scopus_df = convert_to_dataframe(scopus_data, 'scopus')

                scopus_df = scopus_df[['source', 'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']]
                scopus_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']

        # Étape 4 : Comparaison avec HAL (si le champ "Collection HAL" n'est pas vide)
        if collection_a_chercher:
            with st.spinner("HAL"):
                progress_text.text("Étape 4 : Comparaison avec HAL")
                progress_bar.progress(70)
                # Combiner les DataFrames
                combined_df = pd.concat([scopus_df, openalex_df, pubmed_df], ignore_index=True)

                # Récupérer les données HAL
                coll=HalCollImporter(collection_a_chercher,start_year,end_year)
                coll_df=coll.import_data()
                coll_df['nti']=coll_df['Titres'].apply(lambda x : normalise(x).strip())
                check_df(combined_df,coll_df)
        else:
            combined_df = pd.concat([scopus_df, openalex_df, pubmed_df], ignore_index=True)

        # Étape 5 : Fusion des lignes en double
        with st.spinner("Fusion"):
            progress_text.text("Étape 5 : Fusion des lignes en double")
            progress_bar.progress(90)
            merged_data = combined_df.groupby('doi', as_index=False).apply(merge_rows_with_sources)

        # Étape 6 : Ajout des auteurs à partir de Crossref (si la case est cochée)
        if fetch_authors:
            with st.spinner("Auteurs Crossref"):
                progress_text.text("Étape 6 : Ajout des auteurs")
                progress_bar.progress(95)
                merged_data['Auteurs'] = merged_data['doi'].apply(lambda doi: '; '.join(get_authors_from_crossref(doi)) if doi else '')

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

        # Mettre à jour la barre de progression à 100%
        progress_bar.progress(100)
        progress_text.text("Terminé !")

if __name__ == "__main__":
    main()
