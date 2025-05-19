import streamlit as st # Utilisé pour st.error, st.warning, st.info dans certaines fonctions
import pandas as pd
import requests
import json
from metapub import PubMedFetcher
import regex as re
from unidecode import unidecode
import unicodedata
from difflib import get_close_matches
from langdetect import detect
from tqdm import tqdm # Utilisé pour les barres de progression, notamment avec pandas
from concurrent.futures import ThreadPoolExecutor

# Configurer tqdm pour pandas, cela affecte le comportement global de pandas avec tqdm
tqdm.pandas()

# --- Constantes Partagées ---
HAL_API_ENDPOINT = "http://api.archives-ouvertes.fr/search/"
HAL_FIELDS_TO_FETCH = "docid,doiId_s,title_s,submitType_s,linkExtUrl_s,linkExtId_s"
DEFAULT_START_YEAR = 2018
DEFAULT_END_YEAR = '*' # Pour Solr, '*' signifie jusqu'à la fin/plus récent

# Règles d'échappement pour les requêtes Solr
SOLR_ESCAPE_RULES = {
    '+': r'\+', '-': r'\-', '&': r'\&', '|': r'\|', '!': r'\!', '(': r'\(',
    ')': r'\)', '{': r'\{', '}': r'\}', '[': r'\[', ']': r'\]', '^': r'\^',
    '~': r'\~', '*': r'\*', '?': r'\?', ':': r'\:', '"': r'\"'
}

# --- Fonctions Utilitaires ---

def get_scopus_data(api_key, query, max_items=2000):
    """
    Récupère les données de Scopus en fonction d'une requête.
    Args:
        api_key (str): Clé API pour Scopus.
        query (str): Requête de recherche Scopus.
        max_items (int): Nombre maximum d'éléments à récupérer.
    Returns:
        list: Liste des entrées Scopus trouvées, ou liste vide en cas d'erreur.
    """
    found_items_num = -1 # Initialiser pour que la première récupération de totalResults se fasse
    start_item = 0
    items_per_query = 25 # Limite de Scopus par requête
    results_json = []
    processed_items = 0

    while True:
        if found_items_num != -1 and (processed_items >= found_items_num or processed_items >= max_items) :
            break # Sortir si tous les items ont été récupérés ou si max_items est atteint

        try:
            resp = requests.get(
                'https://api.elsevier.com/content/search/scopus',
                headers={'Accept': 'application/json', 'X-ELS-APIKey': api_key},
                params={'query': query, 'count': items_per_query, 'start': start_item},
                timeout=30 # Timeout pour la requête
            )
            resp.raise_for_status()  # Lève une exception pour les codes d'état HTTP 4xx/5xx
            data = resp.json()
        except requests.exceptions.RequestException as e:
            st.error(f"Erreur lors de la requête Scopus (start_item: {start_item}): {e}")
            return results_json # Retourner ce qui a été collecté jusqu'à présent

        search_results = data.get('search-results', {})
        
        if found_items_num == -1: # Première requête, récupérer le total
            try:
                found_items_num = int(search_results.get('opensearch:totalResults', 0))
                if found_items_num == 0:
                    st.info("Aucun résultat trouvé sur Scopus pour cette requête.")
                    return []
            except (ValueError, TypeError):
                st.error("Réponse inattendue de Scopus (totalResults non trouvé ou invalide).")
                return []
        
        entries = search_results.get('entry')
        if entries:
            results_json.extend(entries)
            processed_items += len(entries)
        else: # Plus d'entrées ou fin des résultats paginés
            if found_items_num > 0 and not entries and start_item < found_items_num :
                 st.warning(f"Scopus: {found_items_num} résultats attendus, mais 'entry' est vide à start_item {start_item}. Arrêt.")
            break 

        start_item += items_per_query
        
        if not entries and start_item > found_items_num : # Double sécurité pour sortir
            break

    return results_json[:max_items] # S'assurer de ne pas dépasser max_items

def get_openalex_data(query, max_items=2000):
    """
    Récupère les données d'OpenAlex en fonction d'une requête.
    Args:
        query (str): Requête de filtre OpenAlex.
        max_items (int): Nombre maximum d'éléments à récupérer.
    Returns:
        list: Liste des travaux OpenAlex trouvés, ou liste vide en cas d'erreur.
    """
    url = 'https://api.openalex.org/works'
    # OpenAlex recommande d'inclure un email dans les requêtes pour le pool poli
    email = "hal.dbm@listes.u-paris.fr" # Remplacez par un email de contact approprié
    params = {'filter': query, 'per-page': 200, 'mailto': email} # Max per-page pour OpenAlex est 200
    results_json = []
    next_cursor = "*" # Initialisation correcte du curseur pour la première page

    retries = 3 # Nombre de tentatives en cas d'erreur
    
    while len(results_json) < max_items:
        current_try = 0
        if not next_cursor: # Plus de pages à charger
            break
        
        params['cursor'] = next_cursor

        while current_try < retries:
            try:
                resp = requests.get(url, params=params, timeout=30) # Ajout d'un timeout
                resp.raise_for_status() # Lève une exception pour les erreurs HTTP
                data = resp.json()
                
                if 'results' in data:
                    results_json.extend(data['results'])
                
                next_cursor = data.get('meta', {}).get('next_cursor')
                break # Sortir de la boucle de tentatives si succès
            
            except requests.exceptions.RequestException as e:
                current_try += 1
                st.warning(f"Erreur OpenAlex (tentative {current_try}/{retries}): {e}. Réessai...")
                if current_try >= retries:
                    st.error(f"Échec de la récupération des données OpenAlex après {retries} tentatives.")
                    return results_json[:max_items] # Retourner ce qui a été collecté
            except json.JSONDecodeError:
                current_try +=1
                st.warning(f"Erreur de décodage JSON OpenAlex (tentative {current_try}/{retries}). Réessai...")
                if current_try >= retries:
                    st.error("Échec du décodage JSON OpenAlex.")
                    return results_json[:max_items]
        
        if current_try >= retries: # Si toutes les tentatives ont échoué
            break
            
    return results_json[:max_items] # S'assurer de ne pas dépasser max_items


def get_pubmed_data(query, max_items=1000):
    """
    Récupère les données de PubMed pour une requête donnée.
    Args:
        query (str): Requête PubMed.
        max_items (int): Nombre maximum d'articles à récupérer.
    Returns:
        list: Liste de dictionnaires, chaque dictionnaire représentant un article.
    """
    fetch = PubMedFetcher()
    data = []
    try:
        # Récupérer les PMIDs pour la requête
        pmids = fetch.pmids_for_query(query, retmax=max_items)
        
        # Pour chaque PMID, récupérer les détails de l'article
        for pmid in tqdm(pmids, desc="Récupération des articles PubMed"):
            try:
                article = fetch.article_by_pmid(pmid)
                # Extraire la date de publication PubMed (si disponible)
                pub_date_obj = article.history.get('pubmed') if article.history else None
                pub_date_str = pub_date_obj.date().isoformat() if pub_date_obj and hasattr(pub_date_obj, 'date') else 'N/A'
                
                data.append({
                    'Data source': 'pubmed',
                    'Title': article.title if article.title else "N/A",
                    'doi': article.doi if article.doi else None,
                    'id': pmid, # L'ID PubMed est le PMID lui-même
                    'Source title': article.journal if article.journal else "N/A", # Nom de la revue
                    'Date': pub_date_str
                })
            except Exception as e_article:
                st.warning(f"Erreur lors de la récupération des détails pour l'article PubMed (PMID: {pmid}): {e_article}")
                # Ajouter une entrée partielle même en cas d'erreur pour ne pas perdre le PMID
                data.append({
                    'Data source': 'pubmed', 'Title': "Erreur de récupération", 'doi': None,
                    'id': pmid, 'Source title': "N/A", 'Date': "N/A"
                })
        return data
    except Exception as e_query:
        st.error(f"Erreur lors de la requête PMIDs à PubMed: {e_query}")
        return [] # Retourner une liste vide en cas d'erreur majeure

def convert_to_dataframe(data, source_name):
    """
    Convertit une liste de dictionnaires en DataFrame pandas et ajoute une colonne 'source'.
    Args:
        data (list): Liste de dictionnaires.
        source_name (str): Nom de la source de données (ex: 'scopus', 'openalex').
    Returns:
        pd.DataFrame: DataFrame avec les données et la colonne 'source'.
    """
    if not data: # Si la liste est vide
        return pd.DataFrame() # Retourner un DataFrame vide pour éviter les erreurs
    df = pd.DataFrame(data)
    df['Data source'] = source_name # Nommer la colonne 'Data source' pour la cohérence
    return df

def clean_doi(doi_value):
    """
    Nettoie un DOI en retirant le préfixe 'https://doi.org/'.
    Args:
        doi_value (str or any): La valeur du DOI.
    Returns:
        str or any: Le DOI nettoyé, ou la valeur originale si non applicable.
    """
    if isinstance(doi_value, str):
        doi_value = doi_value.strip() # Enlever les espaces avant/après
        if doi_value.startswith('https://doi.org/'):
            return doi_value[len('https://doi.org/'):]
    return doi_value


def escapedSeq(term_char_list):
    """ Générateur pour échapper les caractères Solr. """
    for char in term_char_list:
        yield SOLR_ESCAPE_RULES.get(char, char)

def escapeSolrArg(term_to_escape):
    """
    Échappe les caractères spéciaux Solr dans un terme de requête.
    Args:
        term_to_escape (str): Le terme à échapper.
    Returns:
        str: Le terme avec les caractères spéciaux échappés.
    """
    if not isinstance(term_to_escape, str):
        return "" # Retourner une chaîne vide si l'entrée n'est pas une chaîne
    # Échapper l'antislash d'abord, car il est utilisé dans les séquences d'échappement
    term_escaped = term_to_escape.replace('\\', r'\\')
    return "".join(list(escapedSeq(term_escaped)))


def normalise(text_to_normalise):
    """
    Normalise une chaîne de caractères : suppression des accents, conversion en minuscules,
    remplacement des caractères non alphanumériques par des espaces, et suppression des espaces multiples.
    Args:
        text_to_normalise (str): La chaîne à normaliser.
    Returns:
        str: La chaîne normalisée.
    """
    if not isinstance(text_to_normalise, str):
        return "" # Retourner une chaîne vide si l'entrée n'est pas une chaîne
    # Supprimer les accents et convertir en ASCII approchant
    text_unaccented = unidecode(text_to_normalise)
    # Remplacer les caractères non alphanumériques (sauf espaces) par un espace
    text_alphanum_spaces = re.sub(r'[^\w\s]', ' ', text_unaccented)
    # Convertir en minuscules et supprimer les espaces multiples et en début/fin
    text_normalised = re.sub(r'\s+', ' ', text_alphanum_spaces).lower().strip()
    return text_normalised

def compare_inex(norm_title1, norm_title2, threshold_strict=0.9, threshold_short=0.85, short_len_def=20):
    """
    Compare deux titres normalisés pour évaluer leur similarité.
    Utilise get_close_matches de difflib.
    Args:
        norm_title1 (str): Premier titre normalisé.
        norm_title2 (str): Deuxième titre normalisé.
        threshold_strict (float): Seuil de similarité pour les titres plus longs.
        threshold_short (float): Seuil de similarité pour les titres courts.
        short_len_def (int): Longueur définissant un titre comme "court".
    Returns:
        bool: True si les titres sont considérés similaires, False sinon.
    """
    if not norm_title1 or not norm_title2: # Gérer les chaînes vides
        return False
    
    # Ajuster le seuil en fonction de la longueur du titre le plus court
    # (pour être plus indulgent avec les titres très courts où une petite différence pèse lourd)
    shorter_len = min(len(norm_title1), len(norm_title2))
    current_threshold = threshold_strict if shorter_len > short_len_def else threshold_short
    
    # Comparaison de longueur (optionnelle, get_close_matches gère déjà les différences)
    # if not (len(norm_title1) * 1.25 > len(norm_title2) > len(norm_title1) * 0.75): # Fenêtre de longueur un peu plus large
    #     return False
        
    matches = get_close_matches(norm_title1, [norm_title2], n=1, cutoff=current_threshold)
    return bool(matches)


def ex_in_coll(original_title_to_check, collection_df):
    """
    Vérifie si un titre (original, non normalisé) existe exactement dans la colonne 'Titres' du DataFrame de la collection.
    Args:
        original_title_to_check (str): Le titre original à rechercher.
        collection_df (pd.DataFrame): DataFrame de la collection HAL (doit contenir 'Titres', 'Hal_ids', etc.).
    Returns:
        list or False: Liste avec le statut et les infos HAL si trouvé, sinon False.
    """
    if 'Titres' not in collection_df.columns or collection_df.empty:
        return False
    
    # Filtrer pour trouver une correspondance exacte du titre original
    match_df = collection_df[collection_df['Titres'] == original_title_to_check]
    if not match_df.empty:
        row = match_df.iloc[0]
        return [
            "Titre trouvé dans la collection : probablement déjà présent",
            original_title_to_check, # Retourner le titre original qui a matché
            row.get('Hal_ids', ''),
            row.get('Types de dépôts', ''),
            row.get('HAL Link', ''),
            row.get('HAL Ext ID', '')
        ]
    return False

def inex_in_coll(normalised_title_to_check, original_title, collection_df):
    """
    Vérifie si un titre normalisé a une correspondance approchante dans la colonne 'nti' (titres normalisés)
    du DataFrame de la collection.
    Args:
        normalised_title_to_check (str): Le titre normalisé à rechercher.
        original_title (str): Le titre original (pour information si une correspondance est trouvée).
        collection_df (pd.DataFrame): DataFrame de la collection HAL (doit contenir 'nti', 'Titres', 'Hal_ids', etc.).
    Returns:
        list or False: Liste avec le statut et les infos HAL si trouvé, sinon False.
    """
    if 'nti' not in collection_df.columns or collection_df.empty:
        return False
        
    # Parcourir les titres normalisés de la collection
    for idx, hal_title_norm_from_coll in enumerate(collection_df['nti']):
        if compare_inex(normalised_title_to_check, hal_title_norm_from_coll): # Comparer les titres normalisés
            row = collection_df.iloc[idx]
            return [
                "Titre approchant trouvé dans la collection : à vérifier",
                row.get('Titres', ''), # Retourner le titre original de HAL pour cette correspondance
                row.get('Hal_ids', ''),
                row.get('Types de dépôts', ''),
                row.get('HAL Link', ''),
                row.get('HAL Ext ID', '')
            ]
    return False


def in_hal(title_solr_escaped_exact, original_title_to_check):
    """
    Recherche un titre dans l'ensemble de HAL, d'abord de manière exacte, puis approchante.
    Args:
        title_solr_escaped_exact (str): Titre original, échappé pour une recherche exacte dans Solr (ex: "titre exact").
        original_title_to_check (str): Le titre original (non normalisé, non échappé) pour la recherche approchante et la comparaison.
    Returns:
        list: Liste avec le statut et les infos HAL si trouvé, ou statut "Hors HAL".
    """
    try:
        # 1. Recherche exacte du titre (utilisant le titre original échappé et entre guillemets pour Solr)
        # Solr gère la tokenisation, donc une recherche title_t:"mon titre" est généralement exacte.
        query_exact = f'title_t:({title_solr_escaped_exact})' # title_solr_escaped_exact devrait déjà être "mon titre"
        
        r_exact_req = requests.get(f"{HAL_API_ENDPOINT}?q={query_exact}&rows=1&fl={HAL_FIELDS_TO_FETCH}", timeout=10)
        r_exact_req.raise_for_status()
        r_exact_json = r_exact_req.json()
        
        if r_exact_json.get('response', {}).get('numFound', 0) > 0:
            doc_exact = r_exact_json['response']['docs'][0]
            # Vérifier si l'un des titres retournés correspond exactement au titre original (sensible à la casse et ponctuation)
            if any(original_title_to_check == hal_title for hal_title in doc_exact.get('title_s', [])):
                return [
                    "Titre trouvé dans HAL mais hors de la collection : affiliation probablement à corriger",
                    doc_exact.get('title_s', [""])[0],
                    doc_exact.get('docid', ''),
                    doc_exact.get('submitType_s', ''),
                    doc_exact.get('linkExtUrl_s', ''),
                    doc_exact.get('linkExtId_s', '')
                ]

        # 2. Si non trouvé exactement, recherche approchante (Solr est assez bon pour ça avec le titre original non échappé)
        # Utiliser le titre original, Solr gère une certaine flexibilité. Échapper les caractères spéciaux pour la requête.
        query_approx = f'title_t:({escapeSolrArg(original_title_to_check)})'

        r_approx_req = requests.get(f"{HAL_API_ENDPOINT}?q={query_approx}&rows=1&fl={HAL_FIELDS_TO_FETCH}", timeout=10)
        r_approx_req.raise_for_status()
        r_approx_json = r_approx_req.json()

        if r_approx_json.get('response', {}).get('numFound', 0) > 0:
            doc_approx = r_approx_json['response']['docs'][0]
            # Comparer le titre original (normalisé) avec les titres retournés (normalisés)
            title_orig_norm = normalise(original_title_to_check)
            if any(compare_inex(title_orig_norm, normalise(hal_title)) for hal_title in doc_approx.get('title_s', [])):
                return [
                    "Titre approchant trouvé dans HAL mais hors de la collection : vérifier les affiliations",
                    doc_approx.get('title_s', [""])[0],
                    doc_approx.get('docid', ''),
                    doc_approx.get('submitType_s', ''),
                    doc_approx.get('linkExtUrl_s', ''),
                    doc_approx.get('linkExtId_s', '')
                ]
    except requests.exceptions.RequestException as e:
        st.warning(f"Erreur de requête à l'API HAL pour le titre '{original_title_to_check}': {e}")
    except (KeyError, IndexError, json.JSONDecodeError) as e_json: # Gérer les erreurs de structure JSON ou de décodage
        st.warning(f"Structure de réponse HAL inattendue ou erreur JSON pour le titre '{original_title_to_check}': {e_json}")
    
    return ["Hors HAL", original_title_to_check, "", "", "", ""] # Retourner le titre original si non trouvé


def statut_titre(title_to_check, collection_df):
    """
    Détermine le statut d'un titre par rapport à une collection HAL et à l'ensemble de HAL.
    Args:
        title_to_check (str): Le titre à vérifier.
        collection_df (pd.DataFrame): DataFrame de la collection HAL.
    Returns:
        list: Statut et informations associées.
    """
    if not isinstance(title_to_check, str) or not title_to_check.strip():
        return ["Titre invalide", "", "", "", "", ""]

    original_title = title_to_check # Conserver le titre original pour l'affichage et certaines recherches

    # Tentative de nettoyage des titres avec traductions (ex: "[Titre traduit]")
    # Cette logique peut être affinée ou rendue optionnelle
    processed_title_for_norm = original_title
    try:
        # Si le titre se termine par ']' et contient une traduction détectable
        if original_title.endswith("]") and '[' in original_title:
            match_bracket = re.match(r"(.*)\[", original_title) # Capturer la partie avant le crochet
            if match_bracket:
                part_before_bracket = match_bracket.group(1).strip()
                # Optionnel: vérifier si la partie entre crochets est une langue différente
                # Pour simplifier, on peut juste prendre la partie avant le crochet
                if part_before_bracket : # S'assurer qu'il y a quelque chose avant le crochet
                    processed_title_for_norm = part_before_bracket
        # Optionnel: gérer les cas où deux langues sont concaténées sans crochet (plus complexe)
        # Exemple simple : si les deux moitiés du titre sont dans des langues différentes
        # elif len(original_title) > 30: # Éviter pour les titres très courts
        #     mid_point = len(original_title) // 2
        #     part1 = original_title[:mid_point].strip()
        #     part2 = original_title[mid_point:].strip()
        #     if part1 and part2 and detect(part1) != detect(part2):
        #         processed_title_for_norm = part1 
    except Exception: # Ignorer les erreurs de détection de langue ou de regex
        processed_title_for_norm = original_title # Revenir au titre original en cas d'erreur de traitement

    title_normalised = normalise(processed_title_for_norm) # Titre normalisé pour la comparaison approchante
    # Pour la recherche exacte dans Solr, utiliser le titre original, échappé et entre guillemets
    title_solr_exact_query_str = '\"' + escapeSolrArg(original_title) + '\"'


    # 1. Recherche exacte du titre original dans la collection
    res_ex_coll = ex_in_coll(original_title, collection_df)
    if res_ex_coll:
        return res_ex_coll

    # 2. Recherche approchante (basée sur le titre normalisé) dans la collection
    res_inex_coll = inex_in_coll(title_normalised, original_title, collection_df)
    if res_inex_coll:
        return res_inex_coll
        
    # 3. Recherche dans tout HAL (exacte avec titre original, puis approchante)
    # in_hal s'attend au titre original échappé pour l'exact, et au titre original pour l'approchant
    res_hal_global = in_hal(escapeSolrArg(original_title), original_title) # Simplifié : Solr gère bien les guillemets via escapeSolrArg si besoin.
                                                                            # Ou passer title_solr_exact_query_str pour la partie exacte.
                                                                            # La fonction in_hal a été modifiée pour gérer cela.
    return res_hal_global


def statut_doi(doi_to_check, collection_df):
    """
    Détermine le statut d'un DOI par rapport à une collection HAL et à l'ensemble de HAL.
    Args:
        doi_to_check (str): Le DOI à vérifier.
        collection_df (pd.DataFrame): DataFrame de la collection HAL.
    Returns:
        list: Statut et informations associées.
    """
    if pd.isna(doi_to_check) or not str(doi_to_check).strip():
        return ["Pas de DOI valide", "", "", "", "", ""]

    doi_cleaned_lower = str(doi_to_check).lower().strip()
    
    # 1. Vérifier dans la collection HAL (colonne 'DOIs')
    if 'DOIs' in collection_df.columns and not collection_df.empty:
        # Créer un ensemble de DOIs de la collection pour une recherche rapide (en minuscules)
        dois_coll_set = set(collection_df['DOIs'].dropna().astype(str).str.lower().str.strip())
        if doi_cleaned_lower in dois_coll_set:
            # Récupérer la ligne correspondante
            match_series = collection_df[collection_df['DOIs'].astype(str).str.lower().str.strip() == doi_cleaned_lower].iloc[0]
            return [
                "Dans la collection",
                match_series.get('Titres', ''), # Titre HAL associé
                match_series.get('Hal_ids', ''),
                match_series.get('Types de dépôts', ''),
                match_series.get('HAL Link', ''),
                match_series.get('HAL Ext ID', '')
            ]

    # 2. Si non trouvé dans la collection, chercher dans tout HAL via l'API
    # Nettoyer le DOI pour la recherche Solr (enlever le préfixe HTTP et échapper)
    # doiId_s est généralement le DOI sans le préfixe https://doi.org/
    solr_doi_query_val = escapeSolrArg(doi_cleaned_lower.replace("https://doi.org/", ""))
    
    try:
        # Utiliser le champ doiId_s pour une recherche plus ciblée du DOI
        r_req = requests.get(f"{HAL_API_ENDPOINT}?q=doiId_s:\"{solr_doi_query_val}\"&rows=1&fl={HAL_FIELDS_TO_FETCH}", timeout=10)
        r_req.raise_for_status()
        r_json = r_req.json()
        
        if r_json.get('response', {}).get('numFound', 0) > 0:
            doc = r_json['response']['docs'][0]
            return [
                "Dans HAL mais hors de la collection",
                doc.get('title_s', [""])[0], # Premier titre trouvé
                doc.get('docid', ''),
                doc.get('submitType_s', ''),
                doc.get('linkExtUrl_s', ''),
                doc.get('linkExtId_s', '')
            ]
    except requests.exceptions.RequestException as e:
        st.warning(f"Erreur de requête à l'API HAL pour le DOI '{doi_to_check}': {e}")
    except (KeyError, IndexError, json.JSONDecodeError) as e_json:
        st.warning(f"Structure de réponse HAL inattendue ou erreur JSON pour le DOI '{doi_to_check}': {e_json}")
        
    return ["Hors HAL", "", "", "", "", ""] # Si non trouvé après toutes les vérifications


def query_upw(doi_value):
    """
    Interroge l'API Unpaywall pour un DOI donné.
    Args:
        doi_value (str): Le DOI à interroger.
    Returns:
        dict: Dictionnaire contenant les informations d'Unpaywall.
    """
    if pd.isna(doi_value) or not str(doi_value).strip():
        return {"Statut Unpaywall": "DOI manquant", "doi_interroge": str(doi_value)}
    
    doi_cleaned = str(doi_value).strip()
    email = "hal.dbm@listes.u-paris.fr" # Email pour l'API Unpaywall (pool poli)
    
    try:
        req = requests.get(f"https://api.unpaywall.org/v2/{doi_cleaned}?email={email}", timeout=15)
        req.raise_for_status()
        res = req.json()
    except requests.exceptions.Timeout:
        return {"Statut Unpaywall": "timeout Unpaywall", "doi_interroge": doi_cleaned}
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return {"Statut Unpaywall": "non trouvé dans Unpaywall", "doi_interroge": doi_cleaned}
        return {"Statut Unpaywall": f"erreur HTTP Unpaywall ({e.response.status_code})", "doi_interroge": doi_cleaned}
    except requests.exceptions.RequestException as e:
        return {"Statut Unpaywall": f"erreur requête Unpaywall: {type(e).__name__}", "doi_interroge": doi_cleaned}
    except json.JSONDecodeError:
        return {"Statut Unpaywall": "erreur JSON Unpaywall", "doi_interroge": doi_cleaned}

    # Vérification supplémentaire si le DOI n'est pas dans Unpaywall
    if res.get("message") and "isn't in Unpaywall" in res.get("message", "").lower():
        return {"Statut Unpaywall": "non trouvé dans Unpaywall (message API)", "doi_interroge": doi_cleaned}

    # Construction du dictionnaire de résultats
    upw_info = {
        "Statut Unpaywall": "closed" if not res.get("is_oa") else "open",
        "oa_status": res.get("oa_status", ""), 
        "oa_publisher_license": "",
        "oa_publisher_link": "",
        "oa_repo_link": "",
        "publisher": res.get("publisher", ""),
        "doi_interroge": doi_cleaned # Garder une trace du DOI effectivement interrogé
    }

    # Informations sur la meilleure localisation OA
    best_oa_loc = res.get("best_oa_location")
    if best_oa_loc:
        host_type = best_oa_loc.get("host_type", "")
        license_val = best_oa_loc.get("license") # Peut être None
        url_pdf = best_oa_loc.get("url_for_pdf")
        url_landing = best_oa_loc.get("url") # 'url' est souvent la landing page

        if host_type == "publisher":
            upw_info["oa_publisher_license"] = license_val if license_val else ""
            upw_info["oa_publisher_link"] = url_pdf or url_landing or ""
        elif host_type == "repository":
            upw_info["oa_repo_link"] = str(url_pdf or url_landing or "")

    return upw_info


def enrich_w_upw_parallel(input_df):
    """
    Enrichit un DataFrame avec les données d'Unpaywall en parallèle.
    Args:
        input_df (pd.DataFrame): DataFrame d'entrée (doit contenir une colonne 'doi').
    Returns:
        pd.DataFrame: DataFrame enrichi avec les colonnes Unpaywall.
    """
    if input_df.empty or 'doi' not in input_df.columns:
        st.warning("DataFrame vide ou colonne 'doi' manquante pour l'enrichissement Unpaywall.")
        # Initialiser les colonnes Unpaywall si elles n'existent pas pour éviter des erreurs en aval
        upw_cols = ["Statut Unpaywall", "oa_status", "oa_publisher_license", "oa_publisher_link", "oa_repo_link", "publisher", "doi_interroge"]
        for col in upw_cols:
            if col not in input_df.columns:
                input_df[col] = pd.NA
        return input_df

    df_copy = input_df.copy() # Travailler sur une copie
    df_copy.reset_index(drop=True, inplace=True)

    # Extraire les DOIs à interroger (remplacer NaN par une chaîne vide pour éviter erreur dans query_upw)
    dois_to_query = df_copy['doi'].fillna("").tolist()

    results = []
    # Utiliser ThreadPoolExecutor pour paralléliser les requêtes
    with ThreadPoolExecutor(max_workers=10) as executor: # Ajuster max_workers selon les limites de l'API et les ressources
        # Utiliser tqdm pour une barre de progression (visible en console ou si Streamlit le gère)
        results = list(tqdm(executor.map(query_upw, dois_to_query), total=len(dois_to_query), desc="Enrichissement Unpaywall"))

    # Convertir la liste de dictionnaires (résultats) en DataFrame
    if results:
        upw_results_df = pd.DataFrame(results)
        # Fusionner/joindre les résultats avec le DataFrame original copié
        # S'assurer que l'ordre est conservé ou utiliser une clé de jointure si l'index a changé
        for col in upw_results_df.columns:
            if col not in df_copy.columns: # Ajouter la colonne si elle n'existe pas
                 df_copy[col] = pd.NA 
            # Assigner les valeurs. S'assurer que la longueur correspond.
            # Si l'ordre est garanti (ce qui est le cas avec map sur une liste), on peut assigner directement.
            df_copy[col] = upw_results_df[col].values 
    else: # Si aucun résultat (ex: tous les DOI étaient invalides)
        st.info("Aucun résultat d'enrichissement Unpaywall à ajouter.")
        # S'assurer que les colonnes sont présentes même si vides
        upw_cols = ["Statut Unpaywall", "oa_status", "oa_publisher_license", "oa_publisher_link", "oa_repo_link", "publisher", "doi_interroge"]
        for col in upw_cols:
            if col not in df_copy.columns:
                df_copy[col] = pd.NA
                
    return df_copy


def add_permissions(row_series_data):
    """
    Ajoute les informations de permission de dépôt via l'API oa.works (anciennement oadoi.org/sherpa).
    Args:
        row_series_data (pd.Series): Une ligne du DataFrame (représentée comme une Series).
    Returns:
        str: Chaîne décrivant les conditions de dépôt, ou un message d'erreur/statut.
    """
    # Vérifier si un lien de dépôt OA existe déjà ou si une licence éditeur claire est présente
    # Ces informations viennent d'Unpaywall (précédemment ajoutées à la ligne)
    oa_repo_link_val = str(row_series_data.get("oa_repo_link", "") or "").strip()
    oa_publisher_license_val = str(row_series_data.get("oa_publisher_license", "") or "").strip()

    # Si déjà clairement Open Access via repo ou licence éditeur, on peut ne pas chercher plus loin
    # ou simplement noter que c'est déjà géré. Pour l'instant, on continue la recherche pour avoir les infos de oa.works.
    # if oa_repo_link_val or oa_publisher_license_val:
    #     return "Déjà OA (repo/licence éditeur)"

    doi_val = row_series_data.get('doi') # Le DOI original de la publication
    if pd.isna(doi_val) or not str(doi_val).strip():
        return "DOI manquant pour permissions"

    doi_cleaned_for_api = str(doi_val).strip()
    try:
        # Utiliser l'API permissions.oa.works
        req = requests.get(f"https://api.permissions.oa.works/permissions/{doi_cleaned_for_api}", timeout=15)
        # Noter que cette API peut retourner 404 si le DOI n'est pas trouvé, ce qui est géré ci-dessous.
        req.raise_for_status() # Lève une exception pour les erreurs HTTP 4xx/5xx autres que 404 (si on ne les gère pas spécifiquement)
        res_json = req.json()
        
        best_permission_info = res_json.get("best_permission") # Peut être None
        if not best_permission_info:
            return "Aucune permission trouvée (oa.works)"

    except requests.exceptions.Timeout:
        return f"Timeout permissions (oa.works) pour DOI {doi_cleaned_for_api}"
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return f"Permissions non trouvées (404 oa.works) pour DOI {doi_cleaned_for_api}"
        return f"Erreur HTTP permissions ({e.response.status_code} oa.works) pour DOI {doi_cleaned_for_api}"
    except requests.exceptions.RequestException as e:
        return f"Erreur requête permissions (oa.works) pour DOI {doi_cleaned_for_api}: {type(e).__name__}"
    except json.JSONDecodeError:
        return f"Erreur JSON permissions (oa.works) pour DOI {doi_cleaned_for_api}"

    # Analyser la meilleure permission trouvée
    locations_allowed = best_permission_info.get("locations", [])
    # Vérifier si le dépôt en "repository" est explicitement autorisé
    if not any("repository" in str(loc).lower() for loc in locations_allowed):
        return "Dépôt en archive non listé dans les permissions (oa.works)"

    version_allowed = best_permission_info.get("version", "Version inconnue")
    licence_info = best_permission_info.get("licence", "Licence inconnue")
    embargo_months_val = best_permission_info.get("embargo_months") # Peut être None, 0, ou un entier

    embargo_display_str = "Pas d'embargo spécifié"
    if isinstance(embargo_months_val, int):
        if embargo_months_val == 0:
            embargo_display_str = "Pas d'embargo"
        elif embargo_months_val > 0:
            embargo_display_str = f"{embargo_months_val} mois d'embargo"
    
    # Construire la chaîne de résultat
    # Privilégier les versions "publishedVersion" ou "acceptedVersion"
    if version_allowed.lower() in ["publishedversion", "acceptedversion"]:
        return f"Version autorisée (oa.works): {version_allowed} ; Licence: {licence_info} ; Embargo: {embargo_display_str}"
    
    # Si une autre version ou information est disponible
    return f"Info permission (oa.works): {version_allowed} ; {licence_info} ; {embargo_display_str}"


def add_permissions_parallel(input_df):
    """
    Ajoute les informations de permission de dépôt à un DataFrame en parallèle.
    Args:
        input_df (pd.DataFrame): DataFrame d'entrée (doit contenir 'doi', et idéalement les colonnes Unpaywall).
    Returns:
        pd.DataFrame: DataFrame enrichi avec la colonne 'deposit_condition'.
    """
    if input_df.empty or 'doi' not in input_df.columns: # 'doi' est la clé pour cette fonction
        st.warning("DataFrame vide ou colonne 'doi' manquante pour l'ajout des permissions.")
        if 'deposit_condition' not in input_df.columns and not input_df.empty:
             input_df['deposit_condition'] = pd.NA # Ajouter la colonne si elle manque
        return input_df

    df_copy = input_df.copy() # Travailler sur une copie
    
    # S'assurer que la colonne 'deposit_condition' existe
    if 'deposit_condition' not in df_copy.columns:
        df_copy['deposit_condition'] = pd.NA

    # Fonction à appliquer à chaque ligne (représentée comme une Series pandas)
    def apply_add_permissions_to_row(row_as_series):
        return add_permissions(row_as_series)

    # Utiliser ThreadPoolExecutor pour appliquer la fonction en parallèle
    # Convertir le DataFrame en une liste de Series (chaque Series est une ligne)
    rows_as_series_list = [row_data for _, row_data in df_copy.iterrows()]
    
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor: # Ajuster max_workers
        results = list(tqdm(executor.map(apply_add_permissions_to_row, rows_as_series_list), total=len(df_copy), desc="Ajout des permissions de dépôt"))

    if results:
        df_copy['deposit_condition'] = results
    else: # Si aucun résultat (ex: DataFrame d'entrée était vide après filtrage implicite)
        st.info("Aucun résultat d'ajout de permissions.")
        # S'assurer que la colonne existe même si vide
        if 'deposit_condition' not in df_copy.columns:
            df_copy['deposit_condition'] = pd.NA
            
    return df_copy


def deduce_todo(row_data):
    """
    Déduit les actions à réaliser pour une publication en fonction de son statut HAL, Unpaywall et permissions.
    Args:
        row_data (pd.Series): Une ligne du DataFrame contenant toutes les informations nécessaires.
    Returns:
        str: Une chaîne de caractères décrivant les actions suggérées, séparées par " | ".
    """
    # Extraction des informations de la ligne
    statut_hal_val = str(row_data.get("Statut_HAL", "")).strip()
    type_depot_hal_val = str(row_data.get("type_dépôt_si_trouvé", "")).strip().lower() # Mettre en minuscule pour la comparaison
    id_hal_val = str(row_data.get("identifiant_hal_si_trouvé", "")).strip()

    statut_upw_val = str(row_data.get("Statut Unpaywall", "")).strip().lower()
    oa_repo_link_val = str(row_data.get("oa_repo_link", "") or "").strip()
    oa_publisher_link_val = str(row_data.get("oa_publisher_link", "") or "").strip()
    oa_publisher_license_val = str(row_data.get("oa_publisher_license", "") or "").strip()
    deposit_condition_val = str(row_data.get("deposit_condition", "")).lower()

    suggested_actions = []

    # --- Analyse du statut HAL ---
    if statut_hal_val == "Dans la collection" and type_depot_hal_val == "file":
        suggested_actions.append("✅ Dépôt HAL OK (avec fichier).")
    elif statut_hal_val == "Titre trouvé dans la collection : probablement déjà présent" and type_depot_hal_val == "file":
        suggested_actions.append("✅ Titre probablement déjà déposé dans la collection (avec fichier).")
    
    if statut_hal_val == "Dans HAL mais hors de la collection":
        suggested_actions.append("🏷️ Affiliation à vérifier dans HAL (trouvé hors collection).")
    if statut_hal_val == "Titre approchant trouvé dans HAL mais hors de la collection":
        suggested_actions.append("🔍 Titre approchant hors collection. Vérifier affiliations HAL.")

    if statut_hal_val == "Dans la collection" and type_depot_hal_val != "file" and id_hal_val:
        suggested_actions.append(f"📄 Notice HAL ({id_hal_val}) sans fichier. Vérifier possibilité d'ajout de fichier.")
    
    if statut_hal_val in ["Hors HAL", "Titre incorrect, probablement absent de HAL"] and not id_hal_val:
        suggested_actions.append("📥 Créer la notice (et si possible déposer le fichier) dans HAL.")
    elif statut_hal_val == "Pas de DOI valide" and not id_hal_val: # Si la recherche par titre (fallback) n'a rien donné non plus
        suggested_actions.append("📥 DOI manquant/invalide et titre non trouvé dans HAL. Créer notice si pertinent.")


    if statut_hal_val == "Titre invalide":
        suggested_actions.append("❌ Titre considéré invalide par le script. Vérifier/corriger le titre source.")
    if statut_hal_val == "Titre approchant trouvé dans la collection : à vérifier":
        suggested_actions.append("🧐 Titre approchant dans la collection. Vérifier si c'est une variante déjà déposée.")

    # --- Suggestions basées sur Unpaywall et Permissions (si pas déjà "OK avec fichier" dans HAL) ---
    is_hal_ok_with_file = any("✅ Dépôt HAL OK (avec fichier)" in act for act in suggested_actions) or \
                          any("✅ Titre probablement déjà déposé" in act for act in suggested_actions)

    if not is_hal_ok_with_file:
        # Si OA via un dépôt en archive (repository) selon Unpaywall
        if oa_repo_link_val:
            suggested_actions.append(f"🔗 OA via archive (Unpaywall): {oa_repo_link_val}. Si pas dans HAL, envisager dépôt notice/fichier.")
        
        # Si OA via éditeur avec licence selon Unpaywall
        if oa_publisher_link_val and oa_publisher_license_val:
            suggested_actions.append(f"📜 OA éditeur (licence {oa_publisher_license_val}): {oa_publisher_link_val}. Vérifier si dépôt HAL souhaité/possible.")
        elif oa_publisher_link_val and not oa_publisher_license_val:
             suggested_actions.append(f"🔗 OA éditeur (sans licence claire via UPW): {oa_publisher_link_val}. Vérifier conditions de dépôt HAL.")

        # Analyse des conditions de dépôt (oa.works) si pas déjà clairement OA par ailleurs
        # (Cette condition peut être redondante si oa_repo_link ou oa_publisher_link sont déjà remplis,
        # mais deposit_condition_val peut donner des infos plus précises sur la version/embargo)
        if "version autorisée (oa.works): publishedversion" in deposit_condition_val:
            suggested_actions.append(f"📄 Dépôt version éditeur possible selon oa.works. ({deposit_condition_val})")
        elif "version autorisée (oa.works): acceptedversion" in deposit_condition_val:
            suggested_actions.append(f"✍️ Dépôt postprint possible selon oa.works. ({deposit_condition_val})")
        
        # Si fermé et aucune condition de dépôt claire, ou si Unpaywall est "closed"
        if statut_upw_val == "closed" and \
           not ("publishedversion" in deposit_condition_val or "acceptedversion" in deposit_condition_val) and \
           not oa_repo_link_val and not (oa_publisher_link_val and oa_publisher_license_val) : # Si vraiment rien n'indique une ouverture
            suggested_actions.append("📧 Article fermé (Unpaywall) et pas de permission claire (oa.works). Contacter auteur pour LRN/dépôt.")
        
        # Cas où Unpaywall ou oa.works retournent des erreurs/statuts non informatifs
        if statut_upw_val not in ["open", "closed", "doi manquant", "non trouvé dans unpaywall", "non trouvé dans unpaywall (message api)"] and "erreur" in statut_upw_val: # Ex: timeout, erreur JSON
            suggested_actions.append(f"⚠️ Statut Unpaywall: {statut_upw_val}. Vérification manuelle des droits nécessaire.")
        if "erreur" in deposit_condition_val or "timeout" in deposit_condition_val or ("doi manquant" in deposit_condition_val and not oa_repo_link_val and not oa_publisher_link_val) :
             suggested_actions.append(f"⚠️ Info permissions (oa.works): {deposit_condition_val}. Vérification manuelle nécessaire.")


    if not suggested_actions:
        return "🛠️ À vérifier manuellement (aucune action spécifique déduite)."
        
    # Éviter les doublons d'actions (si différentes logiques mènent à des suggestions similaires)
    # et joindre. Utiliser un set pour l'unicité puis convertir en liste pour l'ordre (si besoin) ou trier.
    return " | ".join(sorted(list(set(suggested_actions))))


def addCaclLinkFormula(pre_url_str, post_url_str, text_for_link):
    """
    Crée une formule de lien hypertexte pour LibreOffice Calc.
    Args:
        pre_url_str (str): Partie initiale de l'URL (ex: "https://hal.science/").
        post_url_str (str): Partie finale de l'URL (ex: "hal-01234567").
        text_for_link (str): Texte à afficher pour le lien.
    Returns:
        str: Formule HYPERLINK ou chaîne vide si les entrées sont invalides.
    """
    if post_url_str and text_for_link: # S'assurer que post_url et text ne sont pas None ou vides
        # Nettoyer et s'assurer que ce sont des chaînes
        pre_url_cleaned = str(pre_url_str if pre_url_str else "").strip()
        post_url_cleaned = str(post_url_str).strip()
        text_cleaned = str(text_for_link).strip().replace('"', '""') # Échapper les guillemets pour la formule

        full_url = f"{pre_url_cleaned}{post_url_cleaned}"
        
        # Tronquer le texte affiché si trop long
        display_text_final = text_cleaned
        if len(text_cleaned) > 50: # Limite arbitraire pour la lisibilité
            display_text_final = text_cleaned[:47] + "..."
            
        return f'=HYPERLINK("{full_url}";"{display_text_final}")'
    return "" # Retourner une chaîne vide si pas de lien à créer


def check_df(input_df_to_check, hal_collection_df, progress_bar_st=None, progress_text_st=None):
    """
    Compare chaque ligne d'un DataFrame d'entrée avec les données d'une collection HAL.
    Args:
        input_df_to_check (pd.DataFrame): DataFrame contenant les publications à vérifier (avec 'doi' et/ou 'Title').
        hal_collection_df (pd.DataFrame): DataFrame de la collection HAL (avec 'DOIs', 'Titres', 'nti', etc.).
        progress_bar_st (st.progress, optional): Barre de progression Streamlit.
        progress_text_st (st.empty, optional): Zone de texte Streamlit pour les messages de progression.
    Returns:
        pd.DataFrame: DataFrame d'entrée enrichi avec les colonnes de statut HAL.
    """
    if input_df_to_check.empty:
        st.info("Le DataFrame d'entrée pour check_df est vide. Aucune vérification HAL à effectuer.")
        # S'assurer que les colonnes de sortie existent pour éviter les erreurs en aval
        hal_output_cols = ['Statut_HAL', 'titre_HAL_si_trouvé', 'identifiant_hal_si_trouvé', 
                           'type_dépôt_si_trouvé', 'HAL Link', 'HAL Ext ID']
        for col_name in hal_output_cols:
            if col_name not in input_df_to_check.columns:
                input_df_to_check[col_name] = pd.NA
        return input_df_to_check

    df_to_process = input_df_to_check.copy() # Travailler sur une copie

    # Initialiser les listes pour stocker les résultats de la comparaison HAL
    statuts_hal_list = []
    titres_hal_list = []
    ids_hal_list = []
    types_depot_hal_list = []
    links_hal_list = []
    ext_ids_hal_list = []

    total_rows_to_process = len(df_to_process)
    # Utiliser tqdm pour la progression (visible en console, Streamlit gère sa propre barre)
    for index, row_to_check in tqdm(df_to_process.iterrows(), total=total_rows_to_process, desc="Vérification HAL (check_df)"):
        doi_value_from_row = row_to_check.get('doi') # Peut être NaN
        title_value_from_row = row_to_check.get('Title') # Peut être NaN ou vide

        # Priorité à la recherche par DOI si disponible et valide
        hal_status_result = ["Pas de DOI valide", "", "", "", "", ""] # Statut par défaut
        
        if pd.notna(doi_value_from_row) and str(doi_value_from_row).strip():
            hal_status_result = statut_doi(str(doi_value_from_row), hal_collection_df)
        
        # Si le DOI n'a pas donné de résultat concluant (pas trouvé dans la collection ou HAL global)
        # OU si le DOI était invalide/manquant, alors tenter par titre
        if hal_status_result[0] not in ("Dans la collection", "Dans HAL mais hors de la collection"):
            if pd.notna(title_value_from_row) and str(title_value_from_row).strip():
                # Si la recherche DOI a échoué (ex: "Hors HAL" ou "Pas de DOI valide"),
                # on écrase son résultat par celui de la recherche par titre.
                hal_status_result = statut_titre(str(title_value_from_row), hal_collection_df)
            elif not (pd.notna(doi_value_from_row) and str(doi_value_from_row).strip()): 
                # Si ni DOI ni Titre valides, marquer comme données insuffisantes
                hal_status_result = ["Données d'entrée insuffisantes (ni DOI ni Titre)", "", "", "", "", ""]
        
        # Ajouter les résultats aux listes
        statuts_hal_list.append(hal_status_result[0])
        titres_hal_list.append(hal_status_result[1]) # Titre HAL trouvé ou titre original si non trouvé
        ids_hal_list.append(hal_status_result[2])
        types_depot_hal_list.append(hal_status_result[3])
        links_hal_list.append(hal_status_result[4])
        ext_ids_hal_list.append(hal_status_result[5])
        
        # Mettre à jour la barre de progression Streamlit si fournie
        if progress_bar_st is not None and progress_text_st is not None:
            current_progress_val = (index + 1) / total_rows_to_process
            progress_bar_st.progress(int(current_progress_val * 100))
            # Le texte de progression est souvent géré par l'appelant pour indiquer l'étape globale

    # Ajouter les nouvelles colonnes au DataFrame copié
    df_to_process['Statut_HAL'] = statuts_hal_list
    df_to_process['titre_HAL_si_trouvé'] = titres_hal_list
    df_to_process['identifiant_hal_si_trouvé'] = ids_hal_list
    df_to_process['type_dépôt_si_trouvé'] = types_depot_hal_list
    df_to_process['HAL Link'] = links_hal_list
    df_to_process['HAL Ext ID'] = ext_ids_hal_list
    
    if progress_bar_st: progress_bar_st.progress(100) # S'assurer que la barre est à 100% à la fin de cette étape
    return df_to_process


class HalCollImporter:
    """
    Classe pour importer les données d'une collection HAL.
    """
    def __init__(self, collection_code: str, start_year_val=None, end_year_val=None):
        self.collection_code = str(collection_code).strip() if collection_code else "" # "" pour tout HAL
        self.start_year = start_year_val if start_year_val is not None else DEFAULT_START_YEAR
        self.end_year = end_year_val if end_year_val is not None else DEFAULT_END_YEAR # '*' pour Solr signifie "jusqu'à la fin"
        
        self.num_docs_in_collection = self._get_num_docs()

    def _get_num_docs(self):
        """ Récupère le nombre total de documents dans la collection pour la période donnée. """
        try:
            query_params_count = {
                'q': '*:*', # Interroger tous les documents dans la collection/période
                'fq': f'publicationDateY_i:[{self.start_year} TO {self.end_year}]',
                'rows': 0, # Ne pas retourner de documents, juste le compte
                'wt': 'json'
            }
            # Construire l'URL de base : si collection_code est vide, interroger tout HAL
            base_search_url = f"{HAL_API_ENDPOINT}{self.collection_code}/" if self.collection_code else HAL_API_ENDPOINT
            
            response_count = requests.get(base_search_url, params=query_params_count, timeout=15)
            response_count.raise_for_status()
            return response_count.json().get('response', {}).get('numFound', 0)
        except requests.exceptions.RequestException as e:
            st.error(f"Erreur API HAL (comptage) pour '{self.collection_code or 'HAL global'}': {e}")
            return 0
        except (KeyError, json.JSONDecodeError):
            st.error(f"Réponse API HAL (comptage) inattendue pour '{self.collection_code or 'HAL global'}'.")
            return 0

    def import_data(self):
        """ Importe les données de la collection HAL paginées. """
        if self.num_docs_in_collection == 0:
            st.info(f"Aucun document trouvé pour la collection '{self.collection_code or 'HAL global'}' entre {self.start_year} et {self.end_year}.")
            # Retourner un DataFrame vide avec les colonnes attendues pour la cohérence
            return pd.DataFrame(columns=['Hal_ids', 'DOIs', 'Titres', 'Types de dépôts', 
                                         'HAL Link', 'HAL Ext ID', 'nti'])

        all_docs_list = []
        rows_per_api_page = 1000 # Nombre de documents par page (max pour l'API HAL)
        current_api_cursor = "*" # Curseur initial pour la pagination profonde Solr

        # Construire l'URL de base pour la recherche
        base_search_url = f"{HAL_API_ENDPOINT}{self.collection_code}/" if self.collection_code else HAL_API_ENDPOINT

        # Utiliser tqdm pour la barre de progression
        with tqdm(total=self.num_docs_in_collection, desc=f"Import HAL ({self.collection_code or 'Global'})") as pbar_hal:
            while True:
                query_params_page = {
                    'q': '*:*',
                    'fq': f'publicationDateY_i:[{self.start_year} TO {self.end_year}]',
                    'fl': HAL_FIELDS_TO_FETCH,
                    'rows': rows_per_api_page,
                    'sort': 'docid asc', # Tri nécessaire pour la pagination par curseur
                    'cursorMark': current_api_cursor,
                    'wt': 'json'
                }
                try:
                    response_page = requests.get(base_search_url, params=query_params_page, timeout=45) # Timeout plus long pour les grosses requêtes
                    response_page.raise_for_status()
                    data_page = response_page.json()
                except requests.exceptions.RequestException as e:
                    st.error(f"Erreur API HAL (import page, curseur {current_api_cursor}): {e}")
                    break # Arrêter en cas d'erreur
                except json.JSONDecodeError:
                    st.error(f"Erreur décodage JSON (import page HAL, curseur {current_api_cursor}).")
                    break

                docs_on_current_page = data_page.get('response', {}).get('docs', [])
                if not docs_on_current_page: # Plus de documents à récupérer
                    break

                for doc_data in docs_on_current_page:
                    # Un document HAL peut avoir plusieurs titres (ex: langues différentes)
                    # On crée une entrée par titre pour une comparaison plus fine.
                    hal_titles_list = doc_data.get('title_s', [""]) # S'assurer qu'il y a au moins une chaîne vide
                    if not isinstance(hal_titles_list, list): hal_titles_list = [str(hal_titles_list)] # Au cas où ce ne serait pas une liste

                    for title_item in hal_titles_list:
                        all_docs_list.append({
                            'Hal_ids': doc_data.get('docid', ''),
                            'DOIs': str(doc_data.get('doiId_s', '')).lower() if doc_data.get('doiId_s') else '', # DOI en minuscule, ou chaîne vide
                            'Titres': str(title_item), # Titre original de cette entrée
                            'Types de dépôts': doc_data.get('submitType_s', ''),
                            'HAL Link': doc_data.get('linkExtUrl_s', ''), 
                            'HAL Ext ID': doc_data.get('linkExtId_s', '') 
                        })
                pbar_hal.update(len(docs_on_current_page)) # Mettre à jour la barre de progression tqdm

                next_api_cursor = data_page.get('nextCursorMark')
                # Condition d'arrêt de la pagination (si le curseur ne change plus ou est vide)
                if current_api_cursor == next_api_cursor or not next_api_cursor:
                    break
                current_api_cursor = next_api_cursor
        
        if not all_docs_list: # Si aucune donnée n'a été collectée malgré num_docs > 0
             return pd.DataFrame(columns=['Hal_ids', 'DOIs', 'Titres', 'Types de dépôts', 
                                          'HAL Link', 'HAL Ext ID', 'nti'])

        df_collection_hal = pd.DataFrame(all_docs_list)
        # Ajouter la colonne 'nti' (titre normalisé) pour les comparaisons approchantes
        if 'Titres' in df_collection_hal.columns:
            df_collection_hal['nti'] = df_collection_hal['Titres'].apply(normalise)
        else: # Ne devrait pas arriver si HAL_FIELDS_TO_FETCH inclut title_s
            df_collection_hal['nti'] = ""
            
        return df_collection_hal


def merge_rows_with_sources(grouped_data):
    """
    Fusionne les lignes d'un groupe (par DOI ou Titre) en conservant les informations uniques
    et en concaténant les sources de données et les IDs.
    Args:
        grouped_data (pd.DataFrameGroupBy): Groupe de lignes à fusionner.
    Returns:
        pd.Series: Une ligne (Series) représentant les données fusionnées.
    """
    # IDs des sources (ex: ID Scopus, ID OpenAlex, PMID)
    # Gérer le cas où 'id' n'est pas dans les colonnes (ex: données PubMed seules où 'id' est le PMID)
    merged_ids_str = '|'.join(map(str, grouped_data['id'].dropna().astype(str).unique())) if 'id' in grouped_data.columns else None
    
    # Noms des sources de données (ex: 'scopus|openalex')
    merged_sources_str = '|'.join(grouped_data['Data source'].dropna().astype(str).unique()) if 'Data source' in grouped_data.columns else None

    # Initialiser un dictionnaire pour la nouvelle ligne fusionnée
    merged_row_content_dict = {}

    # Parcourir toutes les colonnes du groupe (sauf 'id' et 'Data source' qui sont traitées séparément)
    for column_name in grouped_data.columns:
        if column_name not in ['id', 'Data source']:
            # Obtenir les valeurs uniques, ignorer les NaN, convertir en str pour la jointure/comparaison
            unique_values_in_col = grouped_data[column_name].dropna().astype(str).unique()
            
            if len(unique_values_in_col) == 1:
                merged_row_content_dict[column_name] = unique_values_in_col[0]
            elif len(unique_values_in_col) > 1:
                # Concaténer les valeurs uniques avec '|', triées pour la cohérence
                merged_row_content_dict[column_name] = '|'.join(sorted(list(unique_values_in_col)))
            else: # Si toutes les valeurs sont NaN pour cette colonne dans le groupe
                merged_row_content_dict[column_name] = pd.NA # Utiliser pd.NA pour les valeurs manquantes explicites
    
    # Ajouter les champs 'id' et 'Data source' fusionnés au dictionnaire
    if merged_ids_str is not None: merged_row_content_dict['id'] = merged_ids_str
    if merged_sources_str is not None: merged_row_content_dict['Data source'] = merged_sources_str
    
    return pd.Series(merged_row_content_dict)


def get_authors_from_crossref(doi_value):
    """
    Récupère la liste des auteurs pour un DOI donné via l'API Crossref.
    Args:
        doi_value (str): Le DOI à interroger.
    Returns:
        list: Liste des noms d'auteurs (str "Prénom Nom"), ou liste avec message d'erreur.
    """
    if pd.isna(doi_value) or not str(doi_value).strip():
        return ["DOI manquant pour Crossref"]

    doi_cleaned_for_api = str(doi_value).strip()
    # Email recommandé pour l'API Crossref (pool poli)
    headers = {
        'User-Agent': 'c2LabHAL/1.0 (mailto:YOUR_EMAIL@example.com; https://github.com/GuillaumeGodet/c2labhal)', 
        'Accept': 'application/json'
    }
    url_crossref = f"https://api.crossref.org/works/{doi_cleaned_for_api}"
    
    try:
        response_crossref = requests.get(url_crossref, headers=headers, timeout=10)
        response_crossref.raise_for_status()
        data_crossref = response_crossref.json()
    except requests.exceptions.Timeout:
        return ["Timeout Crossref"]
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if hasattr(e.response, 'status_code') else 'N/A'
        # if status_code == 404: return [f"DOI non trouvé sur Crossref ({status_code})"] # Message moins verbeux pour 404
        return [f"Erreur HTTP Crossref ({status_code})"]
    except requests.exceptions.RequestException as e_req:
        return [f"Erreur requête Crossref: {type(e_req).__name__}"]
    except json.JSONDecodeError:
        return ["Erreur JSON Crossref"]

    # Extraction des auteurs
    authors_data_list = data_crossref.get('message', {}).get('author', [])
    if not authors_data_list:
        return [] # Pas d'auteurs trouvés ou champ auteur manquant

    author_names_list = []
    for author_entry in authors_data_list:
        if not isinstance(author_entry, dict): continue # Ignorer si l'entrée n'est pas un dictionnaire

        given_name = str(author_entry.get('given', '')).strip()
        family_name = str(author_entry.get('family', '')).strip()
        
        full_name = ""
        if given_name and family_name:
            full_name = f"{given_name} {family_name}"
        elif family_name: # Si seulement le nom de famille
            full_name = family_name
        elif given_name: # Si seulement le prénom (rare mais possible)
            full_name = given_name
        
        if full_name: # Ajouter seulement si un nom a été construit
            author_names_list.append(full_name)

    return author_names_list


def normalize_name(name_to_normalize):
    """
    Normalise un nom d'auteur : minuscules, suppression des accents, gestion des formats "Nom, Prénom".
    Args:
        name_to_normalize (str): Le nom à normaliser.
    Returns:
        str: Le nom normalisé.
    """
    if not isinstance(name_to_normalize, str): return ""
    
    name_lower = name_to_normalize.strip().lower()
    # Suppression des accents (NFD normalise en caractères de base + diacritiques, puis on filtre les diacritiques)
    name_unaccented = ''.join(c for c in unicodedata.normalize('NFD', name_lower) 
                              if unicodedata.category(c) != 'Mn')
    # Remplacer tirets et points par des espaces, puis normaliser les espaces multiples
    name_cleaned_spaces = name_unaccented.replace('-', ' ').replace('.', ' ')
    name_single_spaced = re.sub(r'\s+', ' ', name_cleaned_spaces).strip()

    # Gérer les noms au format "Nom, Prénom" (splitter sur la première virgule seulement)
    if ',' in name_single_spaced:
        parts = [part.strip() for part in name_single_spaced.split(',', 1)]
        if len(parts) == 2 and parts[0] and parts[1]: # S'assurer que les deux parties existent et ne sont pas vides
            # Inverser pour avoir "Prénom Nom"
            return f"{parts[1]} {parts[0]}"
            
    return name_single_spaced


def get_initial_form(normalised_author_name):
    """
    Génère une forme "Initiale Prénom NomFamille" à partir d'un nom déjà normalisé.
    Ex: "jean dupont" -> "j dupont"
    Args:
        normalised_author_name (str): Nom d'auteur normalisé (ex: "prénom nom").
    Returns:
        str: Forme avec initiale, ou le nom original si mal formé.
    """
    if not normalised_author_name: return ""
    
    name_parts = normalised_author_name.split()
    if len(name_parts) >= 2: # Au moins un prénom et un nom
        # Prendre la première lettre du premier mot (prénom) et le dernier mot (nom de famille)
        # Cela gère les prénoms composés (ex: "Jean-Luc Picard" -> "j picard" si normalisé en "jean luc picard")
        return f"{name_parts[0][0]} {name_parts[-1]}" 
    elif len(name_parts) == 1: # Si seulement un mot (ex: un seul nom, ou un nom composé sans espace après normalisation)
        return normalised_author_name # Retourner le mot tel quel, car on ne peut pas distinguer prénom/nom
    return "" # Cas où le nom normalisé est vide ou mal formé après split
