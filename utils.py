# -*- coding: utf-8 -*-
"""
Created on Fri May 16 00:16:18 2025

@author: godet-g
"""

# Fonction pour récupérer les données de Scopus
def get_scopus_data(api_key, query, max_items=2000):
    found_items_num = 1
    start_item = 0
    items_per_query = 25
    JSON = []

    while found_items_num > 0:
        try:
            resp = requests.get(
                'https://api.elsevier.com/content/search/scopus',
                headers={'Accept': 'application/json', 'X-ELS-APIKey': api_key},
                params={'query': query, 'count': items_per_query, 'start': start_item}
            )
            resp.raise_for_status()  # Lève une exception pour les codes d'état HTTP 4xx/5xx
        except requests.exceptions.RequestException as e:
            st.error(f"Erreur lors de la requête Scopus : {e}")
            return []

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

hal_fl = "docid,doiId_s,title_s,submitType_s,linkExtUrl_s,linkExtId_s"

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
    if not isinstance(term, str):
        return ""
    term = term.replace('\\', r'\\')  # escape \ first
    return "".join([nextStr for nextStr in escapedSeq(term)])

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

def in_hal(nti, ti):
    """Tries to find a title in HAL, first with a strict character match then if not found with a loose SolR search"""
    try:
        r_ex = requests.get(f"{endpoint}?q=title_t:{nti}&rows=1&fl={hal_fl}").json()['response']
        if r_ex['numFound'] > 0:
            if any(ti == x for x in r_ex['docs'][0]['title_s']):
                return [
                    "Titre trouvé dans HAL mais hors de la collection : affiliation probablement à corriger",
                    r_ex['docs'][0]['title_s'][0],
                    r_ex['docs'][0]['docid'],
                    r_ex['docs'][0]['submitType_s'],
                    r_ex['docs'][0].get('linkExtUrl_s', ""),  # Sécurité pour linkExtUrl_s
                    r_ex['docs'][0].get('linkExtId_s', ""),    # Sécurité pour linkExtId_s
                ]
    except KeyError:
        r_inex = requests.get(f"{endpoint}?q=title_t:{ti}&rows=1&fl={hal_fl}").json()['response']
        if r_inex['numFound'] > 0:
            return [
                "Titre approchant trouvé dans HAL mais hors de la collection : vérifier les affiliations",
                r_inex['response']['docs'][0]['title_s'][0],
                r_inex['response']['docs'][0]['docid'],
                r_inex['response']['docs'][0]['submitType_s'],
                r_inex['response']['docs'][0].get('linkExtUrl_s', ""),  # Sécurité pour linkExtUrl_s
                r_inex['response']['docs'][0].get('linkExtId_s', ""),    # Sécurité pour linkExtId_s
            ] if any(
                compare_inex(ti, x) for x in [r_inex['response']['docs'][0]['title_s']]
            ) else ["Hors HAL", "", "", "", "", ""]  # Et ici
    return ["Hors HAL", "", "", "", "", ""]  # Et ici

def statut_titre(title, coll_df):
    """
    Applies the matching process to a title, from searching it exactly in the HAL collection to be compared,
    to searching it loosely in HAL search API.
    """
    if not isinstance(title, str):
        return ["Titre invalide", "", "", "", ""]  # Modification ici

    try:
        if title[len(title) - 1] == "]" and detect(
            title[: re.match(r".*\[", title).span()[1]]
        ) != detect(title[re.match(r".*\[", title).span()[1] :]):
            title = title[: re.match(r".*\[", title).span()[1] :]
        elif detect(title[: len(title) // 2]) != detect(title[len(title) // 2 :]):
            title = title[: len(title) // 2]
        else:
            title = title
    except:
        title = title

    try:
        ti = '\"' + escapeSolrArg(title) + '\"'
    except TypeError:
        return ["Titre invalide", "", "", "", ""]  # Modification ici

    try:
        c_ex = ex_in_coll(title, coll_df)
        if c_ex:
            return c_ex
        else:
            c_inex = inex_in_coll(title, coll_df)
            if c_inex:
                return c_inex
            else:
                r_ex = in_hal(ti, title)
                return r_ex
    except KeyError:
        return ["Titre incorrect, probablement absent de HAL", "", "", "", ""]  # Modification ici


def ex_in_coll(ti, coll_df):
    """
    Takes a title from the list to be compared. If it is in the list of titles from the compared HAL collection,
    returns the corresponding HAL reference. Else, returns False.
    """
    try:
        return [
            "Titre trouvé dans la collection : probablement déjà présent",
            ti,
            coll_df[coll_df['Titres'] == ti].iloc[0, 0],
            coll_df[coll_df['Titres'] == ti].iloc[0, 3],
            coll_df[coll_df['Titres'] == ti].iloc[0, 4],
            coll_df[coll_df['Titres'] == ti].iloc[0, 5],  
        ]
    except IndexError:
        return False

def inex_in_coll(nti, coll_df):
    """
    Takes a title from the list to be compared. If it has at least 90% similarity with one of the titles from the compared HAL collection,
    returns the corresponding HAL reference. Else, returns False.
    """
    for x in list(coll_df['nti']):
        y = compare_inex(nti, x)
        if y:
            return [
                "Titre approchant trouvé dans la collection : à vérifier",
                coll_df[coll_df['nti'] == y].iloc[0, 2],
                coll_df[coll_df['nti'] == y].iloc[0, 0],
                coll_df[coll_df['nti'] == y].iloc[0, 3],
                coll_df[coll_df['nti'] == y].iloc[0, 4],
                coll_df[coll_df['nti'] == y].iloc[0, 5],  # Ajout de linkExtId_s ici
            ]
    return False



def in_hal(nti, ti):
    """Tries to find a title in HAL, first with a strict character match then if not found with a loose SolR search"""
    try:
        r_ex = requests.get(f"{endpoint}?q=title_t:{nti}&rows=1&fl={hal_fl}").json()['response']
        if r_ex['numFound'] > 0:
            if any(ti == x for x in r_ex['docs'][0]['title_s']):
                return [
                    "Titre trouvé dans HAL mais hors de la collection : affiliation probablement à corriger",
                    r_ex['docs'][0]['title_s'][0],
                    r_ex['docs'][0]['docid'],
                    r_ex['docs'][0]['submitType_s'],
                    r_ex['docs'][0].get('linkExtUrl_s', ""),  # Et ici (avec sécurité)
                ]
    except KeyError:
        r_inex = requests.get(f"{endpoint}?q=title_t:{ti}&rows=1&fl={hal_fl}").json()['response']
        if r_inex['numFound'] > 0:
            return [
                "Titre approchant trouvé dans HAL mais hors de la collection : vérifier les affiliations",
                r_inex['response']['docs'][0]['title_s'][0],
                r_inex['response']['docs'][0]['docid'],
                r_inex['response']['docs'][0]['submitType_s'],
                r_inex['response']['docs'][0].get('linkExtUrl_s', ""),  # Et ici (avec sécurité)
            ] if any(
                compare_inex(ti, x) for x in [r_inex['response']['docs'][0]['title_s']]
            ) else ["Hors HAL", "", "", "", ""]  # Et ici
    return ["Hors HAL", "", "", "", ""]  # Et ici
def statut_doi(do, coll_df):
    """applies the matching process to a DOI, searching it in the collection to be compared then in all of HAL"""
    dois_coll = coll_df['DOIs'].tolist()
    if do and do == do:
        ldo = do.lower()
        ndo = escapeSolrArg(re.sub(r"\[.*\]", "", do.replace("https://doi.org/", "").lower()))
        if ldo in dois_coll:
            return [
                "Dans la collection",
                coll_df[coll_df['DOIs'] == ldo].iloc[0, 2],
                coll_df[coll_df['DOIs'] == ldo].iloc[0, 0],
                coll_df[coll_df['DOIs'] == ldo].iloc[0, 3],
                coll_df[coll_df['DOIs'] == ldo].iloc[0, 4],
                coll_df[coll_df['DOIs'] == ldo].iloc[0, 5],  # Ajout de linkExtId_s ici
            ]
        else:
            r = requests.get(f"{endpoint}?q=doiId_id:{ndo}&rows=1&fl={hal_fl}").json()
            if r['response']['numFound'] > 0:
                return [
                    "Dans HAL mais hors de la collection",
                    r['response']['docs'][0]['title_s'][0],
                    r['response']['docs'][0]['docid'],
                    r['response']['docs'][0]['submitType_s'],
                    r['response']['docs'][0].get('linkExtUrl_s', ""),  # Sécurité pour linkExtUrl_s
                    r['response']['docs'][0].get('linkExtId_s', ""),    # Sécurité pour linkExtId_s
                ]
            return ["Hors HAL", "", "", "", "", ""]
    elif do != do:
        return ["Pas de DOI valide", "", "", "", "", ""]
    
def query_upw(doi):
    try:
        req = requests.get(f"https://api.unpaywall.org/v2/{doi}?email=hal.dbm@listes.u-paris.fr")
        res = req.json()
    except requests.RequestException as e:
        print(f"Erreur lors de la requête : {e}")
        return {}

    # Si l'article n'est pas dans Unpaywall
    if res.get("message") and "isn't in Unpaywall" in res.get("message"):
        return {"Statut Unpaywall": "missing"}

    # Construire toujours les métadonnées
    temp = {
        "Statut Unpaywall": "closed" if not res.get("is_oa") else "open",
        "oa_status": res.get("oa_status", ""), 
        "oa_publisher_license": "",
        "oa_publisher_link": "",
        "oa_repo_link": "",
        "publisher": res.get("publisher", "")
    }

    # Ajouter les liens/licences s'ils existent
    if res.get("best_oa_location"):
        best_loc = res["best_oa_location"]
        if best_loc["host_type"] == "publisher":
            temp["oa_publisher_license"] = best_loc.get("license", "")
            temp["oa_publisher_link"] = best_loc.get("url_for_pdf") or best_loc.get("url_for_landing_page")
        elif best_loc["host_type"] == "repository":
            temp["oa_repo_link"] = str(best_loc.get("url_for_pdf"))

    return temp


def enrich_w_upw(df):
    """
    Enrichit le DataFrame avec les données d'Unpaywall.
    """
    print(f"nb DOI à vérifier dans Unpaywall : {len(df)}")
    df.reset_index(drop=True, inplace=True)
    

    for row in df.itertuples():
        upw_data = query_upw(row.doi)
        for field in upw_data:
            try:
                df.at[row.Index, field] = upw_data[field]
            except Exception as e:
                print("\n\nProblème avec le DOI Unpaywall\n", field, row.doi, '\n\n', upw_data, e)
                break
    print("Unpaywall 100%")
    return df

from concurrent.futures import ThreadPoolExecutor

def enrich_w_upw_parallel(df):
    df.reset_index(drop=True, inplace=True)

    def process(index_row):
        index, row = index_row
        return query_upw(row['doi'])

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process, df.iterrows()))

    # S'assurer que toutes les colonnes nécessaires sont bien typées en 'object' (texte)
    for col in ["Statut Unpaywall", "oa_status", "oa_publisher_license", "oa_publisher_link", "oa_repo_link", "publisher"]:
        if col not in df.columns:
            df[col] = None
            df[col] = df[col].astype("object")
    for idx, upw_data in enumerate(results):
        for field, value in upw_data.items():
            df.at[idx, field] = value
    return df



def add_permissions(row):
    """
    Ajoute les possibilités de dépôt en archive via l'API permissions.
    """
  
    if str(row.get("oa_repo_link") or "").strip() or str(row.get("oa_publisher_license") or "").strip():
        return ""

    try:
        req = requests.get(f"https://bg.api.oa.works/permissions/{row['doi']}")
        res = req.json()
        best_permission = res.get("best_permission", {})
        print(f"[INFO] DOI {row['doi']} - permission trouvée")
    except Exception as e:
        print(f"[ERREUR] DOI {row['doi']} - exception: {e}")
        return ""

    locations = best_permission.get("locations", [])
    if not any("repository" in loc.lower() for loc in locations):
        print(f"[INFO] DOI {row['doi']} - pas de dépôt en référentiel autorisé")
        return ""

    version = best_permission.get("version")
    licence = best_permission.get("licence", "unknown licence")
    embargo_months = best_permission.get("embargo_months", "no months")
    embargo_str = f"{embargo_months} months" if isinstance(embargo_months, int) else embargo_months

    if version in ["acceptedVersion", "publishedVersion"]:
        print(f"[OK] DOI {row['doi']} - version autorisée : {version}, embargo : {embargo_str}")
        return f"{version} ; {licence} ; {embargo_str}"

    return ""

def add_permissions_parallel(df):
    def safe_add(row_dict):
        return add_permissions(pd.Series(row_dict))

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(safe_add, df.to_dict('records')))
    df['deposit_condition'] = results
    return df




def deduce_todo(row):
    """
    Déduit les actions à réaliser et les indique sous forme de texte avec des emojis.
    """

    if row["Statut_HAL"] == "Dans la collection" and row["type_dépôt_si_trouvé"] == "file":
        return "✅ Dépôt HAL OK"

    if row["Statut_HAL"] == "Dans HAL mais hors de la collection":
        return "🏷️ Vérifier l'affiliation dans HAL"

    if row["Statut_HAL"] == "Hors HAL":
        return "📥 Créer la référence dans HAL"

    if row["Statut_HAL"] == "Titre approchant trouvé dans la collection : à vérifier":
        return "🧐 Vérifier le titre — peut-être une variante déjà déposée"

    if row["Statut_HAL"] == "Titre trouvé dans la collection : probablement déjà présent" and row["type_dépôt_si_trouvé"] == "file":
        return "✅ Titre probablement déjà déposé"

    if row["Statut_HAL"] == "Titre invalide":
        return "❌ Titre invalide — corriger et réessayer"

    if row["Statut_HAL"] == "Titre incorrect, probablement absent de HAL":
        return "❌ Titre mal formé ou absent — à corriger"

    if row["Statut_HAL"] == "Titre approchant trouvé dans HAL mais hors de la collection":
        return "🔍 Présent dans HAL mais hors collection — vérifier affiliations"

    # Conditions de dépôt à analyser
    if "publishedVersion" in str(row["deposit_condition"]):
        return "📄 Récupérer le PDF éditeur"

    if row["oa_publisher_license"] and not row["oa_repo_link"]:
        return "📜 Ajouter le PDF éditeur selon la licence"

    if row["Statut Unpaywall"] != "open":
        return "📧 Article fermé : contacter l’auteur pour appliquer la LRN"

    if not row["identifiant_hal_si_trouvé"]:
        return "🆕 Aucune notice HAL détectée — en créer une"

    return "🛠️ À vérifier manuellement"



def addCaclLinkFormula(pre_url, post_url, txt):
    """
    fonction pour rendre les liens cliquables avec formule de libreOffice
    """
    if post_url:
        post_url, txt = str(post_url), str(txt)
        if txt.startswith("http"):
            txt = txt[txt.index("/") + 2:]
            txt = txt[4:25] if txt.startswith("www") else txt[:20]
        return '=LIEN.HYPERTEXTE("' + pre_url + post_url + '";"' + txt + '")'

def check_df(df, coll_df, progress_bar=None, progress_text=None):
    # Optimisations : accès rapide
    dois_coll_set = set(coll_df['DOIs'].dropna().str.lower())
    titres_coll_dict = {
        normalise(t).strip(): (docid, submit_type, t, link, ext_id)  # Modification ici
        for t, docid, submit_type, link, ext_id in zip(  # Et ici
            coll_df['Titres'], coll_df['Hal_ids'], coll_df['Types de dépôts'],
            coll_df['HAL Link'], coll_df['HAL Ext ID']
        )
    }

    # Nouvelle fonction statut_doi rapide
    def fast_statut_doi(doi):
        if pd.isna(doi):
            return ["Pas de DOI valide", "", "", "", "", ""]  # Modification ici
        doi = doi.lower()
        if doi in dois_coll_set:
            match = coll_df[coll_df['DOIs'] == doi].iloc[0]
            return [
                "Dans la collection",
                match['Titres'],
                match['Hal_ids'],
                match['Types de dépôts'],
                match['HAL Link'],
                match['HAL Ext ID'],  # Et ici
            ]
        else:
            # Appel à HAL si nécessaire
            try:
                ndo = escapeSolrArg(re.sub(r"\[.*\]", "", doi.replace("https://doi.org/", "").lower()))
                r = requests.get(f"{endpoint}?q=doiId_id:{ndo}&rows=1&fl={hal_fl}").json()
                if r['response']['numFound'] > 0:
                    d = r['response']['docs'][0]
                    return [
                        "Dans HAL mais hors de la collection",
                        d['title_s'][0],
                        d['docid'],
                        d['submitType_s'],
                        d.get('linkExtUrl_s', ""),  # Sécurité pour linkExtUrl_s
                        d.get('linkExtId_s', ""),    # Sécurité pour linkExtId_s
                    ]
                return ["Hors HAL", "", "", "", "", ""]  # Et ici
            except:
                return ["Erreur HAL DOI", "", "", "", "", ""]  # Et ici

    # Fallback : enrichissement par titre, en parallèle
    def enrich_titre(row):
        return statut_titre(row["Title"], coll_df)

    if progress_text:
        progress_text.text("Étape 4 : Matching avec HAL")
    if progress_bar:
        progress_bar.progress(70)

    # Résultats initiaux avec DOIs
    first_pass_results = df.apply(lambda x: fast_statut_doi(x["doi"]), axis=1)
    need_title_check = [
        i
        for i, r in enumerate(first_pass_results)
        if r[0] not in ("Dans la collection", "Dans HAL mais hors de la collection")
    ]

    # Enrichir par titre (parallèle)
    if progress_text:
        progress_text.text("Étape 4bis : Recherche par titre dans HAL")
    with ThreadPoolExecutor(max_workers=10) as executor:
        title_results = list(executor.map(lambda i: enrich_titre(df.iloc[i]), need_title_check))

    # Remplacer les valeurs dans first_pass_results
    for i, res in zip(need_title_check, title_results):
        first_pass_results[i] = res

    # Injecter les colonnes
    df['Statut_HAL'] = [r[0] for r in first_pass_results]
    df['titre_HAL_si_trouvé'] = [r[1] for r in first_pass_results]
    df['identifiant_hal_si_trouvé'] = [r[2] for r in first_pass_results]
    df['type_dépôt_si_trouvé'] = [r[3] for r in first_pass_results]
    df['HAL Link'] = [r[4] for r in first_pass_results]
    df['HAL Ext ID'] = [r[5] if len(r) > 5 else "" for r in first_pass_results]

    return df

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
        n = self.nbdocs
        docid_coll = list()
        dois_coll = list()
        titres_coll = list()
        submit_types_coll = list()
        link_ext_url_coll = list()
        link_ext_id_coll = list()  # Ajout de cette liste

        if n > 1000:
            current = 0
            cursor = ""
            next_cursor = "*"
            while cursor != next_cursor:
                cursor = next_cursor
                page = requests.get(
                    f"{endpoint}{self.coll_code}/?q=*&fq=publicationDateY_i:[{self.start_year} TO {self.end_year}]&fl={hal_fl}&rows=1000&cursorMark={cursor}&sort=docid asc&wt=json"
                ).json()
                for d in page['response']['docs']:
                    for t in d['title_s']:
                        titres_coll.append(t)
                        docid_coll.append(d['docid'])
                        try:
                            dois_coll.append(d['doiId_s'].lower())
                        except KeyError:
                            dois_coll.append("")
                        submit_types_coll.append(d['submitType_s'])
                        try:
                            link_ext_url_coll.append(d['linkExtUrl_s'])
                        except KeyError:
                            link_ext_url_coll.append("")
                        try:
                            link_ext_id_coll.append(d['linkExtId_s'])  # Ajout ici
                        except KeyError:
                            link_ext_id_coll.append("")  # Ou une autre valeur par défaut
                current += 1000
                next_cursor = page['nextCursorMark']
        else:
            for d in requests.get(
                f"{endpoint}{self.coll_code}/?q=*&fq=publicationDateY_i:[{self.start_year} TO {self.end_year}]&fl={hal_fl}&rows=1000&sort=docid asc&wt=json"
            ).json()['response']['docs']:
                for t in d['title_s']:
                    titres_coll.append(t)
                    docid_coll.append(d['docid'])
                    try:
                        dois_coll.append(d['doiId_s'].lower())
                    except KeyError:
                        dois_coll.append("")
                    submit_types_coll.append(d['submitType_s'])
                    try:
                        link_ext_url_coll.append(d['linkExtUrl_s'])
                    except KeyError:
                        link_ext_url_coll.append("")
                    try:
                        link_ext_id_coll.append(d['linkExtId_s'])  # Ajout ici
                    except KeyError:
                        link_ext_id_coll.append("")  # Ou une autre valeur par défaut
        return pd.DataFrame(
            {
                'Hal_ids': docid_coll,
                'DOIs': dois_coll,
                'Titres': titres_coll,
                'Types de dépôts': submit_types_coll,
                'HAL Link': link_ext_url_coll,
                'HAL Ext ID': link_ext_id_coll,  # Ajout de la nouvelle colonne
            }
        )
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

def normalize_name(name):
    name = name.strip().lower()
    name = ''.join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')
    name = name.replace('-', ' ')
    name = name.replace('.', '')
    name = re.sub(r'\s+', ' ', name)

    # Gérer les noms au format "Nom, Prénom"
    if ',' in name:
        parts = [part.strip() for part in name.split(',')]
        if len(parts) == 2:
            name = f"{parts[1]} {parts[0]}"

    return name

def get_initial_form(name):
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[0][0]} {parts[-1]}"
    return name