import os # Pour la variable d'environnement NCBI_API_KEY
import streamlit as st
import pandas as pd
import io
# Supprimé: requests, json, unicodedata, difflib, tqdm, concurrent
# Ces imports sont maintenant dans utils.py ou non nécessaires directement ici

# Importer les fonctions et constantes partagées depuis utils.py
from utils import (
    get_scopus_data, get_openalex_data, get_pubmed_data, convert_to_dataframe,
    clean_doi, HalCollImporter, merge_rows_with_sources, get_authors_from_crossref,
    check_df, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo,
    normalise, normalize_name, get_initial_form # normalise est utilisé par HalCollImporter et check_df
)
# Les constantes comme HAL_API_ENDPOINT sont utilisées par les fonctions dans utils.py

# --- Définition de la liste des laboratoires (spécifique à cette application) ---
labos_list_rennes = [
    {
        "collection": "CAPHI", "scopus_id": "60105490", "openalex_id": "I4387152714",
        "pubmed_query": "(CAPHI[Affiliation]) OR (\"CENTRE ATLANTIQUE DE PHILOSOPHIE\"[Affiliation]) OR (\"EA 7463\" [Affiliation]) OR (EA7463[Affiliation]) OR (UR7463[Affiliation]) OR (\"UR 7463\"[Affiliation])"
    },
    {
        "collection": "ARENES", "scopus_id": "60105601", "openalex_id": "I4387155702",
        "pubmed_query": "(ARENES[Affiliation]) OR (\"UMR6051\"[Affiliation]) OR (UMR 6051[Affiliation] OR OR (UMR CNRS 6051[Affiliation])"
    },
    {"collection": "CREAAH", "scopus_id": "60105602", "openalex_id": "I4387153012", "pubmed_query": ""},
    {
        "collection": "BIOSIT", "scopus_id": "60105514", "openalex_id": "I4210159878",
        "pubmed_query": "(3480[affiliation]) OR (biosit[affiliation]) OR (\"Biology Health Innovation and Technology\"[affiliation]) OR (\"Structure Fédérative de Recherche en Biologie Santé\"[affiliation]) OR (us018[affiliation]) OR (ImPACcell[affiliation])"
    },
    {
        "collection": "BRM", "scopus_id": "60206583", "openalex_id": "I4387155446", 
        "pubmed_query": "(U835[affiliation]) OR (UMR_S1230[affiliation]) OR (UMR1230[affiliation]) OR (U1230[affiliation]) OR (\"ARN régulateurs bactériens et médecine\"[affiliation]) OR (\"Bacterial regulatory RNAs and Medicine\"[affiliation]) OR (Tattevin[Author])"
    },
    {
        "collection": "CIC", "scopus_id": "60105521", "openalex_id": "I4210116274", 
        "pubmed_query": "((INSERM 1414[Affiliation]) OR (INSERM-CIC-1414[Affiliation]) OR (CIC-1414[Affiliation]) OR (0203[Affiliation]) OR (1414[Affiliation]) OR (INSERM 0203[Affiliation]) OR (Unité dʼInvestigation Clinique Rennes[Affiliation]) OR (Centre dʼInvestigation Clinique* Rennes[Affiliation]) OR (Clinical Investigation Center Rennes[Affiliation]) OR (Rennes Clinical Investigation Center[Affiliation]) NOT (U 804[Affiliation]) NOT (U804[Affiliation]) NOT (CIC-IT[Affiliation]) AND (rennes[Affiliation])) NOT (Inria[Affiliation]) NOT (FORTH/ICE-HT[Affiliation])"
    },
    {
        "collection": "OSS", "scopus_id": "60138518", "openalex_id": "I4210090689",
        "pubmed_query": "((u1242[Affiliation]) OR (u 1242[Affiliation]) OR (Oncogenesis, Stress and Signaling[Affiliation]) OR (COSS[Affiliation]) OR (ERL440[Affiliation]) OR (ER440[Affiliation]) AND (Rennes[Affiliation]))"
    },
    {
        "collection": "ECOBIO", "scopus_id": "60105587", "openalex_id": "I4210087209", 
        "pubmed_query": "((ecobio[Affiliation]) OR (6553[Affiliation]) OR (Écosystèmes, biodiversité, évolution[Affiliation]) OR (Ecosystems, Biodiversity, Evolution[Affiliation]) AND (rennes[Affiliation])) OR (paimpont[Affiliation])"
    },
    {
        "collection": "ETHOS", "scopus_id": "60105604", "openalex_id": "I4387154707",
        "pubmed_query": "((ethos[Affiliation] AND (rennes[Affiliation])) OR ((ethos[Affiliation] AND (caen[Affiliation])) OR (UMR6552[Affiliation]) OR (UMR 6552[Affiliation]) OR (Ethologie animale et humaine[Affiliation]) OR (animal and human ethology[Affiliation])"
    },
    {
        "collection": "FOTON", "scopus_id": "60105599", "openalex_id": "I4210138837",
        "pubmed_query": "(\"Fonctions Optiques pour les Technologies de l’information\"[Affiliation]) OR (UMR6082[Affiliation]) OR (UMR 6082[Affiliation])"
    },
    {
        "collection": "IETR", "scopus_id": "60105585", "openalex_id": "I4210100151",
        "pubmed_query": "(IETR[Affiliation]) OR (Institut d'Électronique et des Technologies du numéRique[Affiliation]) OR (UMR6164[Affiliation]) OR (UMR 6164[Affiliation])"
    },
    {
        "collection": "IGDR", "scopus_id": "60105597", "openalex_id": "I4210127029",
        "pubmed_query": "(IGDR[Affiliation]) OR (6290[Affiliation]) OR (Institut de génétique et développement de Rennes[Affiliation]) OR (Institute of Genetics and Development of Rennes[Affiliation]) NOT (Nikon[Affiliation])"
    },
    {
        "collection": "IPR", "scopus_id": "60105586", "openalex_id": "I4210109443",
        "pubmed_query": "((IPR[Affiliation]) AND (rennes[Affiliation])) OR (Institut de Physique de Rennes[Affiliation]) OR (UMR6251[Affiliation]) OR (UMR 6251[Affiliation]) NOT (Institut Pierre Richet[Affiliation]) NOT (Intelligent Process Automation and Robotics[Affiliation]) NOT (Sant Joan de Déu[Affiliation])"
    },
    {
        "collection": "IETR", "scopus_id": "60105585", "openalex_id": "I4210100151",
        "pubmed_query": "(IETR[Affiliation]) OR (CNRS 6164[Affiliation]) OR (UMR 6164[Affiliation]) OR (UMR6164[Affiliation]) OR (IETR Polytech[Affiliation]) OR (Institut d'Electronique et des Technologies du numéRique[Affiliation]) OR (Institut d'Électronique et de Télécommunications[Affiliation])"
    },
    {
        "collection": "IRSET", "scopus_id": "60105594", "openalex_id": "I4210108239",
        "pubmed_query": "(irset[Affiliation]) OR (U1085[Affiliation]) OR (UMR1085[Affiliation]) OR (UMR-S 1085[Affiliation]) OR (Institut de recherche en santé, environnement et travail[Affiliation]) OR (Research Institute for Environmental and Occupational Health[Affiliation]) OR (Institute for Research in Health Environment and Work[Affiliation]) NOT (IRSET-Center[Affiliation]) NOT (Kristensen[Author]"
    },
    {
        "collection": "ISCR", "scopus_id": "60072944", "openalex_id": "I4210090783",
        "pubmed_query": "((institut des sciences chimiques de rennes[Affiliation]) OR (6226[Affiliation]) OR (ISCR[Affiliation]) OR (ISCR-UMR[Affiliation]) OR (National Higher School of Chemistry[Affiliation]) OR (MACSE[Affiliation]) OR (CORINT[Affiliation]) OR (Glasses and Ceramics[Affiliation]) OR (Institute of Chemical Science[Affiliation]) OR (Institute for Chemical Science[Affiliation]) OR (Ecole nationale supérieure de chimie de Rennes[Affiliation]) OR (ENSCR[Affiliation]) AND (rennes[Affiliation]))"
    },
    {
        "collection": "LGCGM", "scopus_id": "60105557", "openalex_id": "I4387155956",
        "pubmed_query": "(LGCGM[Affiliation]) OR (UR 3913[Affiliation]) OR (UR3913[Affiliation]) OR (EA 3913[Affiliation]) OR (EA3913[Affiliation]) OR (\"Laboratoire de génie civil et génie mécanique\"[Affiliation] OR ((Quang Huy Nguyen[Author]) OR (Maël Couchaux[Author]) OR (Fabrice Bernard[Author]) OR (Paul Byrne[Author]) OR (Amina Meslem[Author]) OR (Florence Collet[Author]) OR (Mohammed Hjiaj[Author]) OR (Piseth Heng[Author]) OR (Hugues Somja[Author]) OR (Siham Kamali-Bernard[Author]) OR (Balaji Raghavan[Author]) AND (rennes[Affiliation]))"
    },
    {
        "collection": "LTSI", "scopus_id": "60105589", "openalex_id": "I4210105651",
        "pubmed_query": "((LTSI[Affiliation]) OR (1099[Affiliation]) OR (CRIBS[Affiliation]) OR (CIC-IT[Affiliation]) OR (804[Affiliation]) OR (medicis[Affiliation]) OR (Signal and Image Processing Laboratory[Affiliation] OR (\"Laboratoire traitement du signal et de l'image\"[Affiliation]) OR (\"Centre de Recherche en Information Biomédicale Sino-Français\"[Affiliation]) AND (Rennes[Affiliation]))"
    },
    {
        "collection": "M2S", "scopus_id": "60105531", "openalex_id": "I4210160484",
        "pubmed_query": "((UR 7470[Affiliation]) OR (UR7470[Affiliation]) OR (UR 1274[Affiliation]) OR (EA 7470[Affiliation]) OR (EA7470[Affiliation]) OR (EA 1274[Affiliation]) OR (EA1274[Affiliation]) OR (Laboratoire mouvement, sport et santé[Affiliation]) OR (Movement, Sport, Health[Affiliation]) OR (M2S[Affiliation]) AND (Rennes[Affiliation]))"
    },
    {
        "collection": "MOBIDIC", "scopus_id": "60105591", "openalex_id": "I4387154398",
        "pubmed_query": "(MOBIDIC[Affiliation]) OR (Microenvironment and B-cells: Immunopathology, Cell Differentiation, and Cancer[Affiliation]) OR (Microenvironment and B-cells and Cancer[Affiliation]) OR (micmac[Affiliation]) OR (Microenvironment, Cell Differentiation, Immunology and Cancer[Affiliation]) OR (UMR_S 1236[Affiliation]) OR (U1236[Affiliation]) OR (U 1236[Affiliation]) OR (UMR_S1236[Affiliation]) OR (u917[Affiliation]) OR (U 917[Affiliation]) OR (UMR_S917[Affiliation]) OR (UMR_S 917[Affiliation]) NOT (Educell Ltd[Affiliation]) NOT (MicMac Road[Affiliation]) NOT (Montpellier BioInformatics for Clinical Diagnosis[Affiliation]))"
    },
    {
        "collection": "NUMECAN", "scopus_id": "60112105", "openalex_id": "I4387156410",
        "pubmed_query": "(UMR991[affiliation]) OR (UMR 991[affiliation]) OR (U991[affiliation]) OR (U 991[affiliation]) OR (foie, métabolismes et cancer[affiliation]) OR (foie, metabolismes et cancer[affiliation]) OR (liver, metabolisms and cancer[affiliation]) OR (EA 1254[affiliation]) OR (EA1254[affiliation]) OR (microbiologie: risques infectieux[affiliation]) OR (U1317[Affiliation]) OR (U 1317[Affiliation]) OR (U-1317[Affiliation]) OR (UMR_S 1317[Affiliation]) OR (UMR 1317[Affiliation]) OR (U1241[Affiliation]) OR (U 1241[Affiliation]) OR (U-1241[Affiliation]) OR (UMR_S 1241[Affiliation]) OR (UMR 1241[Affiliation]) OR (UMR 1341[Affiliation]) OR (UMR INRA 1341[Affiliation]) OR (numecan[Affiliation]) OR (nutrition, métabolismes et cancer[Affiliation]) OR (nutrition, metabolismes et cancer[Affiliation]) OR (nutrition, metabolisms and cancer[Affiliation]) OR ((ARNAUD Alexis OR BELLANGER Amandine OR BUFFET-BATAILLON Sylvie OR MOIRAND Romain OR THIBAULT Ronan OR ARTRU Florent OR BENDAVID Claude OR BOUGUEN Guillaume OR CABILLIC Florian OR GICQUEL Thomas OR LE DARE Brendan OR LEBOUVIER Thomas OR MOREL Isabelle OR NESSELER Nicolas  OR PELLETIER Romain OR PEYRONNET Benoit OR RAYAR Michel OR  SIPROUDHIS Laurent OR GUGGENBUHL Pascal OR  BARDOU-JACQUET Edouard OR BONNET Fabrice OR DESCLOS-THEVENIAU Marie OR GARIN Etienne OR HAMDI-ROZE Houda OR LAINE Fabrice OR MEURIC Vincent OR RANGE Hélène OR ROBIN François OR ROPARS Mickael OR ROPERT Martine OR TURLIN Bruno) AND (Rennes[Affiliation]))"
    },
    {
        "collection": "SCANMAT", "scopus_id": "60138457", "openalex_id": "I4387156459",
        "pubmed_query": "((SCANMAT[affiliation]) OR (UMS2001[affiliation]) AND (rennes[Affiliation]))"
    },
    {
        "collection": "CREM", "scopus_id": "60105603", "openalex_id": "I4210088544",
        "pubmed_query": ""
    },
    {
        "collection": "VIPS2", "scopus_id": "60105580", "openalex_id": "I4387155754",
        "pubmed_query": "(VIPS2[Affiliation]) OR (Valeurs, Innovations, Politiques, Socialisations et Sports[Affiliation]) OR (UR 4636[Affiliation]) OR (UR4636[Affiliation])"
    },
]
labos_df_rennes_global = pd.DataFrame(labos_list_rennes)


# Fonction pour ajouter le menu de navigation (spécifique à cette app)
def add_sidebar_menu():
    st.sidebar.header("À Propos")
    st.sidebar.info(
    """
    **c2LabHAL - Version Université de Rennes** :
    Cette version est préconfigurée pour les laboratoires de l'Université de Rennes.
    Sélectionnez un laboratoire dans la liste pour lancer la comparaison de ses publications
    (Scopus, OpenAlex, PubMed) avec sa collection HAL. c2LabHAL est une application créée par Guillaume Godet (Nantes Univ)
    """
)
    st.sidebar.markdown("---")

    st.sidebar.header("Autres applications c2LabHAL")
    st.sidebar.markdown("📖 [c2LabHAL - Application Principale](https://c2labhal.streamlit.app/)")
    st.sidebar.markdown("📄 [c2LabHAL version CSV](https://c2labhal-csv.streamlit.app/)")


    st.sidebar.markdown("---")
    
    st.sidebar.markdown("Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("Code source :")
    st.sidebar.markdown("[🐙 Voir sur GitHub](https://github.com/GuillaumeGodet/c2labhal)")


def main():
    st.set_page_config(page_title="c2LabHAL - Rennes", layout="wide")
    add_sidebar_menu() 

    st.title("🥎 c2LabHAL - Version Université de Rennes")
    st.subheader("Comparez les publications d’un laboratoire de l'Université de Rennes avec sa collection HAL")

    labo_choisi_nom_rennes = st.selectbox(
        "Choisissez une collection HAL de laboratoire (Université de Rennes) :", 
        sorted(labos_df_rennes_global['collection'].unique())
    )

    labo_selectionne_details_rennes = labos_df_rennes_global[labos_df_rennes_global['collection'] == labo_choisi_nom_rennes].iloc[0]
    collection_a_chercher_rennes = labo_selectionne_details_rennes['collection']
    scopus_lab_id_rennes = labo_selectionne_details_rennes.get('scopus_id', '') 
    openalex_institution_id_rennes = labo_selectionne_details_rennes.get('openalex_id', '')
    pubmed_query_labo_rennes = labo_selectionne_details_rennes.get('pubmed_query', '')

    scopus_api_key_secret_rennes = st.secrets.get("SCOPUS_API_KEY")
    pubmed_api_key_secret_rennes = st.secrets.get("PUBMED_API_KEY")

    col1_dates_rennes, col2_dates_rennes = st.columns(2)
    with col1_dates_rennes:
        start_year_rennes = st.number_input("Année de début", min_value=1900, max_value=2100, value=2020, key="rennes_start_year")
    with col2_dates_rennes:
        end_year_rennes = st.number_input("Année de fin", min_value=1900, max_value=2100, value=pd.Timestamp.now().year, key="rennes_end_year")

    with st.expander("🔧 Options avancées pour les auteurs"):
        fetch_authors_rennes = st.checkbox("🧑‍🔬 Récupérer les auteurs via Crossref (peut ralentir)", value=False, key="rennes_fetch_authors_cb")
        compare_authors_rennes = False
        uploaded_authors_file_rennes = None
        if fetch_authors_rennes:
            compare_authors_rennes = st.checkbox("🔍 Comparer les auteurs avec une liste de chercheurs", value=False, key="rennes_compare_authors_cb")
            if compare_authors_rennes:
                uploaded_authors_file_rennes = st.file_uploader(
                    "📤 Téléversez un fichier CSV de chercheurs (colonnes: 'collection', 'prénom nom')", 
                    type=["csv"], 
                    key="rennes_upload_authors_fu",
                    help="Le fichier CSV doit avoir une colonne 'collection' (code de la collection HAL) et une colonne avec les noms des chercheurs."
                )
    
    progress_bar_rennes = st.progress(0)
    progress_text_area_rennes = st.empty() # Correction: Suffixe _rennes ajouté

    if st.button(f"🚀 Lancer la recherche pour {collection_a_chercher_rennes}"):
        if pubmed_api_key_secret_rennes and pubmed_query_labo_rennes:
            os.environ['NCBI_API_KEY'] = pubmed_api_key_secret_rennes

        scopus_df_rennes = pd.DataFrame()
        openalex_df_rennes = pd.DataFrame()
        pubmed_df_rennes = pd.DataFrame()

        # --- Étape 1 : Récupération OpenAlex ---
        if openalex_institution_id_rennes:
            with st.spinner(f"Récupération OpenAlex pour {collection_a_chercher_rennes}..."):
                progress_text_area_rennes.info("Étape 1/9 : Récupération des données OpenAlex...") # Corrigé
                progress_bar_rennes.progress(5) # Corrigé
                openalex_query_complet_rennes = f"authorships.institutions.id:{openalex_institution_id_rennes},publication_year:{start_year_rennes}-{end_year_rennes}"
                openalex_data_rennes = get_openalex_data(openalex_query_complet_rennes, max_items=5000)
                if openalex_data_rennes:
                    openalex_df_rennes = convert_to_dataframe(openalex_data_rennes, 'openalex')
                    openalex_df_rennes['Source title'] = openalex_df_rennes.apply(
                        lambda row: row.get('primary_location', {}).get('source', {}).get('display_name') if isinstance(row.get('primary_location'), dict) and row['primary_location'].get('source') else None, axis=1
                    )
                    openalex_df_rennes['Date'] = openalex_df_rennes.get('publication_date', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['doi'] = openalex_df_rennes.get('doi', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['id'] = openalex_df_rennes.get('id', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['Title'] = openalex_df_rennes.get('title', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    cols_to_keep_rennes = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                    openalex_df_rennes = openalex_df_rennes[[col for col in cols_to_keep_rennes if col in openalex_df_rennes.columns]]
                    if 'doi' in openalex_df_rennes.columns:
                        openalex_df_rennes['doi'] = openalex_df_rennes['doi'].apply(clean_doi)
                st.success(f"{len(openalex_df_rennes)} publications OpenAlex trouvées pour {collection_a_chercher_rennes}.")
        progress_bar_rennes.progress(10) # Corrigé

        # --- Étape 2 : Récupération PubMed ---
        if pubmed_query_labo_rennes: 
            with st.spinner(f"Récupération PubMed pour {collection_a_chercher_rennes}..."):
                progress_text_area_rennes.info("Étape 2/9 : Récupération des données PubMed...") # Corrigé
                progress_bar_rennes.progress(20) # Corrigé (ajusté pour être après l'info)
                pubmed_full_query_rennes = f"({pubmed_query_labo_rennes}) AND ({start_year_rennes}/01/01[Date - Publication] : {end_year_rennes}/12/31[Date - Publication])"
                pubmed_data_rennes = get_pubmed_data(pubmed_full_query_rennes, max_items=5000)
                if pubmed_data_rennes:
                    pubmed_df_rennes = pd.DataFrame(pubmed_data_rennes)
                st.success(f"{len(pubmed_df_rennes)} publications PubMed trouvées pour {collection_a_chercher_rennes}.")
        else:
            st.info(f"Aucune requête PubMed configurée pour {collection_a_chercher_rennes}.")
        progress_bar_rennes.progress(20) # Corrigé (ou 25 si on veut marquer la fin de l'étape)

        # --- Étape 3 : Récupération Scopus ---
        if scopus_lab_id_rennes and scopus_api_key_secret_rennes:
            with st.spinner(f"Récupération Scopus pour {collection_a_chercher_rennes}..."):
                progress_text_area_rennes.info("Étape 3/9 : Récupération des données Scopus...") # Corrigé
                progress_bar_rennes.progress(25) # Corrigé (ajusté)
                scopus_query_complet_rennes = f"AF-ID({scopus_lab_id_rennes}) AND PUBYEAR > {start_year_rennes - 1} AND PUBYEAR < {end_year_rennes + 1}"
                scopus_data_rennes = get_scopus_data(scopus_api_key_secret_rennes, scopus_query_complet_rennes, max_items=5000)
                if scopus_data_rennes:
                    scopus_df_raw_rennes = convert_to_dataframe(scopus_data_rennes, 'scopus')
                    required_scopus_cols_rennes = {'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate'}
                    if required_scopus_cols_rennes.issubset(scopus_df_raw_rennes.columns):
                        scopus_df_rennes = scopus_df_raw_rennes[['Data source', 'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']].copy()
                        scopus_df_rennes.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                        if 'doi' in scopus_df_rennes.columns:
                            scopus_df_rennes['doi'] = scopus_df_rennes['doi'].apply(clean_doi)
                    else:
                        st.warning(f"Données Scopus incomplètes pour {collection_a_chercher_rennes}. Scopus sera ignoré.")
                        scopus_df_rennes = pd.DataFrame()
                st.success(f"{len(scopus_df_rennes)} publications Scopus trouvées pour {collection_a_chercher_rennes}.")
        elif scopus_lab_id_rennes and not scopus_api_key_secret_rennes:
            st.warning(f"L'ID Scopus est fourni pour {collection_a_chercher_rennes} mais la clé API Scopus n'est pas configurée. Scopus sera ignoré.")
        progress_bar_rennes.progress(30) # Corrigé
        
        # --- Étape 4 : Combinaison des données ---
        progress_text_area_rennes.info("Étape 4/9 : Combinaison des données sources...") # Corrigé
        combined_df_rennes = pd.concat([scopus_df_rennes, openalex_df_rennes, pubmed_df_rennes], ignore_index=True)

        if combined_df_rennes.empty:
            st.error(f"Aucune publication récupérée pour {collection_a_chercher_rennes}. Vérifiez la configuration du laboratoire.")
            st.stop()
        
        if 'doi' not in combined_df_rennes.columns:
            combined_df_rennes['doi'] = pd.NA
        combined_df_rennes['doi'] = combined_df_rennes['doi'].astype(str).str.lower().str.strip().replace(['nan', 'none', 'NaN', ''], pd.NA, regex=False)


        # --- Étape 5 : Fusion des lignes en double ---
        progress_text_area_rennes.info("Étape 5/9 : Fusion des doublons...") # Corrigé
        progress_bar_rennes.progress(40) # Corrigé
        
        with_doi_df_rennes = combined_df_rennes[combined_df_rennes['doi'].notna()].copy()
        without_doi_df_rennes = combined_df_rennes[combined_df_rennes['doi'].isna()].copy()
        
        
        merged_data_doi_rennes = pd.DataFrame()
        if not with_doi_df_rennes.empty:
            merged_data_doi_rennes = with_doi_df_rennes.groupby('doi', as_index=False).apply(merge_rows_with_sources)
            if 'doi' not in merged_data_doi_rennes.columns and merged_data_doi_rennes.index.name == 'doi':
                merged_data_doi_rennes.reset_index(inplace=True)
            if isinstance(merged_data_doi_rennes.columns, pd.MultiIndex):
                 merged_data_doi_rennes.columns = merged_data_doi_rennes.columns.droplevel(0)
        
       
        merged_data_no_doi_rennes = pd.DataFrame()
        if not without_doi_df_rennes.empty:
            merged_data_no_doi_rennes = without_doi_df_rennes.copy() 
        
       
        final_merged_data_rennes = pd.concat([merged_data_doi_rennes, merged_data_no_doi_rennes], ignore_index=True)

        if final_merged_data_rennes.empty:
            st.error(f"Aucune donnée après fusion pour {collection_a_chercher_rennes}.")
            st.stop()
        st.success(f"{len(final_merged_data_rennes)} publications uniques après fusion pour {collection_a_chercher_rennes}.")
        progress_bar_rennes.progress(50) # Corrigé

        # --- Étape 6 : Comparaison HAL ---
        coll_df_hal_rennes = pd.DataFrame()
        with st.spinner(f"Importation de la collection HAL '{collection_a_chercher_rennes}'..."):
            progress_text_area_rennes.info(f"Étape 6a/9 : Importation de la collection HAL '{collection_a_chercher_rennes}'...") # Corrigé
            coll_importer_rennes_obj = HalCollImporter(collection_a_chercher_rennes, start_year_rennes, end_year_rennes)
            coll_df_hal_rennes = coll_importer_rennes_obj.import_data()
            if coll_df_hal_rennes.empty:
                st.warning(f"Collection HAL '{collection_a_chercher_rennes}' vide ou non chargée.")
            else:
                st.success(f"{len(coll_df_hal_rennes)} notices HAL pour {collection_a_chercher_rennes}.")
        
        progress_text_area_rennes.info("Étape 6b/9 : Comparaison avec les données HAL...") # Corrigé
        result_df_rennes = check_df(final_merged_data_rennes.copy(), coll_df_hal_rennes, progress_bar_st=progress_bar_rennes, progress_text_st=progress_text_area_rennes) # Passé les bons objets
        st.success(f"Comparaison HAL pour {collection_a_chercher_rennes} terminée.")
        # progress_bar_rennes est géré par check_df

        # --- Étape 7 : Enrichissement Unpaywall ---
        with st.spinner(f"Enrichissement Unpaywall pour {collection_a_chercher_rennes}..."):
            progress_text_area_rennes.info("Étape 7/9 : Enrichissement Unpaywall...") # Corrigé
            progress_bar_rennes.progress(70) # Corrigé (ajouté avant l'appel)
            result_df_rennes = enrich_w_upw_parallel(result_df_rennes.copy())
            st.success(f"Enrichissement Unpaywall pour {collection_a_chercher_rennes} terminé.")
        # progress_bar_rennes.progress(70) # Déplacé avant l'appel

        # --- Étape 8 : Permissions de dépôt ---
        with st.spinner(f"Récupération des permissions pour {collection_a_chercher_rennes}..."):
            progress_text_area_rennes.info("Étape 8/9 : Récupération des permissions de dépôt...") # Corrigé
            progress_bar_rennes.progress(80) # Corrigé (ajouté avant l'appel)
            result_df_rennes = add_permissions_parallel(result_df_rennes.copy())
            st.success(f"Permissions pour {collection_a_chercher_rennes} récupérées.")
        # progress_bar_rennes.progress(80) # Déplacé avant l'appel

        # --- Étape 9 : Déduction des actions et auteurs ---
        progress_text_area_rennes.info("Étape 9/9 : Déduction des actions et traitement des auteurs...") # Corrigé
        if 'Action' not in result_df_rennes.columns: result_df_rennes['Action'] = pd.NA
        result_df_rennes['Action'] = result_df_rennes.apply(deduce_todo, axis=1)

        if fetch_authors_rennes: 
            with st.spinner(f"Récupération des auteurs Crossref pour {collection_a_chercher_rennes}..."):
                if 'doi' in result_df_rennes.columns:
                    from concurrent.futures import ThreadPoolExecutor 
                    from tqdm import tqdm 

                    dois_for_authors_rennes = result_df_rennes['doi'].fillna("").tolist()
                    authors_results_rennes = []
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        authors_results_rennes = list(tqdm(executor.map(get_authors_from_crossref, dois_for_authors_rennes), total=len(dois_for_authors_rennes), desc="Auteurs Crossref (rennes)"))
                    
                    result_df_rennes['Auteurs_Crossref'] = ['; '.join(author_l) if isinstance(author_l, list) and not any("Erreur" in str(a) or "Timeout" in str(a) for a in author_l) else (author_l[0] if isinstance(author_l, list) and author_l else '') for author_l in authors_results_rennes]
                    st.success(f"Auteurs Crossref pour {collection_a_chercher_rennes} récupérés.")
                else:
                    st.warning("Colonne 'doi' non trouvée, impossible de récupérer les auteurs pour la version rennes.")
                    result_df_rennes['Auteurs_Crossref'] = ''
            
            if compare_authors_rennes and uploaded_authors_file_rennes:
                with st.spinner(f"Comparaison des auteurs (fichier) pour {collection_a_chercher_rennes}..."):
                    try:
                        user_authors_df_rennes_file = pd.read_csv(uploaded_authors_file_rennes)
                        if not ({'collection', user_authors_df_rennes_file.columns[1]} <= set(user_authors_df_rennes_file.columns)):
                            st.error("Fichier CSV auteurs mal formaté pour la version rennes.")
                        else:
                            author_name_col_rennes_file = user_authors_df_rennes_file.columns[1]
                            noms_ref_rennes_list = user_authors_df_rennes_file[user_authors_df_rennes_file["collection"].astype(str).str.lower() == str(collection_a_chercher_rennes).lower()][author_name_col_rennes_file].dropna().unique().tolist()
                            if not noms_ref_rennes_list:
                                st.warning(f"Aucun chercheur pour '{collection_a_chercher_rennes}' dans le fichier fourni (rennes).")
                            else:
                                chercheur_map_rennes_file = {normalize_name(n): n for n in noms_ref_rennes_list}
                                initial_map_rennes_file = {get_initial_form(normalize_name(n)): n for n in noms_ref_rennes_list}
                                from difflib import get_close_matches 

                                def detect_known_authors_rennes_file(authors_str_rennes):
                                    if pd.isna(authors_str_rennes) or not str(authors_str_rennes).strip() or "Erreur" in authors_str_rennes or "Timeout" in authors_str_rennes: return ""
                                    authors_pub_rennes = [a.strip() for a in str(authors_str_rennes).split(';') if a.strip()]
                                    detectes_originaux_rennes = set()
                                    for author_o_rennes in authors_pub_rennes:
                                        author_n_rennes = normalize_name(author_o_rennes)
                                        author_i_n_rennes = get_initial_form(author_n_rennes)
                                        match_c_rennes = get_close_matches(author_n_rennes, chercheur_map_rennes_file.keys(), n=1, cutoff=0.85)
                                        if match_c_rennes:
                                            detectes_originaux_rennes.add(chercheur_map_rennes_file[match_c_rennes[0]])
                                            continue
                                        match_i_rennes = get_close_matches(author_i_n_rennes, initial_map_rennes_file.keys(), n=1, cutoff=0.9)
                                        if match_i_rennes:
                                            detectes_originaux_rennes.add(initial_map_rennes_file[match_i_rennes[0]])
                                    return "; ".join(sorted(list(detectes_originaux_rennes))) if detectes_originaux_rennes else ""
                                result_df_rennes['Auteurs_Laboratoire_Détectés'] = result_df_rennes['Auteurs_Crossref'].apply(detect_known_authors_rennes_file)
                                st.success(f"Comparaison auteurs (fichier) pour {collection_a_chercher_rennes} terminée.")
                    except Exception as e_auth_file_rennes_exc:
                        st.error(f"Erreur fichier auteurs (rennes): {e_auth_file_rennes_exc}")
            elif compare_authors_rennes and not uploaded_authors_file_rennes:
                 st.warning("Veuillez téléverser un fichier CSV de chercheurs pour la comparaison des auteurs (rennes).")

        progress_bar_rennes.progress(90) # Corrigé
        st.success(f"Déduction des actions et traitement des auteurs pour {collection_a_chercher_rennes} terminés.")
        
        st.dataframe(result_df_rennes)

        if not result_df_rennes.empty:
            csv_export_rennes_data = result_df_rennes.to_csv(index=False, encoding='utf-8-sig')
            output_filename_rennes_final = f"c2LabHAL_resultats_{collection_a_chercher_rennes.replace(' ', '_')}_{start_year_rennes}-{end_year_rennes}.csv"
            st.download_button(
                label=f"📥 Télécharger les résultats pour {collection_a_chercher_rennes}",
                data=csv_export_rennes_data,
                file_name=output_filename_rennes_final,
                mime="text/csv",
                key=f"download_rennes_{collection_a_chercher_rennes}"
            )
        progress_bar_rennes.progress(100) # Corrigé
        progress_text_area_rennes.success(f"🎉 Traitement pour {collection_a_chercher_rennes} terminé avec succès !") # Corrigé

if __name__ == "__main__":
    main()
