

import os
import streamlit as st
import pandas as pd
import io
from streamlit_app import (
    get_scopus_data, get_openalex_data, get_pubmed_data, convert_to_dataframe,
    clean_doi, HalCollImporter, merge_rows_with_sources, get_authors_from_crossref,
    check_df, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo, normalise
)

# Liste des laboratoires avec leurs informations
labos_list = [
    {
        "collection": "CAPHI",
        "scopus_id": "60105490",
        "openalex_id": "I4387152714",
        "pubmed_query": "(CAPHI[Affiliation]) OR (\"CENTRE ATLANTIQUE DE PHILOSOPHIE\"[Affiliation]) OR (\"EA 7463\" [Affiliation]) OR (EA7463[Affiliation]) OR (UR7463[Affiliation]) OR (\"UR 7463\"[Affiliation])"
    },
    {
        "collection": "CFV",
        "scopus_id": "60105524",
        "openalex_id": "I4387153064",
        "pubmed_query": "(CFV[Affiliation]) OR (\"EA 1161\"[Affiliation]) OR (Viete[Affiliation])"
    },
    {
        "collection": "CReAAH",
        "scopus_id": "60105602",
        "openalex_id": "I4387153012",
        "pubmed_query": "CReAAH[Affiliation] OR (LARA[Affiliation]) OR (6566[Affiliation])"
    },
    {
        "collection": "CREN",
        "scopus_id": "60105539",
        "openalex_id": "I4387152322",
        "pubmed_query": "(CREN[Affiliation]) OR (2661[Affiliation]) OR (\"Ctr Rech Educ\"[Affiliation])"
    },
    {
        "collection": "CRHIA",
        "scopus_id": "60105526",
        "openalex_id": "I4399598365",
        "pubmed_query": "(CRHIA[Affiliation]) OR (1163[Affiliation]) OR (\"Ctr Rech Hist Int\"[Affiliation])"
    },
    {
        "collection": "CRINI",
        "scopus_id": "60105525",
        "openalex_id": "I4387153799",
        "pubmed_query": "(CRINI[Affiliation]) OR (1162[Affiliation]) OR (\"Ctr Rech Identites Nations\"[Affiliation])"
    },
    {
        "collection": "ESO",
        "scopus_id": "60105581",
        "openalex_id": "I4387153532",
        "pubmed_query": "(\"UMR 6590\"[Affiliation]) OR (\"Espaces et Sociétés\"[Affiliation]) OR (ESO Nantes[Affiliation]) OR (ESO-Angers[Affiliation]) NOT (\"UFR Sciences Espaces et Sociétés\"[Affiliation])"
    },
    {
        "collection": "LAMO",
        "scopus_id": "60105566",
        "openalex_id": "I4387152722",
        "pubmed_query": "(\"L'AMO\"[Affiliation]) OR (LAMO[Affiliation]) OR (4276[Affiliation])"
    },
    {
        "collection": "LETG",
        "scopus_id": "60105608",
        "openalex_id": "I4387153176",
        "pubmed_query": "(LETG[Affiliation]) OR (UMR6554[Affiliation]) OR (UMR 6554[Affiliation]) OR (UMR CNRS 6554[Affiliation]) OR (Geolittomer[Affiliation])"
    },
    {
        "collection": "LLING",
        "scopus_id": "60105540",
        "openalex_id": "I4387152679",
        "pubmed_query": "(\"Laboratoire de linguistique de Nantes\"[Affiliation])"
    },
    {
        "collection": "LPPL",
        "scopus_id": "60105621",
        "openalex_id": "I4210089331",
        "pubmed_query": "(LPPL[Affiliation]) OR (Laboratoire de Psychologie des Pays de la Loire[Affiliation]) OR (EA 4638[Affiliation]) OR (EA4638[Affiliation]) OR (EA 3259[Affiliation]) OR (EA3259[Affiliation]) OR (EA 2646[Affiliation])"
    },
    {
        "collection": "CEISAM",
        "scopus_id": "60105571",
        "openalex_id": "I4210138474",
        "pubmed_query": "(CEISAM[Affiliation]) OR (UMR6230[Affiliation]) OR (UMR 6230[Affiliation]) OR (LAIEM[Affiliation]) OR (Laboratory of Isotopic and Electrochemical Analysis of Metabolism[Affiliation]) OR (Laboratoire d'Analyse Isotopique et Electrochimique des Métabolismes[Affiliation]) OR (Chimie et Interdisciplinarite Synthese Analyse Modelisation[Affiliation]) OR (CNRS 6230[Affiliation]) OR (CNRS6230[Affiliation]) OR (Interdisciplinary Chemistry Synthesis Analysis Modelling[Affiliation]) OR (CNRS 6513[Affiliation]) OR ((Laboratoire de Synthèse Organique[Affiliation]) AND (nantes[Affiliation])) OR (EA 1149[Affiliation]) OR ((Laboratoire de Spectrochimie[Affiliation]) AND (nantes[Affiliation]) NOT (villeneuve[Affiliation])) OR (UMR 6513[Affiliation]) OR (UMR6006[Affiliation]) OR (UMR 6006[Affiliation])"
    },
    {
        "collection": "GeM",
        "scopus_id": "60105606",
        "openalex_id": "I4210137520",
        "pubmed_query": "((GEM[Affiliation]) AND ((nazaire[Affiliation]) OR (nantes[Affiliation])) NOT (chicago[Affiliation])) OR (UMR 6183[Affiliation]) OR (UMR6183[Affiliation]) OR (CNRS 6183[Affiliation]) OR ((Laboratoire de Mécanique et Matériaux [Affiliation]) AND (nantes[Affiliation])) OR (Institute for Research in Civil and Mechanical Engineering[Affiliation]) OR (Research Institute in Civil Engineering and Mechanics[Affiliation]) OR (Institut de Recherche en Génie Civil[Affiliation]) OR (Research Institute in Civil and Mechanical Engineering[Affiliation])"
    },
    {
        "collection": "GEPEA",
        "scopus_id": "60105518",
        "openalex_id": "I4210148006",
        "pubmed_query": "((GEPEA[Affiliation]) AND ((nantes[Affiliation]) OR (nazaire[Affiliation]) OR (angers[ad) OR (Nntes Université[Affiliation]))) OR (UMR6144[Affiliation]) OR (UMR 6144[Affiliation]) OR (CNRS 6144[Affiliation]) OR (Génie des Procédés Environnement[Affiliation]) AND ((nantes[Affiliation]) OR (nazaire[Affiliation]))"
    },
    {
        "collection": "IETR",
        "scopus_id": "60105585",
        "openalex_id": "I4210100151",
        "pubmed_query": "(IETR[Affiliation]) OR (CNRS 6164[Affiliation]) OR (UMR 6164[Affiliation]) OR (UMR6164[Affiliation]) OR (IETR Polytech[Affiliation]) OR (Institut d'Electronique et des Technologies du numéRique[Affiliation]) OR (Institut d'Électronique et de Télécommunications[Affiliation])"
    },
    {
        "collection": "IMN",
        "scopus_id": "60020658",
        "openalex_id": "I4210091049",
        "pubmed_query": "((IMN[Affiliation]) AND (nantes[Affiliation])) OR (CNRS 6502[Affiliation]) OR ((UMR 6502[Affiliation]) AND (nantes[Affiliation])) OR (UMR6502[Affiliation]) OR (Inst. des Mat. Jean Rouxel[Affiliation]) OR (Institut des Matériaux de Nantes Jean Rouxel[Affiliation]) OR (Institut des Matériaux Jean Rouxel[Affiliation])"
    },
    {
        "collection": "IREENA",
        "scopus_id": "60105577",
        "openalex_id": "I4392021119",
        "pubmed_query": "(IREENA[Affiliation]) OR (EA 4642[Affiliation]) OR (Institut de Recherche en Energie Electrique de Nantes Atlantique[Affiliation]) OR (Institute of Research in Electric Power of Nantes Atlantique[Affiliation]) OR (Institut de Recherche en Electrotechnique et Electronique de Nantes Atlantique[Affiliation]) OR (Institut de Recherche en Electronique et Electrotechnique de Nantes Atlantique[Affiliation])"
    },
    {
        "collection": "LMJL",
        "scopus_id": "60105520",
        "openalex_id": "I4210153365",
        "pubmed_query": "(LMJL[Affiliation]) OR ((Leray[Affiliation]) AND (nantes[Affiliation]) NOT ((Leray[au]) OR (Le Ray[au]))) OR (CNRS 6629[Affiliation]) OR (UMR 6629[Affiliation]) OR (Laboratoire de mathématiques Jean Leray[Affiliation]) OR (Department of Mathematics Jean Leray[Affiliation]) OR (Laboratoire Jean Leray[Affiliation]) OR (Laboratory of Mathematics Jean Leray[Affiliation])"
    },
    {
        "collection": "LPG",
        "scopus_id": "60105669",
        "openalex_id": "I4210146808",
        "pubmed_query": "((LPG[Affiliation]) AND (france[Affiliation])) OR (CNRS 6112[Affiliation]) OR (UMR 6112[Affiliation]) OR (UMR6112[Affiliation]) OR (LPGN[Affiliation]) OR (Laboratoire de Planétologie et Géodynamique[Affiliation]) OR (Laboratoire de Planétologie et Géosciences[Affiliation]) OR (Laboratorie du Planétologie et Géosciences[Affiliation])"
    },
    {
        "collection": "LS2N",
        "scopus_id": "60110511",
        "openalex_id": "I4210117005",
        "pubmed_query": "(LS2N[Affiliation]) OR (UMR 6004[Affiliation]) OR (UMR6004[Affiliation]) OR ((Cnrs 6004[Affiliation]) AND (nantes[Affiliation])) OR (Laboratoire des Sciences du Numérique[Affiliation]) OR ((Laboratory of Digital Sciences[Affiliation]) NOT (orsay[Affiliation])) OR (IRCCYN[Affiliation]) OR (Cnrs 6597[Affiliation]) OR (Umr 6597[Affiliation]) OR (UMR_C 6597[Affiliation]) OR (Institut de Recherche en Communications et Cybernétique de Nantes[Affiliation]) OR (Research Institute in Communications and Cybernetics of Nantes[Affiliation]) OR (UMR 6241[Affiliation]) OR (UMR6241[Affiliation]) OR (CNRS 6241[Affiliation]) OR (Computer Science Institute of Nantes-Atlantic[Affiliation]) OR (Computer Science Laboratory of Nantes Atlantique[Affiliation]) OR (Laboratoire d'Informatique de Nantes-Atlantique[Affiliation])"
    },
    {
        "collection": "LTeN",
        "scopus_id": "60105570",
        "openalex_id": "I4210109587",
        "pubmed_query": "((LTEN[Affiliation]) NOT (Louisville[Affiliation])) OR ((LTN[Affiliation]) AND (nantes[Affiliation])) OR (UMR 6607[Affiliation]) OR (CNRS 6607[Affiliation]) OR (Laboratoire de Thermocinétique[Affiliation]) OR (Laboratoire de Thermique et Energie de Nantes[Affiliation]) OR (Laboratoire Thermique et Energie[Affiliation])"
    },
    {
        "collection": "SUBATECH",
        "scopus_id": "60008689",
        "openalex_id": "I4210109007",
        "pubmed_query": "(SUBATECH[Affiliation]) OR (UMR 6457[Affiliation]) OR (UMR6457[Affiliation]) OR (CNRS 6457[Affiliation]) OR (laboratoire de physique subatomique et des technologies associées[Affiliation])"
    },
    {
        "collection": "US2B",
        "scopus_id": "60276652",
        "openalex_id": "I4387154840",
        "pubmed_query": "((US2B[Affiliation]) NOT (bordeaux[Affiliation])) OR (UMR6286[Affiliation]) OR (UMR 6286[Affiliation]) OR (CNRS 6286[Affiliation]) OR ((UFIP[Affiliation]) NOT ((spain[Affiliation]) OR (España[Affiliation]))) OR (Biological Sciences and Biotechnologies unit[Affiliation]) OR (Unité en Sciences Biologiques et Biotechnologies[Affiliation]) OR (Fonctionnalité et Ingénierie des Protéines[Affiliation]) OR (Unit Function & Protein Engineering[Affiliation]) OR (Protein Engineering and Functionality Unit[Affiliation]) OR (Laboratoire de Biologie et Pathologie Végétales[Affiliation]) OR ((LBPV[Affiliation]) AND (nantes[Affiliation])) OR (Laboratory of Plant Biology and Pathology[Affiliation]) OR (EA 1157[Affiliation]) OR (EA1157[Affiliation])"
    },
    {
        "collection": "CR2TI",
        "scopus_id": "60105579",
        "openalex_id": "I4392021198",
        "pubmed_query": "((CRTI[Affiliation]) AND (Nantes[Affiliation])) OR (CRT2I[Affiliation]) OR (CR2TI[Affiliation]) OR ((UMR 1064[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1064[Affiliation]) NOT (Sophia Antipolis[Affiliation])) OR (Unité Mixte de Recherche 1064[Affiliation]) OR ((ITUN[Affiliation]) AND (nantes[Affiliation])) OR (Institut de Transplantation Urologie Néphrologie[Affiliation]) OR (U 643[Affiliation]) OR (U643[Affiliation]) OR ((Department of Nephrology and Immunology[Affiliation]) AND (nantes[Affiliation])) OR (Centre de Recherche en Transplantation et Immunologie[Affiliation]) OR (Center for Research in Transplantation and Immunology[Affiliation]) OR (Center for Research in Transplantation and Translational Immunology[Affiliation]) OR (Institut de Transplantation et de Recherche en Transplantation Urologie Néphrologie[Affiliation]) OR (U1064[Affiliation]) AND (nantes[Affiliation]) OR (Centre de recherche translationnelle en transplantation et immunologie[Affiliation]) OR (INSERM CR1064[Affiliation]) OR (Institut National de la Santé et de la Recherche Médicale 1064[Affiliation]) OR (INSERM Unité Mixte de Recherche 1064[Affiliation]) OR (Inserm 1064[Affiliation])"
    },
    {
        "collection": "CRCI2NA",
        "scopus_id": "60117278",
        "openalex_id": "I4210092509",
        "pubmed_query": "(CRCI2NA[Affiliation]) OR (CRC2INA[Affiliation]) OR ((CRCINA[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR (UMR 1307[Affiliation]) OR (UMR1307[Affiliation]) OR (U1307[Affiliation]) OR (UMR 6075[Affiliation]) OR (UMR6075[Affiliation]) OR (ERL6075[Affiliation]) OR ((ERL6001[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((ERL 6001[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((UMR 1232[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((UMR1232[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((Inserm 1232[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((Nantes-Angers Cancer Research Center[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((CRCNA[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR (Centre de Recherche en Cancérologie et Immunologie Intégrée[Affiliation]) OR ((Centre de Recherche en Cancérologie et Immunologie[Affiliation]) AND ((2022[dp]) OR (2023[dp]))) OR ((Center for Research in Cancerology and Immunology[Affiliation]) AND ((2022[dp]) OR (2023[dp])))"
    },
    {
        "collection": "IICIMED",
        "scopus_id": "60105522",
        "openalex_id": "I4387930219",
        "pubmed_query": "((IICIMED[Affiliation]) AND (nantes[Affiliation])) OR (UR 1155[Affiliation]) OR (UR1155[Affiliation]) OR (EA 1155[Affiliation]) OR (EA1155[Affiliation]) OR (Cibles et Médicaments des Infections et du Cancer[Affiliation]) OR (Cibles et médicaments des infections et de l'immunité[Affiliation]) OR (Cibles et Médicaments des Infections de l'Immunité et du Cancer[Affiliation]) OR (cibles et medicaments des infections et du l immunite[Affiliation])"
    },
    {
        "collection": "INCIT",
        "scopus_id": "60276656",
        "openalex_id": "I4392021193",
        "pubmed_query": "((INCIT[Affiliation]) AND ((nantes[Affiliation]) OR (angers[Affiliation]))) OR ((UMR 1302[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1302[Affiliation]) AND (nantes[Affiliation])) OR (EMR6001[Affiliation]) OR (Immunology and New Concepts in ImmunoTherapy[Affiliation])"
    },
    {
        "collection": "ISOMER",
        "scopus_id": "60105535",
        "openalex_id": "I4392021232",
        "pubmed_query": "((ISOMER[Affiliation]) AND (nantes[Affiliation])) OR ((MMS[Affiliation]) AND ((nantes[Affiliation]) OR (st nazaire[Affiliation]) OR (angers[Affiliation]) OR (le mans[Affiliation]))) OR (UR 2160[Affiliation]) OR (UR2160[Affiliation]) OR (EA 2160[Affiliation]) OR (EA2160[Affiliation]) OR (MMS EA 2160[Affiliation]) OR ((MicroMar[Affiliation]) AND (france[Affiliation])) OR (Institut des Substances et Organismes de la Mer[Affiliation]) OR (Mer Molécules Santé[Affiliation]) OR (Sea Molecules Health[Affiliation])"
    },
    {
        "collection": "MIP",
        "scopus_id": "60105638",
        "openalex_id": "I4392021216",
        "pubmed_query": "((MIP[Affiliation]) AND ((mans[Affiliation]) OR (nantes[Affiliation]))) OR (EA 4334[Affiliation]) OR (EA4334[Affiliation]) OR (UR 4334[Affiliation]) OR (UR4334[Affiliation]) OR (Movement Interactions Performance[Affiliation]) OR (Motricité Interactions Performance[Affiliation]) OR (mouvement interactions performance[Affiliation])"
    },
    {
        "collection": "PHAN",
        "scopus_id": "60105574",
        "openalex_id": "I4210162532",
        "pubmed_query": "((PHAN[Affiliation]) AND (nantes[Affiliation]) AND (1280[Affiliation])) OR (UMR 1280[Affiliation]) OR (UMR1280[Affiliation]) OR (Physiologie des Adaptations Nutritionnelles[Affiliation]) OR (Unité Mixte de Recherche 1280[Affiliation]) OR (Physiology of Nutritional Adaptations[Affiliation]) OR (Physiopathologie des Adaptations Nutritionnelles[Affiliation]) OR (Physiopathology of Nutritional Adaptations[Affiliation])"
    },
    {
        "collection": "RMeS",
        "scopus_id": "60117279",
        "openalex_id": "I4387152865",
        "pubmed_query": "((RMES[Affiliation]) AND (nantes[Affiliation])) OR (UMRS 1229[Affiliation]) OR (UMR S 1229[Affiliation]) OR ((UMR 1229[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1229[Affiliation]) AND (nantes[Affiliation])) OR (U 1229[Affiliation]) OR ((U1229[Affiliation]) AND (nantes[Affiliation])) OR (LIOAD[Affiliation]) OR (UMRS791[Affiliation]) OR (UMRS 791[Affiliation]) OR (UMR S 791[Affiliation]) OR (U 791[Affiliation]) OR (U791[Affiliation]) OR ((UMR 791[Affiliation]) AND (nantes[Affiliation])) OR ((UMR791[Affiliation]) AND (nantes[Affiliation])) OR (Regenerative Medicine and Skeleton[Affiliation]) OR (Osteoarticular and Dental Tissue Engineering[Affiliation]) OR (Laboratoire d'Ingénierie Ostéo-Articulaire et Dentaire[Affiliation]) OR (Ingénierie des Tissus Ostéo-Articulaires et Dentaires[Affiliation]) OR (osteo-articular and dental tissue engineering[Affiliation])"
    },
    {
        "collection": "SPHERE",
        "scopus_id": "60117638",
        "openalex_id": "I4392021239",
        "pubmed_query": "((SPHERE[Affiliation]) AND (1246[Affiliation])) OR (U 1246[Affiliation]) OR (U1246[Affiliation]) OR (UMR 1246[Affiliation]) OR (UMR1246[Affiliation]) OR (UMR S 1246[Affiliation]) OR (INSERM 1246[Affiliation]) OR (MethodS in Patients-centered outcomes and HEalth Research[Affiliation])"
    },
    {
        "collection": "TaRGeT",
        "scopus_id": "60105668",
        "openalex_id": "I4392021141",
        "pubmed_query": "((TARGET[Affiliation]) AND ((nantes université[Affiliation]) OR (nantes university[Affiliation]))) OR ((U1089[Affiliation]) AND (nantes[Affiliation])) OR (UMR S 1089[Affiliation]) OR ((UMR 1089[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1089[Affiliation]) AND (nantes[Affiliation])) OR ((Laboratoire de Thérapie Génique[Affiliation]) AND (nantes[Affiliation])) OR (thérapie génique translationnelle des maladies génétiques[Affiliation]) OR (Translational Gene Therapy for Genetic Diseases[Affiliation]) OR (Gene Therapy Laboratory[Affiliation])"
    },
    {
        "collection": "TENS",
        "scopus_id": "60105652",
        "openalex_id": "I4210108033",
        "pubmed_query": "((TENS[Affiliation]) AND (nantes[Affiliation])) OR ((UMR 1235[Affiliation]) AND (nantes[Affiliation])) OR ((UMR1235[Affiliation]) AND (nantes[Affiliation])) OR ((U 1235[Affiliation]) AND (nantes[Affiliation])) OR ((U1235[Affiliation]) AND (nantes[Affiliation])) OR (U 913[Affiliation]) OR (U913[Affiliation]) OR (UMR 913[Affiliation]) OR (UMR913[Affiliation]) OR (UMR S 913[Affiliation]) OR (The Enteric Nervous System in Gut and Brain Diseases[Affiliation]) OR (neuropathies du système nerveux entérique[Affiliation])"
    },
    {
        "collection": "ITX",
        "scopus_id": "60105651",
        "openalex_id": "I4210144168",
        "pubmed_query": "(Umr1087[Affiliation]) OR (Umr 1087[Affiliation]) OR (UMR S 1087[Affiliation]) OR (UMR 6291[Affiliation]) OR (UMRS1087[Affiliation]) OR (UMR6291[Affiliation]) OR ((Inst Thorax[Affiliation])) AND (nantes[Affiliation]) OR (l'institut du thorax[Affiliation]) OR (Institut du Thorax[Affiliation])"
    },
    {
        "collection": "CDMO",
        "scopus_id": "60105527",
        "openalex_id": "I4392021194",
        "pubmed_query": ""
    },
    {
        "collection": "CENS",
        "scopus_id": "60105489",
        "openalex_id": "I4210153136",
        "pubmed_query": ""
    },
    {
        "collection": "DCS",
        "scopus_id": "60105572",
        "openalex_id": "I4210100746",
        "pubmed_query": ""
    },
    {
        "collection": "IRDP",
        "scopus_id": "60105528",
        "openalex_id": "I4392021099",
        "pubmed_query": ""
    },
    {
        "collection": "LEMNA",
        "scopus_id": "60105575",
        "openalex_id": "I4390039323",
        "pubmed_query": ""
    },
    {
        "collection": "LHEEA",
        "scopus_id": "60105605",
        "openalex_id": "I4210153154",
        "pubmed_query": "(LHEEA[Affiliation]) OR (UMR 6598[Affiliation]) OR (UMR6598[Affiliation]) OR (CNRS 6598[Affiliation]) OR (Research Laboratory in Hydrodynamics, Energetics & Atmospheric Environment [Affiliation]) OR (Laboratoire de recherche en hydrodynamique[Affiliation])"
    },
    {
        "collection": "AAU",
        "scopus_id": "60110513",
        "openalex_id": "I4210162214",
        "pubmed_query": ""
    }
]

# Convertir la liste en DataFrame
labos_df = pd.DataFrame(labos_list)

def main():
    st.title("🥎 c2LabHAL - Version Nantes")
    st.subheader("Comparez les publications d’un labo nantais avec sa collection HAL")

    # Chargement des labos depuis la liste
    labos_df = pd.DataFrame(labos_list)

    # Sélection du labo
    labo_choisi = st.selectbox("Choisissez une collection HAL", labos_df['collection'].unique())

    # Récupération des infos correspondantes
    row = labos_df[labos_df['collection'] == labo_choisi].iloc[0]
    collection_a_chercher = row['collection']
    scopus_lab_id = row['scopus_id']
    openalex_institution_id = row['openalex_id']
    pubmed_query = row['pubmed_query']

    
    # Clés API depuis Streamlit secrets
    scopus_api_key = st.secrets["SCOPUS_API_KEY"]
    pubmed_api_key = st.secrets["PUBMED_API_KEY"]

    # Paramètres supplémentaires
    col1, col2 = st.columns(2)
    with col1:
        start_year = st.number_input("Année de début", min_value=1900, max_value=2100, value=2020)
    with col2:
        end_year = st.number_input("Année de fin", min_value=1900, max_value=2100, value=2025)

    fetch_authors = st.checkbox("Récupérer les auteurs sur Crossref", value=True)

    progress_bar = st.progress(0)
    progress_text = st.empty()

    if st.button("Rechercher"):
        # Initialiser les DataFrames
        scopus_df = pd.DataFrame()
        openalex_df = pd.DataFrame()
        pubmed_df = pd.DataFrame()

        # Étape 1 : OpenAlex
        with st.spinner("OpenAlex"):
            progress_text.text("Étape 1 : OpenAlex")
            progress_bar.progress(10)
            openalex_query = f"institutions.id:{openalex_institution_id},publication_year:{start_year}-{end_year}"
            openalex_data = get_openalex_data(openalex_query)
            openalex_df = convert_to_dataframe(openalex_data, 'openalex')
            openalex_df['Source title'] = openalex_df.apply(
                lambda row: row['primary_location']['source']['display_name'] if row['primary_location'] and row['primary_location'].get('source') else None, axis=1
            )
            openalex_df['Date'] = openalex_df.apply(lambda row: row.get('publication_date', None), axis=1)
            openalex_df['doi'] = openalex_df.apply(lambda row: row.get('doi', None), axis=1)
            openalex_df['id'] = openalex_df.apply(lambda row: row.get('id', None), axis=1)
            openalex_df['title'] = openalex_df.apply(lambda row: row.get('title', None), axis=1)
            openalex_df = openalex_df[['source', 'title', 'doi', 'id', 'Source title', 'Date']]
            openalex_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
            openalex_df['doi'] = openalex_df['doi'].apply(clean_doi)

        # Étape 2 : PubMed
        with st.spinner("PubMed"):
            progress_text.text("Étape 2 : PubMed")
            progress_bar.progress(30)
            pubmed_query = f"{pubmed_query} AND {start_year}/01/01:{end_year}/12/31[Date - Publication]"
            pubmed_data = get_pubmed_data(pubmed_query)
            pubmed_df = pd.DataFrame(pubmed_data)

        # Étape 3 : Scopus
        with st.spinner("Scopus"):
            progress_text.text("Étape 3 : Scopus")
            progress_bar.progress(50)
            scopus_query = f"af-ID({scopus_lab_id}) AND PUBYEAR > {start_year - 1} AND PUBYEAR < {end_year + 1}"
            scopus_data = get_scopus_data(scopus_api_key, scopus_query)
            scopus_df = pd.DataFrame()

        if scopus_data:
            raw_df = convert_to_dataframe(scopus_data, 'scopus')
            expected_cols = ['dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']
    
        # Vérifie que toutes les colonnes attendues sont présentes
            if all(col in raw_df.columns for col in expected_cols):
                scopus_df = raw_df[['source'] + expected_cols]
                scopus_df.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
            else:
                st.warning("Les données Scopus sont incomplètes ou mal formées.")
        else:
            st.info("Aucune donnée Scopus récupérée.")

        # Étape 4 : Comparaison avec HAL
        with st.spinner("HAL"):
            progress_text.text("Étape 4 : HAL")
            progress_bar.progress(70)
            combined_df = pd.concat([scopus_df, openalex_df, pubmed_df], ignore_index=True)
            coll = HalCollImporter(collection_a_chercher, start_year, end_year)
            coll_df = coll.import_data()
            coll_df['nti'] = coll_df['Titres'].apply(lambda x: normalise(x).strip())
            check_df(combined_df, coll_df, progress_bar=progress_bar, progress_text=progress_text)

        # Étape 5 : Fusion
        with st.spinner("Fusion"):
            progress_text.text("Étape 5 : Fusion des lignes en double")
            progress_bar.progress(90)
            with_doi = combined_df.dropna(subset=['doi'])
            without_doi = combined_df[combined_df['doi'].isna()]
            merged_with_doi = with_doi.groupby('doi', as_index=False).apply(merge_rows_with_sources)
            merged_data = pd.concat([merged_with_doi, without_doi], ignore_index=True)

        # Étape 6 : Auteurs Crossref
        if fetch_authors:
            with st.spinner("Auteurs Crossref"):
                progress_text.text("Étape 6 : Auteurs Crossref")
                progress_bar.progress(95)
                merged_data['Auteurs'] = merged_data['doi'].apply(lambda doi: '; '.join(get_authors_from_crossref(doi)) if doi else '')

        if not merged_data.empty:
            csv = merged_data.to_csv(index=False)
            csv_bytes = io.BytesIO()
            csv_bytes.write(csv.encode('utf-8'))
            csv_bytes.seek(0)

            st.download_button(
                label="📥 Télécharger le CSV",
                data=csv_bytes,
                file_name=f"{collection_a_chercher}_c2LabHAL.csv",
                mime="text/csv"
            )

            progress_bar.progress(100)
            progress_text.text("Terminé ✅")
        else:
            st.error("Aucune donnée à exporter. Veuillez vérifier les paramètres de recherche.")

if __name__ == "__main__":
    main()
