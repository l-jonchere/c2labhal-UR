# -*- coding: utf-8 -*-
"""
Created on Thu May 15 19:01:37 2025

@author: godet-g
"""

import streamlit as st
from streamlit_app_Scopus_OpenAlex_Pubmed import main as app1_main  # Renommer pour éviter les conflits
from streamlit_app_csv import main as app2_main
from streamlit_app_nantes import main as app3_main

def main():

    # Créer les onglets
    tab1, tab2, tab3 = st.tabs([
        "Comparer Scopus/OpenAlex/Pubmed avec HAL",
        "Comparer CSV avec HAL",
        "Comparer Labo Nantes Université avec HAL"
    ])

    # Contenu de chaque onglet
    with tab1:
        app1_main()  # Exécutez le contenu de streamlit_app_Scopus_OpenAlex_Pubmed.py
    with tab2:
        app2_main()  # Exécutez le contenu de streamlit_app_csv.py
    with tab3:
        app3_main()  # Exécutez le contenu de streamlit_app_nantes.py

if __name__ == "__main__":
    main()