import streamlit as st
from streamlit_app_Scopus_OpenAlex_Pubmed import ScopusOpenAlexPubmedApp  # Nom de la classe !
from streamlit_app_csv import CSVApp  # Nom de la classe !
from streamlit_app_nantes import NantesApp  # Nom de la classe !

def main():
    st.title("🥎 c2LabHAL - Application Fusionnée")

    # Créer les onglets
    tab1, tab2, tab3 = st.tabs([
        "Comparer Scopus/OpenAlex/Pubmed avec HAL",
        "Comparer CSV avec HAL",
        "Comparer Labo Nantes Université avec HAL"
    ])

    # Contenu de chaque onglet
    with tab1:
        app1 = ScopusOpenAlexPubmedApp()  # Instancier la classe
        app1.run()  # Appeler la méthode run
    with tab2:
        app2 = CSVApp()
        app2.run()
    with tab3:
        app3 = NantesApp()
        app3.run()

if __name__ == "__main__":
    main()