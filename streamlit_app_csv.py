import streamlit as st
from streamlit_app_Scopus_OpenAlex_Pubmed import ScopusOpenAlexPubmedApp
from streamlit_app_csv import CSVApp
from streamlit_app_nantes import NantesApp

def main():
    st.title("🥎 c2LabHAL - Application Fusionnée")

    tab1, tab2, tab3 = st.tabs([
        "Comparer Scopus/OpenAlex/Pubmed avec HAL",
        "Comparer CSV avec HAL",
        "Comparer Labo Nantes Université avec HAL"
    ])

    app1 = ScopusOpenAlexPubmedApp()
    app2 = CSVApp()
    app3 = NantesApp()

    with tab1:
        app1.run()
    with tab2:
        app2.run()
    with tab3:
        app3.run()

if __name__ == "__main__":
    main()