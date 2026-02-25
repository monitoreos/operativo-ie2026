import streamlit as st

def login():
    st.title("🔐 Dashboard Operativo CGR 2026")

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:

        username = st.text_input("Usuario")
        password = st.text_input("Contraseña", type="password")

        if st.button("Ingresar"):
            if username in st.secrets["passwords"] and \
               st.secrets["passwords"][username] == password:

                st.session_state["authenticated"] = True
                st.session_state["user"] = username
                st.success("Acceso autorizado")
                st.rerun()
            else:
                st.error("Credenciales incorrectas")

        st.stop()

    return True