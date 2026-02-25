<<<<<<< HEAD
import io
import re
from datetime import datetime

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ====== PDF (MVP) ======
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import cm


# -------------------------
# ⚙️ CONFIG STREAMLIT
# -------------------------
st.set_page_config(
    page_title="Dashboard Operativo CGR 2026",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------------
# 🔐 LOGIN
# -------------------------
def login():
    st.sidebar.title("🔐 Acceso")
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        username = st.sidebar.text_input("Usuario")
        password = st.sidebar.text_input("Contraseña", type="password")

        if st.sidebar.button("Ingresar"):
            if "passwords" in st.secrets and username in st.secrets["passwords"] and st.secrets["passwords"][username] == password:
                st.session_state["authenticated"] = True
                st.session_state["user"] = username
                st.sidebar.success("Acceso autorizado")
                st.rerun()
            else:
                st.sidebar.error("Credenciales incorrectas")
        st.stop()

login()


# -------------------------
# 🔗 CONEXIÓN GOOGLE SHEETS
# -------------------------
@st.cache_resource
def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = dict(st.secrets["google_service_account"])

    # Normaliza saltos de línea por si vienen escapados como \\n
    if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(credentials)
    return client





@st.cache_data(ttl=300)
def load_all_sheets(spreadsheet_name: str) -> pd.DataFrame:
    client = get_gspread_client()
    spreadsheet = client.open(spreadsheet_name)

    worksheets = spreadsheet.worksheets()

    df_base = None
    df_actas = []

    for ws in worksheets:
        sheet_name = ws.title.strip().upper()

        # 🔹 LEER DATOS SIN get_all_records()
        values = ws.get_all_values()

        if not values or len(values) < 2:
            continue

        headers = values[0]

        # 🔹 HACER HEADERS ÚNICOS
        seen = {}
        unique_headers = []
        for h in headers:
            h_clean = h.strip().lower()
            if h_clean in seen:
                seen[h_clean] += 1
                h_clean = f"{h_clean}_{seen[h_clean]}"
            else:
                seen[h_clean] = 0
            unique_headers.append(h_clean)

        data = values[1:]
        temp_df = pd.DataFrame(data, columns=unique_headers)

        if temp_df.empty:
            continue

        # 🔹 BASE CONSOLIDADA
        if sheet_name == "BASE_CONSOLIDADA":
            df_base = temp_df

        # 🔹 ACTAS
        elif sheet_name.startswith("ACTA"):
            temp_df["acta"] = sheet_name
            df_actas.append(temp_df)

    if df_base is None:
        st.error("No se encontró la pestaña BASE_CONSOLIDADA.")
        return pd.DataFrame()

    if not df_actas:
        st.error("No se encontraron pestañas de Actas.")
        return pd.DataFrame()

    df_actas_full = pd.concat(df_actas, ignore_index=True)

    # 🔗 DETECTAR COLUMNA CLAVE
    possible_keys = ["codigo_modular", "cod_mod", "cod_modular"]
    key_col = None

    for k in possible_keys:
        if k in df_base.columns and k in df_actas_full.columns:
            key_col = k
            break

    if key_col is None:
        st.error("No se encontró columna común de código modular para hacer el merge.")
        return pd.DataFrame()



    return df_base, df_actas_full






# -------------------------
# 🧼 UTILIDADES (NORMALIZACIÓN)
# -------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def best_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def coerce_acta(df: pd.DataFrame, col_acta: str) -> pd.DataFrame:
    """
    Asegura formato 'ACTA 01'...'ACTA 06' si viene raro.
    """
    df = df.copy()
    def fmt(x):
        s = str(x).strip().upper()
        m = re.search(r'(\d+)', s)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 6:
                return f"ACTA {n:02d}"
        return s
    df[col_acta] = df[col_acta].apply(fmt)
    return df


def detect_question_columns(df: pd.DataFrame, known_meta: set[str]) -> list[str]:
    qcols = []
    for c in df.columns:
        if c in known_meta:
            continue
        if c in {"llave_unica", "id", "timestamp"}:
            continue
        # 🔥 CLAVE: eliminar columnas completamente vacías
        if df[c].dropna().empty:
            continue
        qcols.append(c)
    return qcols


def count_yes_no(series: pd.Series):
    """
    Cuenta SI/NO de manera robusta (acepta variantes).
    """
    s = series.astype(str).str.strip().str.upper()

    yes = s.isin(["SI", "SÍ", "1", "TRUE", "VERDADERO", "YES"]).sum()
    no = s.isin(["NO", "0", "FALSE", "FALSO"]).sum()

    # Otros (incluye vacíos)
    other = len(s) - yes - no
    return int(yes), int(no), int(other)





# -------------------------
# 📊 GENERADOR DE CUADROS RESUMEN (Tipo Informe Ayacucho)
# -------------------------
def generar_cuadro_resumen(df_filtrado, question_cols):
    total_iiee = df_filtrado[COL_CODMOD].nunique()
    resultados = []

    for col in question_cols:
        if col not in df_filtrado.columns:
            continue

        yes, no, other = count_yes_no(df_filtrado[col])
        total = yes + no + other

        if total == 0:
            continue

        resultados.append({
            "Pregunta": col,
            "IEE SI": yes,
            "% SI": round((yes/total)*100,1),
            "IEE NO": no,
            "% NO": round((no/total)*100,1),
        })

    return pd.DataFrame(resultados)









# -------------------------
# 📥 CARGA DE DATA BASE
# -------------------------

SPREADSHEET_NAME = "BASE_CONSOLIDADA_OPERATIVO_2026"

with st.spinner("Cargando todas las actas desde Google Sheets..."):

    df_base_raw, df_actas_raw = load_all_sheets(SPREADSHEET_NAME)




df_base = normalize_columns(df_base_raw)
df_actas = normalize_columns(df_actas_raw)

# Columnas base

COL_ACTA = best_col(df_actas, ["acta"])
COL_UGEL = best_col(df_base, ["ugel", "ugel_1", ...])
COL_CODMOD = best_col(df_base, ["codigo_modular", ...])





# Columnas BASE (metadatos vienen de BASE_CONSOLIDADA)
COL_UGEL = best_col(df_base, ["ugel", "ugel_1", "dre_ugel", "d_dreugel", "ugel_x", "ugel_y"])
COL_CODMOD = best_col(df_base, ["codigo_modular", "cod_mod", "cod_modular"])
COL_FECHA = best_col(df_base, ["fecha_visita", "fecha", "fecha_de_visita"])
COL_DEP = best_col(df_base, ["departamento_final", "departamento", "dpto", "d_dpto"])
COL_PROV = best_col(df_base, ["provincia_final"])
COL_DIST = best_col(df_base, ["distrito_final"])
COL_IE = best_col(df_base, ["nombre_ie_final"])

# Acta viene de las hojas ACTA 01–06
COL_ACTA = best_col(df_actas, ["acta"])

# ==========================
# 🔎 MODO DEBUG (opcional)
# ==========================
DEBUG = False

if DEBUG:
    st.write("Columnas detectadas en el dataframe:")
    st.write("BASE:", df_base.columns.tolist())
    st.write("ACTAS:", df_actas.columns.tolist())
    st.write("Columna Acta detectada:", COL_ACTA)
    st.write("Columna UGEL detectada:", COL_UGEL)
    st.write("Columna Código Modular detectada:", COL_CODMOD)










missing_required = [name for name, col in {
    "acta": COL_ACTA,
    "ugel": COL_UGEL,
    "codigo_modular": COL_CODMOD,
}.items() if col is None]

if missing_required:
    st.error(
        "Tu hoja no tiene algunas columnas necesarias para los módulos principales. "
        f"Faltan: {', '.join(missing_required)}.\n\n"
        "Solución rápida: dime cómo se llaman EXACTO en tu Google Sheet y lo ajusto en 1 línea."
    )
    st.stop()

df_actas = coerce_acta(df_actas, COL_ACTA)

# Metadatos conocidos (se excluyen del módulo de “preguntas”)


KNOWN_META = {
    COL_ACTA, COL_UGEL, COL_CODMOD, COL_FECHA, COL_DEP, COL_PROV, COL_DIST,

    # Campos administrativos / descriptivos
    "marca_temporal", "timestamp",
    "nombre_ie", "nombre_ie_final",
    "direccion",
    "titular_ie",
    "dni_titular_ie",
    "auditor",
    "dni_auditor",

    "departamento", "provincia", "distrito",
    "d_dpto", "d_prov", "d_dist",
    "cen_edu",
    "t_alumno", "talumno", "t_alumnos", "cantidad_alumnos",
    "llave_unica",
}

KNOWN_META = {c for c in KNOWN_META if c is not None}










def apply_all_filters(
    df_in,
    acta_sel,
    ugel_sel,
    dep_sel,
    prov_sel,
    dist_sel,
    codmod_sel,
    ie_sel
):
    out = df_in.copy()

    if acta_sel != "TODAS":
        out = out[out[COL_ACTA] == acta_sel]

    if ugel_sel != "TODAS":
        out = out[out[COL_UGEL] == ugel_sel]

    if COL_DEP and dep_sel != "TODOS":
        out = out[out[COL_DEP] == dep_sel]

    if COL_PROV and prov_sel != "TODOS":
        out = out[out[COL_PROV] == prov_sel]

    if COL_DIST and dist_sel != "TODOS":
        out = out[out[COL_DIST] == dist_sel]

    if codmod_sel != "TODOS":
        out = out[out[COL_CODMOD] == codmod_sel]

    if COL_IE and ie_sel != "TODOS":
        out = out[out[COL_IE] == ie_sel]

    return out







# -------------------------
# 🧭 SIDEBAR: MÓDULOS
# -------------------------
st.sidebar.markdown("---")
st.sidebar.title("📁 Módulos")

module = st.sidebar.radio(
    "Seleccione un módulo:",
    [
        "Inicio / KPIs Estratégicos",
        "Seguimiento y Control de Actas",
        "Análisis por Pregunta",
        "Generador de Informe PDF (Completo)",
    ],
)

st.sidebar.markdown("---")
st.sidebar.success(f"Usuario: {st.session_state.get('user','')}")



# Actas / UGEL para filtros
acta_list = ["TODAS"] + sorted(df_actas[COL_ACTA].dropna().unique().tolist())
ugel_list = ["TODAS"] + sorted(df_base[COL_UGEL].dropna().unique().tolist())


st.sidebar.markdown("---")



st.sidebar.subheader("Filtros Globales")

# Acta
acta_sel = st.sidebar.selectbox("Acta", acta_list)

# UGEL
ugel_sel = st.sidebar.selectbox("UGEL", ugel_list)




# Departamento
if COL_DEP:
    dep_list = ["TODOS"] + sorted(df_base[COL_DEP].dropna().unique())
    dep_sel = st.sidebar.selectbox("Departamento", dep_list)
else:
    dep_sel = "TODOS"

# Provincia dependiente
if COL_PROV and COL_DEP:
    df_temp = df_base if dep_sel == "TODOS" else df_base[df_base[COL_DEP] == dep_sel]
    prov_list = ["TODOS"] + sorted(df_temp[COL_PROV].dropna().unique())
    prov_sel = st.sidebar.selectbox("Provincia", prov_list)
else:
    prov_sel = "TODOS"

# Distrito dependiente
if COL_DIST and COL_PROV:
    df_temp2 = df_temp if prov_sel == "TODOS" else df_temp[df_temp[COL_PROV] == prov_sel]
    dist_list = ["TODOS"] + sorted(df_temp2[COL_DIST].dropna().unique())
    dist_sel = st.sidebar.selectbox("Distrito", dist_list)
else:
    dist_sel = "TODOS"

# Código Modular
codmod_list = ["TODOS"] + sorted(df_base[COL_CODMOD].dropna().unique())
codmod_sel = st.sidebar.selectbox("Código Modular", codmod_list)

# Institución Educativa
if COL_IE:
    ie_list = ["TODOS"] + sorted(df_base[COL_IE].dropna().unique())
    ie_sel = st.sidebar.selectbox("Institución Educativa", ie_list)
else:
    ie_sel = "TODOS"


df_base_filtrado = apply_all_filters(
    df_base,
    acta_sel,
    ugel_sel,
    dep_sel,
    prov_sel,
    dist_sel,
    codmod_sel,
    ie_sel
)

df_actas_filtrado = apply_all_filters(
    df_actas,
    acta_sel,
    ugel_sel,
    dep_sel,
    prov_sel,
    dist_sel,
    codmod_sel,
    ie_sel
)








# -------------------------
# 🧱 LAYOUT PRINCIPAL
# -------------------------
st.title("📊 Dashboard Operativo CGR 2026")


# =========================================================
# 1) INICIO / KPIs ESTRATÉGICOS (Alta Dirección)
# =========================================================
if module == "Inicio / KPIs Estratégicos":
    st.subheader("📌 KPIs Estratégicos (Alta Dirección)")

    







    df_f = df_actas_filtrado

    # KPIs
    total_registros = len(df_f)
    total_iiee = df_f[COL_CODMOD].nunique(dropna=True)
    total_ugel = df_f[COL_UGEL].nunique(dropna=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Registros", f"{total_registros:,}".replace(",", " "))
    c2.metric("Total IIEE (cód. modular únicos)", f"{total_iiee:,}".replace(",", " "))
    c3.metric("Total UGEL", f"{total_ugel:,}".replace(",", " "))

    # Completitud global (si filtras TODAS)
    # Mide cuántos cod_mod tienen presencia en las 6 actas
    pivot = (
        df_f.groupby([COL_CODMOD, COL_ACTA])
            .size()
            .unstack(fill_value=0)
    )
    # Asegura columnas actas 01-06
    for a in [f"ACTA {i:02d}" for i in range(1, 7)]:
        if a not in pivot.columns:
            pivot[a] = 0
    pivot = pivot[[f"ACTA {i:02d}" for i in range(1, 7)]]

    pivot_bin = (pivot > 0).astype(int)
    pivot_bin["avance_actas"] = pivot_bin.sum(axis=1)
    completos = (pivot_bin["avance_actas"] == 6).sum()
    incompletos = (pivot_bin["avance_actas"] < 6).sum()

    pct_completo = (completos / (completos + incompletos) * 100) if (completos + incompletos) else 0
    c4.metric("IIEE con 6/6 Actas", f"{pct_completo:.1f}%")

    st.markdown("### 📍 Resumen por UGEL (Top)")
    resumen_ugel = (
        df_f.groupby(COL_UGEL)[COL_CODMOD]
            .nunique()
            .sort_values(ascending=False)
            .reset_index(name="iiee_unicas")
    )
    st.dataframe(resumen_ugel, use_container_width=True, height=420)

    st.markdown("### 🧾 Vista de datos filtrados")
    st.dataframe(df_f, use_container_width=True, height=520)


# =========================================================
# 2) SEGUIMIENTO Y CONTROL DE ACTAS
# =========================================================
elif module == "Seguimiento y Control de Actas":
    st.subheader("🧩 Seguimiento y Control del Llenado de Actas (por Código Modular)")

    



    df_f = df_actas_filtrado

    # Matriz de completitud por cod_mod
    pivot = (
        df_f.groupby([COL_CODMOD, COL_ACTA])
            .size()
            .unstack(fill_value=0)
    )
    for a in [f"ACTA {i:02d}" for i in range(1, 7)]:
        if a not in pivot.columns:
            pivot[a] = 0
    pivot = pivot[[f"ACTA {i:02d}" for i in range(1, 7)]]

    binm = (pivot > 0).astype(int)
    binm["avance_actas"] = binm.sum(axis=1)
    binm["estado"] = binm["avance_actas"].apply(lambda x: "COMPLETO" if x == 6 else "INCOMPLETO")

    # KPI del módulo
    total_iiee = len(binm)
    completos = (binm["estado"] == "COMPLETO").sum()
    incompletos = (binm["estado"] == "INCOMPLETO").sum()

    k1, k2, k3 = st.columns(3)
    k1.metric("Total IIEE evaluadas", f"{total_iiee:,}".replace(",", " "))
    k2.metric("Completos (6/6)", f"{completos:,}".replace(",", " "))
    k3.metric("Incompletos", f"{incompletos:,}".replace(",", " "))

    st.sidebar.markdown("---")
    st.sidebar.subheader("Control")
    show_only_incomplete = st.sidebar.checkbox("Mostrar solo INCOMPLETOS", value=True)
    min_actas = st.sidebar.slider("Mínimo de actas registradas", 0, 6, 0)

    out = binm.copy()
    if show_only_incomplete:
        out = out[out["estado"] == "INCOMPLETO"]
    out = out[out["avance_actas"] >= min_actas]

    # Identificar qué actas faltan (texto)
    def faltantes(row):
        missing = [a for a in [f"ACTA {i:02d}" for i in range(1, 7)] if row[a] == 0]
        return ", ".join(missing) if missing else ""
    out["actas_faltantes"] = out.apply(faltantes, axis=1)

    st.markdown("### 🚨 Matriz de Control (1 = registrado, 0 = faltante)")
    st.dataframe(out.reset_index().rename(columns={COL_CODMOD: "codigo_modular"}), use_container_width=True, height=600)


# =========================================================
# 3) ANÁLISIS POR PREGUNTA (SI/NO)
# =========================================================
elif module == "Análisis por Pregunta":
    st.subheader("📋 Análisis Estadístico por Pregunta (SI/NO)")



    df_f = df_actas_filtrado

    question_cols_filtradas = detect_question_columns(df_f, KNOWN_META)

    if not question_cols_filtradas:
        st.warning(
            "No detecté columnas de preguntas (además de metadatos). "
            "Revisa si tu hoja tiene columnas de respuestas tipo SI/NO."
        )
        st.dataframe(df_f, use_container_width=True)
        st.stop()

    pregunta_col = st.selectbox(
        "Seleccione la columna de pregunta / respuesta",
        question_cols_filtradas,
        key="analisis_pregunta_select"
    )
    









    yes, no, other = count_yes_no(df_f[pregunta_col])

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Total IIEE (únicas)", f"{df_f[COL_CODMOD].nunique(dropna=True):,}".replace(",", " "))
    a2.metric("SI", yes)
    a3.metric("NO", no)
    a4.metric("Otros / Vacíos", other)

    # Tabla resumen (para el informe tipo “Cuadro n° X”)
    total = yes + no + other
    resumen = pd.DataFrame({
        "Respuesta": ["SI", "NO", "OTROS/VACÍO"],
        "Cantidad IIEE": [yes, no, other],
        "Porcentaje": [
            f"{(yes/total*100):.1f}%" if total else "0.0%",
            f"{(no/total*100):.1f}%" if total else "0.0%",
            f"{(other/total*100):.1f}%" if total else "0.0%",
        ]
    })

    st.markdown("### 🧾 Cuadro Resumen (para el Informe)")
    st.dataframe(resumen, use_container_width=True)

    st.markdown("### 📌 Registros (muestra)")
    show_cols = [COL_ACTA, COL_UGEL, COL_CODMOD]
    if COL_FECHA:
        show_cols.append(COL_FECHA)
    show_cols.append(pregunta_col)

    st.dataframe(df_f[show_cols].head(500), use_container_width=True, height=520)


# =========================================================
# 4) GENERADOR DE INFORME PDF (MVP)
# =========================================================
elif module == "Generador de Informe PDF (Completo)":

    st.subheader("📑 Generador de Informe de Visita de Control – Consolidado")

    


    df_f = df_actas_filtrado

    if df_f.empty:
        st.warning("No hay datos con los filtros seleccionados.")
        st.stop()

    st.markdown("### 📊 Cuadros Resumen por Pregunta")

    
    question_cols_filtradas = detect_question_columns(df_f, KNOWN_META)

    if not question_cols_filtradas:
        st.warning("No hay columnas de preguntas detectadas.")
        st.stop()

    resumen_df = generar_cuadro_resumen(df_f, question_cols_filtradas)




    st.dataframe(resumen_df, use_container_width=True, height=600)



    



    st.markdown("### 📌 Seleccione pregunta para incluir en el PDF")

    pregunta_col = st.selectbox(
        "Pregunta (columna) para incluir en el PDF",
        question_cols_filtradas,
        key="pdf_pregunta_select"
    )

    # -------- PDF COMPLETO --------
    def build_pdf():
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4)
        styles = getSampleStyleSheet()
        story = []

        story.append(Paragraph("INFORME DE VISITA DE CONTROL", styles["Title"]))
        story.append(Spacer(1,12))

        story.append(Paragraph(
            f"Acta: {acta_sel} | UGEL: {ugel_sel} | Departamento: {dep_sel}",
            styles["Normal"]
        ))
        story.append(Spacer(1,12))

        # KPIs generales
        total_registros = len(df_f)
        total_iiee = df_f[COL_CODMOD].nunique()

        tabla_kpi = Table([
            ["Indicador","Valor"],
            ["Total Registros", total_registros],
            ["Total IIEE", total_iiee]
        ])

        tabla_kpi.setStyle(TableStyle([
            ("GRID",(0,0),(-1,-1),0.5,colors.black),
            ("BACKGROUND",(0,0),(-1,0),colors.lightgrey)
        ]))

        story.append(tabla_kpi)
        story.append(Spacer(1,20))

        # CUADROS POR PREGUNTA
        for _, row in resumen_df.iterrows():
            story.append(Paragraph(f"Pregunta: {row['Pregunta']}", styles["Heading3"]))
            story.append(Spacer(1,6))

            tabla = Table([
                ["Respuesta","Cantidad IEE","%"],
                ["SI", row["IEE SI"], f"{row['% SI']}%"],
                ["NO", row["IEE NO"], f"{row['% NO']}%"],
            ])

            tabla.setStyle(TableStyle([
                ("GRID",(0,0),(-1,-1),0.5,colors.black),
                ("BACKGROUND",(0,0),(-1,0),colors.lightgrey)
            ]))

            story.append(tabla)
            story.append(Spacer(1,15))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    if st.button("📄 Generar Informe Completo"):
        pdf_bytes = build_pdf()
        st.download_button(
            "⬇️ Descargar Informe PDF",
            pdf_bytes,
            "informe_visita_control_completo.pdf",
            "application/pdf"
        )


    

   






    

    # KPIs
    total_registros = len(df_f)
    total_iiee = df_f[COL_CODMOD].nunique(dropna=True)
    total_ugel = df_f[COL_UGEL].nunique(dropna=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Registros", f"{total_registros:,}".replace(",", " "))
    c2.metric("Total IIEE", f"{total_iiee:,}".replace(",", " "))
    c3.metric("Total UGEL", f"{total_ugel:,}".replace(",", " "))

    st.markdown("### Vista previa (datos filtrados)")
    st.dataframe(df_f.head(300), use_container_width=True, height=420)

    def build_pdf_bytes():
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=1.8*cm,
            leftMargin=1.8*cm,
            topMargin=1.6*cm,
            bottomMargin=1.6*cm
        )
        styles = getSampleStyleSheet()
        story = []

        title = "INFORME DE VISITA DE CONTROL"
        story.append(Paragraph(title, styles["Title"]))
        story.append(Spacer(1, 12))

        # Encabezado
        subt = f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')} | Filtro Acta: {acta_sel} | Filtro UGEL: {ugel_sel}"
        story.append(Paragraph(subt, styles["Normal"]))
        story.append(Spacer(1, 12))

        # Tabla KPIs
        kpi_data = [
            ["Indicador", "Valor"],
            ["Total Registros", str(total_registros)],
            ["Total IIEE (cód. modular únicos)", str(total_iiee)],
            ["Total UGEL", str(total_ugel)],
        ]
        t = Table(kpi_data, colWidths=[10*cm, 6*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (1, 1), (1, -1), "CENTER"),
        ]))
        story.append(t)
        story.append(Spacer(1, 16))

        # Cuadro SI/NO por pregunta
        if pregunta_col:
            yes, no, other = count_yes_no(df_f[pregunta_col])
            total = yes + no + other

            story.append(Paragraph(f"CUADRO: Resumen de Respuestas – {pregunta_col}", styles["Heading2"]))
            story.append(Spacer(1, 8))

            cuadro = [
                ["Respuesta", "Cantidad IIEE", "Porcentaje"],
                ["SI", str(yes), f"{(yes/total*100):.1f}%" if total else "0.0%"],
                ["NO", str(no), f"{(no/total*100):.1f}%" if total else "0.0%"],
                ["OTROS/VACÍO", str(other), f"{(other/total*100):.1f}%" if total else "0.0%"],
            ]
            tt = Table(cuadro, colWidths=[6*cm, 5*cm, 5*cm])
            tt.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (1, 1), (-1, -1), "CENTER"),
            ]))
            story.append(tt)
            story.append(Spacer(1, 10))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    if st.button("📄 Generar PDF (MVP)"):
        pdf_bytes = build_pdf_bytes()
        st.success("PDF generado.")
        st.download_button(
            label="⬇️ Descargar Informe PDF",
            data=pdf_bytes,
            file_name="informe_visita_control_mvp.pdf",
            mime="application/pdf"
=======
import io
import re
from datetime import datetime

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ====== PDF (MVP) ======
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import cm


# -------------------------
# ⚙️ CONFIG STREAMLIT
# -------------------------
st.set_page_config(
    page_title="Dashboard Operativo CGR 2026",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------------
# 🔐 LOGIN
# -------------------------
def login():
    st.sidebar.title("🔐 Acceso")
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        username = st.sidebar.text_input("Usuario")
        password = st.sidebar.text_input("Contraseña", type="password")

        if st.sidebar.button("Ingresar"):
            if "passwords" in st.secrets and username in st.secrets["passwords"] and st.secrets["passwords"][username] == password:
                st.session_state["authenticated"] = True
                st.session_state["user"] = username
                st.sidebar.success("Acceso autorizado")
                st.rerun()
            else:
                st.sidebar.error("Credenciales incorrectas")
        st.stop()

login()


# -------------------------
# 🔗 CONEXIÓN GOOGLE SHEETS
# -------------------------
@st.cache_resource
def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = dict(st.secrets["google_service_account"])

    # Normaliza saltos de línea por si vienen escapados como \\n
    if "private_key" in creds_dict and isinstance(creds_dict["private_key"], str):
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(credentials)
    return client





@st.cache_data(ttl=300)
def load_all_sheets(spreadsheet_name: str) -> pd.DataFrame:
    client = get_gspread_client()
    spreadsheet = client.open(spreadsheet_name)

    worksheets = spreadsheet.worksheets()

    df_base = None
    df_actas = []

    for ws in worksheets:
        sheet_name = ws.title.strip().upper()

        # 🔹 LEER DATOS SIN get_all_records()
        values = ws.get_all_values()

        if not values or len(values) < 2:
            continue

        headers = values[0]

        # 🔹 HACER HEADERS ÚNICOS
        seen = {}
        unique_headers = []
        for h in headers:
            h_clean = h.strip().lower()
            if h_clean in seen:
                seen[h_clean] += 1
                h_clean = f"{h_clean}_{seen[h_clean]}"
            else:
                seen[h_clean] = 0
            unique_headers.append(h_clean)

        data = values[1:]
        temp_df = pd.DataFrame(data, columns=unique_headers)

        if temp_df.empty:
            continue

        # 🔹 BASE CONSOLIDADA
        if sheet_name == "BASE_CONSOLIDADA":
            df_base = temp_df

        # 🔹 ACTAS
        elif sheet_name.startswith("ACTA"):
            temp_df["acta"] = sheet_name
            df_actas.append(temp_df)

    if df_base is None:
        st.error("No se encontró la pestaña BASE_CONSOLIDADA.")
        return pd.DataFrame()

    if not df_actas:
        st.error("No se encontraron pestañas de Actas.")
        return pd.DataFrame()

    df_actas_full = pd.concat(df_actas, ignore_index=True)

    # 🔗 DETECTAR COLUMNA CLAVE
    possible_keys = ["codigo_modular", "cod_mod", "cod_modular"]
    key_col = None

    for k in possible_keys:
        if k in df_base.columns and k in df_actas_full.columns:
            key_col = k
            break

    if key_col is None:
        st.error("No se encontró columna común de código modular para hacer el merge.")
        return pd.DataFrame()



    return df_base, df_actas_full






# -------------------------
# 🧼 UTILIDADES (NORMALIZACIÓN)
# -------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def best_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def coerce_acta(df: pd.DataFrame, col_acta: str) -> pd.DataFrame:
    """
    Asegura formato 'ACTA 01'...'ACTA 06' si viene raro.
    """
    df = df.copy()
    def fmt(x):
        s = str(x).strip().upper()
        m = re.search(r'(\d+)', s)
        if m:
            n = int(m.group(1))
            if 1 <= n <= 6:
                return f"ACTA {n:02d}"
        return s
    df[col_acta] = df[col_acta].apply(fmt)
    return df


def detect_question_columns(df: pd.DataFrame, known_meta: set[str]) -> list[str]:
    qcols = []
    for c in df.columns:
        if c in known_meta:
            continue
        if c in {"llave_unica", "id", "timestamp"}:
            continue
        # 🔥 CLAVE: eliminar columnas completamente vacías
        if df[c].dropna().empty:
            continue
        qcols.append(c)
    return qcols


def count_yes_no(series: pd.Series):
    """
    Cuenta SI/NO de manera robusta (acepta variantes).
    """
    s = series.astype(str).str.strip().str.upper()

    yes = s.isin(["SI", "SÍ", "1", "TRUE", "VERDADERO", "YES"]).sum()
    no = s.isin(["NO", "0", "FALSE", "FALSO"]).sum()

    # Otros (incluye vacíos)
    other = len(s) - yes - no
    return int(yes), int(no), int(other)





# -------------------------
# 📊 GENERADOR DE CUADROS RESUMEN (Tipo Informe Ayacucho)
# -------------------------
def generar_cuadro_resumen(df_filtrado, question_cols):
    total_iiee = df_filtrado[COL_CODMOD].nunique()
    resultados = []

    for col in question_cols:
        if col not in df_filtrado.columns:
            continue

        yes, no, other = count_yes_no(df_filtrado[col])
        total = yes + no + other

        if total == 0:
            continue

        resultados.append({
            "Pregunta": col,
            "IEE SI": yes,
            "% SI": round((yes/total)*100,1),
            "IEE NO": no,
            "% NO": round((no/total)*100,1),
        })

    return pd.DataFrame(resultados)









# -------------------------
# 📥 CARGA DE DATA BASE
# -------------------------

SPREADSHEET_NAME = "BASE_CONSOLIDADA_OPERATIVO_2026"

with st.spinner("Cargando todas las actas desde Google Sheets..."):

    df_base_raw, df_actas_raw = load_all_sheets(SPREADSHEET_NAME)




df_base = normalize_columns(df_base_raw)
df_actas = normalize_columns(df_actas_raw)

# Columnas base

COL_ACTA = best_col(df_actas, ["acta"])
COL_UGEL = best_col(df_base, ["ugel", "ugel_1", ...])
COL_CODMOD = best_col(df_base, ["codigo_modular", ...])





# Columnas BASE (metadatos vienen de BASE_CONSOLIDADA)
COL_UGEL = best_col(df_base, ["ugel", "ugel_1", "dre_ugel", "d_dreugel", "ugel_x", "ugel_y"])
COL_CODMOD = best_col(df_base, ["codigo_modular", "cod_mod", "cod_modular"])
COL_FECHA = best_col(df_base, ["fecha_visita", "fecha", "fecha_de_visita"])
COL_DEP = best_col(df_base, ["departamento_final", "departamento", "dpto", "d_dpto"])
COL_PROV = best_col(df_base, ["provincia_final"])
COL_DIST = best_col(df_base, ["distrito_final"])
COL_IE = best_col(df_base, ["nombre_ie_final"])

# Acta viene de las hojas ACTA 01–06
COL_ACTA = best_col(df_actas, ["acta"])

# ==========================
# 🔎 MODO DEBUG (opcional)
# ==========================
DEBUG = False

if DEBUG:
    st.write("Columnas detectadas en el dataframe:")
    st.write("BASE:", df_base.columns.tolist())
    st.write("ACTAS:", df_actas.columns.tolist())
    st.write("Columna Acta detectada:", COL_ACTA)
    st.write("Columna UGEL detectada:", COL_UGEL)
    st.write("Columna Código Modular detectada:", COL_CODMOD)










missing_required = [name for name, col in {
    "acta": COL_ACTA,
    "ugel": COL_UGEL,
    "codigo_modular": COL_CODMOD,
}.items() if col is None]

if missing_required:
    st.error(
        "Tu hoja no tiene algunas columnas necesarias para los módulos principales. "
        f"Faltan: {', '.join(missing_required)}.\n\n"
        "Solución rápida: dime cómo se llaman EXACTO en tu Google Sheet y lo ajusto en 1 línea."
    )
    st.stop()

df_actas = coerce_acta(df_actas, COL_ACTA)

# Metadatos conocidos (se excluyen del módulo de “preguntas”)


KNOWN_META = {
    COL_ACTA, COL_UGEL, COL_CODMOD, COL_FECHA, COL_DEP, COL_PROV, COL_DIST,

    # Campos administrativos / descriptivos
    "marca_temporal", "timestamp",
    "nombre_ie", "nombre_ie_final",
    "direccion",
    "titular_ie",
    "dni_titular_ie",
    "auditor",
    "dni_auditor",

    "departamento", "provincia", "distrito",
    "d_dpto", "d_prov", "d_dist",
    "cen_edu",
    "t_alumno", "talumno", "t_alumnos", "cantidad_alumnos",
    "llave_unica",
}

KNOWN_META = {c for c in KNOWN_META if c is not None}










def apply_all_filters(
    df_in,
    acta_sel,
    ugel_sel,
    dep_sel,
    prov_sel,
    dist_sel,
    codmod_sel,
    ie_sel
):
    out = df_in.copy()

    if acta_sel != "TODAS":
        out = out[out[COL_ACTA] == acta_sel]

    if ugel_sel != "TODAS":
        out = out[out[COL_UGEL] == ugel_sel]

    if COL_DEP and dep_sel != "TODOS":
        out = out[out[COL_DEP] == dep_sel]

    if COL_PROV and prov_sel != "TODOS":
        out = out[out[COL_PROV] == prov_sel]

    if COL_DIST and dist_sel != "TODOS":
        out = out[out[COL_DIST] == dist_sel]

    if codmod_sel != "TODOS":
        out = out[out[COL_CODMOD] == codmod_sel]

    if COL_IE and ie_sel != "TODOS":
        out = out[out[COL_IE] == ie_sel]

    return out







# -------------------------
# 🧭 SIDEBAR: MÓDULOS
# -------------------------
st.sidebar.markdown("---")
st.sidebar.title("📁 Módulos")

module = st.sidebar.radio(
    "Seleccione un módulo:",
    [
        "Inicio / KPIs Estratégicos",
        "Seguimiento y Control de Actas",
        "Análisis por Pregunta",
        "Generador de Informe PDF (Completo)",
    ],
)

st.sidebar.markdown("---")
st.sidebar.success(f"Usuario: {st.session_state.get('user','')}")



# Actas / UGEL para filtros
acta_list = ["TODAS"] + sorted(df_actas[COL_ACTA].dropna().unique().tolist())
ugel_list = ["TODAS"] + sorted(df_base[COL_UGEL].dropna().unique().tolist())


st.sidebar.markdown("---")



st.sidebar.subheader("Filtros Globales")

# Acta
acta_sel = st.sidebar.selectbox("Acta", acta_list)

# UGEL
ugel_sel = st.sidebar.selectbox("UGEL", ugel_list)




# Departamento
if COL_DEP:
    dep_list = ["TODOS"] + sorted(df_base[COL_DEP].dropna().unique())
    dep_sel = st.sidebar.selectbox("Departamento", dep_list)
else:
    dep_sel = "TODOS"

# Provincia dependiente
if COL_PROV and COL_DEP:
    df_temp = df_base if dep_sel == "TODOS" else df_base[df_base[COL_DEP] == dep_sel]
    prov_list = ["TODOS"] + sorted(df_temp[COL_PROV].dropna().unique())
    prov_sel = st.sidebar.selectbox("Provincia", prov_list)
else:
    prov_sel = "TODOS"

# Distrito dependiente
if COL_DIST and COL_PROV:
    df_temp2 = df_temp if prov_sel == "TODOS" else df_temp[df_temp[COL_PROV] == prov_sel]
    dist_list = ["TODOS"] + sorted(df_temp2[COL_DIST].dropna().unique())
    dist_sel = st.sidebar.selectbox("Distrito", dist_list)
else:
    dist_sel = "TODOS"

# Código Modular
codmod_list = ["TODOS"] + sorted(df_base[COL_CODMOD].dropna().unique())
codmod_sel = st.sidebar.selectbox("Código Modular", codmod_list)

# Institución Educativa
if COL_IE:
    ie_list = ["TODOS"] + sorted(df_base[COL_IE].dropna().unique())
    ie_sel = st.sidebar.selectbox("Institución Educativa", ie_list)
else:
    ie_sel = "TODOS"


df_base_filtrado = apply_all_filters(
    df_base,
    acta_sel,
    ugel_sel,
    dep_sel,
    prov_sel,
    dist_sel,
    codmod_sel,
    ie_sel
)

df_actas_filtrado = apply_all_filters(
    df_actas,
    acta_sel,
    ugel_sel,
    dep_sel,
    prov_sel,
    dist_sel,
    codmod_sel,
    ie_sel
)








# -------------------------
# 🧱 LAYOUT PRINCIPAL
# -------------------------
st.title("📊 Dashboard Operativo CGR 2026")


# =========================================================
# 1) INICIO / KPIs ESTRATÉGICOS (Alta Dirección)
# =========================================================
if module == "Inicio / KPIs Estratégicos":
    st.subheader("📌 KPIs Estratégicos (Alta Dirección)")

    







    df_f = df_actas_filtrado

    # KPIs
    total_registros = len(df_f)
    total_iiee = df_f[COL_CODMOD].nunique(dropna=True)
    total_ugel = df_f[COL_UGEL].nunique(dropna=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Registros", f"{total_registros:,}".replace(",", " "))
    c2.metric("Total IIEE (cód. modular únicos)", f"{total_iiee:,}".replace(",", " "))
    c3.metric("Total UGEL", f"{total_ugel:,}".replace(",", " "))

    # Completitud global (si filtras TODAS)
    # Mide cuántos cod_mod tienen presencia en las 6 actas
    pivot = (
        df_f.groupby([COL_CODMOD, COL_ACTA])
            .size()
            .unstack(fill_value=0)
    )
    # Asegura columnas actas 01-06
    for a in [f"ACTA {i:02d}" for i in range(1, 7)]:
        if a not in pivot.columns:
            pivot[a] = 0
    pivot = pivot[[f"ACTA {i:02d}" for i in range(1, 7)]]

    pivot_bin = (pivot > 0).astype(int)
    pivot_bin["avance_actas"] = pivot_bin.sum(axis=1)
    completos = (pivot_bin["avance_actas"] == 6).sum()
    incompletos = (pivot_bin["avance_actas"] < 6).sum()

    pct_completo = (completos / (completos + incompletos) * 100) if (completos + incompletos) else 0
    c4.metric("IIEE con 6/6 Actas", f"{pct_completo:.1f}%")

    st.markdown("### 📍 Resumen por UGEL (Top)")
    resumen_ugel = (
        df_f.groupby(COL_UGEL)[COL_CODMOD]
            .nunique()
            .sort_values(ascending=False)
            .reset_index(name="iiee_unicas")
    )
    st.dataframe(resumen_ugel, use_container_width=True, height=420)

    st.markdown("### 🧾 Vista de datos filtrados")
    st.dataframe(df_f, use_container_width=True, height=520)


# =========================================================
# 2) SEGUIMIENTO Y CONTROL DE ACTAS
# =========================================================
elif module == "Seguimiento y Control de Actas":
    st.subheader("🧩 Seguimiento y Control del Llenado de Actas (por Código Modular)")

    



    df_f = df_actas_filtrado

    # Matriz de completitud por cod_mod
    pivot = (
        df_f.groupby([COL_CODMOD, COL_ACTA])
            .size()
            .unstack(fill_value=0)
    )
    for a in [f"ACTA {i:02d}" for i in range(1, 7)]:
        if a not in pivot.columns:
            pivot[a] = 0
    pivot = pivot[[f"ACTA {i:02d}" for i in range(1, 7)]]

    binm = (pivot > 0).astype(int)
    binm["avance_actas"] = binm.sum(axis=1)
    binm["estado"] = binm["avance_actas"].apply(lambda x: "COMPLETO" if x == 6 else "INCOMPLETO")

    # KPI del módulo
    total_iiee = len(binm)
    completos = (binm["estado"] == "COMPLETO").sum()
    incompletos = (binm["estado"] == "INCOMPLETO").sum()

    k1, k2, k3 = st.columns(3)
    k1.metric("Total IIEE evaluadas", f"{total_iiee:,}".replace(",", " "))
    k2.metric("Completos (6/6)", f"{completos:,}".replace(",", " "))
    k3.metric("Incompletos", f"{incompletos:,}".replace(",", " "))

    st.sidebar.markdown("---")
    st.sidebar.subheader("Control")
    show_only_incomplete = st.sidebar.checkbox("Mostrar solo INCOMPLETOS", value=True)
    min_actas = st.sidebar.slider("Mínimo de actas registradas", 0, 6, 0)

    out = binm.copy()
    if show_only_incomplete:
        out = out[out["estado"] == "INCOMPLETO"]
    out = out[out["avance_actas"] >= min_actas]

    # Identificar qué actas faltan (texto)
    def faltantes(row):
        missing = [a for a in [f"ACTA {i:02d}" for i in range(1, 7)] if row[a] == 0]
        return ", ".join(missing) if missing else ""
    out["actas_faltantes"] = out.apply(faltantes, axis=1)

    st.markdown("### 🚨 Matriz de Control (1 = registrado, 0 = faltante)")
    st.dataframe(out.reset_index().rename(columns={COL_CODMOD: "codigo_modular"}), use_container_width=True, height=600)


# =========================================================
# 3) ANÁLISIS POR PREGUNTA (SI/NO)
# =========================================================
elif module == "Análisis por Pregunta":
    st.subheader("📋 Análisis Estadístico por Pregunta (SI/NO)")



    df_f = df_actas_filtrado

    question_cols_filtradas = detect_question_columns(df_f, KNOWN_META)

    if not question_cols_filtradas:
        st.warning(
            "No detecté columnas de preguntas (además de metadatos). "
            "Revisa si tu hoja tiene columnas de respuestas tipo SI/NO."
        )
        st.dataframe(df_f, use_container_width=True)
        st.stop()

    pregunta_col = st.selectbox(
        "Seleccione la columna de pregunta / respuesta",
        question_cols_filtradas,
        key="analisis_pregunta_select"
    )
    









    yes, no, other = count_yes_no(df_f[pregunta_col])

    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Total IIEE (únicas)", f"{df_f[COL_CODMOD].nunique(dropna=True):,}".replace(",", " "))
    a2.metric("SI", yes)
    a3.metric("NO", no)
    a4.metric("Otros / Vacíos", other)

    # Tabla resumen (para el informe tipo “Cuadro n° X”)
    total = yes + no + other
    resumen = pd.DataFrame({
        "Respuesta": ["SI", "NO", "OTROS/VACÍO"],
        "Cantidad IIEE": [yes, no, other],
        "Porcentaje": [
            f"{(yes/total*100):.1f}%" if total else "0.0%",
            f"{(no/total*100):.1f}%" if total else "0.0%",
            f"{(other/total*100):.1f}%" if total else "0.0%",
        ]
    })

    st.markdown("### 🧾 Cuadro Resumen (para el Informe)")
    st.dataframe(resumen, use_container_width=True)

    st.markdown("### 📌 Registros (muestra)")
    show_cols = [COL_ACTA, COL_UGEL, COL_CODMOD]
    if COL_FECHA:
        show_cols.append(COL_FECHA)
    show_cols.append(pregunta_col)

    st.dataframe(df_f[show_cols].head(500), use_container_width=True, height=520)


# =========================================================
# 4) GENERADOR DE INFORME PDF (MVP)
# =========================================================
elif module == "Generador de Informe PDF (Completo)":

    st.subheader("📑 Generador de Informe de Visita de Control – Consolidado")

    


    df_f = df_actas_filtrado

    if df_f.empty:
        st.warning("No hay datos con los filtros seleccionados.")
        st.stop()

    st.markdown("### 📊 Cuadros Resumen por Pregunta")

    
    question_cols_filtradas = detect_question_columns(df_f, KNOWN_META)

    if not question_cols_filtradas:
        st.warning("No hay columnas de preguntas detectadas.")
        st.stop()

    resumen_df = generar_cuadro_resumen(df_f, question_cols_filtradas)




    st.dataframe(resumen_df, use_container_width=True, height=600)



    



    st.markdown("### 📌 Seleccione pregunta para incluir en el PDF")

    pregunta_col = st.selectbox(
        "Pregunta (columna) para incluir en el PDF",
        question_cols_filtradas,
        key="pdf_pregunta_select"
    )

    # -------- PDF COMPLETO --------
    def build_pdf():
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4)
        styles = getSampleStyleSheet()
        story = []

        story.append(Paragraph("INFORME DE VISITA DE CONTROL", styles["Title"]))
        story.append(Spacer(1,12))

        story.append(Paragraph(
            f"Acta: {acta_sel} | UGEL: {ugel_sel} | Departamento: {dep_sel}",
            styles["Normal"]
        ))
        story.append(Spacer(1,12))

        # KPIs generales
        total_registros = len(df_f)
        total_iiee = df_f[COL_CODMOD].nunique()

        tabla_kpi = Table([
            ["Indicador","Valor"],
            ["Total Registros", total_registros],
            ["Total IIEE", total_iiee]
        ])

        tabla_kpi.setStyle(TableStyle([
            ("GRID",(0,0),(-1,-1),0.5,colors.black),
            ("BACKGROUND",(0,0),(-1,0),colors.lightgrey)
        ]))

        story.append(tabla_kpi)
        story.append(Spacer(1,20))

        # CUADROS POR PREGUNTA
        for _, row in resumen_df.iterrows():
            story.append(Paragraph(f"Pregunta: {row['Pregunta']}", styles["Heading3"]))
            story.append(Spacer(1,6))

            tabla = Table([
                ["Respuesta","Cantidad IEE","%"],
                ["SI", row["IEE SI"], f"{row['% SI']}%"],
                ["NO", row["IEE NO"], f"{row['% NO']}%"],
            ])

            tabla.setStyle(TableStyle([
                ("GRID",(0,0),(-1,-1),0.5,colors.black),
                ("BACKGROUND",(0,0),(-1,0),colors.lightgrey)
            ]))

            story.append(tabla)
            story.append(Spacer(1,15))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    if st.button("📄 Generar Informe Completo"):
        pdf_bytes = build_pdf()
        st.download_button(
            "⬇️ Descargar Informe PDF",
            pdf_bytes,
            "informe_visita_control_completo.pdf",
            "application/pdf"
        )


    

   






    

    # KPIs
    total_registros = len(df_f)
    total_iiee = df_f[COL_CODMOD].nunique(dropna=True)
    total_ugel = df_f[COL_UGEL].nunique(dropna=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Registros", f"{total_registros:,}".replace(",", " "))
    c2.metric("Total IIEE", f"{total_iiee:,}".replace(",", " "))
    c3.metric("Total UGEL", f"{total_ugel:,}".replace(",", " "))

    st.markdown("### Vista previa (datos filtrados)")
    st.dataframe(df_f.head(300), use_container_width=True, height=420)

    def build_pdf_bytes():
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=1.8*cm,
            leftMargin=1.8*cm,
            topMargin=1.6*cm,
            bottomMargin=1.6*cm
        )
        styles = getSampleStyleSheet()
        story = []

        title = "INFORME DE VISITA DE CONTROL"
        story.append(Paragraph(title, styles["Title"]))
        story.append(Spacer(1, 12))

        # Encabezado
        subt = f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')} | Filtro Acta: {acta_sel} | Filtro UGEL: {ugel_sel}"
        story.append(Paragraph(subt, styles["Normal"]))
        story.append(Spacer(1, 12))

        # Tabla KPIs
        kpi_data = [
            ["Indicador", "Valor"],
            ["Total Registros", str(total_registros)],
            ["Total IIEE (cód. modular únicos)", str(total_iiee)],
            ["Total UGEL", str(total_ugel)],
        ]
        t = Table(kpi_data, colWidths=[10*cm, 6*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN", (1, 1), (1, -1), "CENTER"),
        ]))
        story.append(t)
        story.append(Spacer(1, 16))

        # Cuadro SI/NO por pregunta
        if pregunta_col:
            yes, no, other = count_yes_no(df_f[pregunta_col])
            total = yes + no + other

            story.append(Paragraph(f"CUADRO: Resumen de Respuestas – {pregunta_col}", styles["Heading2"]))
            story.append(Spacer(1, 8))

            cuadro = [
                ["Respuesta", "Cantidad IIEE", "Porcentaje"],
                ["SI", str(yes), f"{(yes/total*100):.1f}%" if total else "0.0%"],
                ["NO", str(no), f"{(no/total*100):.1f}%" if total else "0.0%"],
                ["OTROS/VACÍO", str(other), f"{(other/total*100):.1f}%" if total else "0.0%"],
            ]
            tt = Table(cuadro, colWidths=[6*cm, 5*cm, 5*cm])
            tt.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (1, 1), (-1, -1), "CENTER"),
            ]))
            story.append(tt)
            story.append(Spacer(1, 10))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

    if st.button("📄 Generar PDF (MVP)"):
        pdf_bytes = build_pdf_bytes()
        st.success("PDF generado.")
        st.download_button(
            label="⬇️ Descargar Informe PDF",
            data=pdf_bytes,
            file_name="informe_visita_control_mvp.pdf",
            mime="application/pdf"
>>>>>>> f2a2ef5cbc135bc588eb48799d6f70eeebf3bb9d
        )