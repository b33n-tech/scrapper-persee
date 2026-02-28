#!/usr/bin/env python3
"""
Persée OAI-PMH Harvester — Interface Streamlit
Lancement : streamlit run streamlit_persee.py
"""

import streamlit as st
import urllib.request
import xml.etree.ElementTree as ET
import csv
import time
import json
import io
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

OAI_BASE = "http://oai.persee.fr/oai"

NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "dc":     "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
}

FIELDNAMES = [
    "url_persee", "titre", "auteur", "date", "description",
    "sujet", "type", "source", "langue", "relation",
    "couverture", "editeur", "set_name", "identifier_oai",
]

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def fetch_xml(url, delay=1.0):
    time.sleep(delay)
    req = urllib.request.Request(url, headers={"User-Agent": "PerseeHarvester-Streamlit/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return ET.fromstring(r.read())

def list_sets(prefix="ephe"):
    url = f"{OAI_BASE}?verb=ListSets"
    sets = []
    while url:
        root = fetch_xml(url, delay=0.5)
        for s in root.findall(".//oai:set", NS):
            sid  = s.findtext("oai:setSpec",  namespaces=NS, default="")
            name = s.findtext("oai:setName",  namespaces=NS, default="")
            if prefix.lower() in sid.lower() or prefix.lower() in name.lower():
                sets.append({"id": sid, "name": name})
        token = root.findtext(".//oai:resumptionToken", namespaces=NS)
        url = f"{OAI_BASE}?verb=ListSets&resumptionToken={token}" if token and token.strip() else None
    return sets

def list_identifiers(set_id, delay=1.0):
    url = f"{OAI_BASE}?verb=ListIdentifiers&metadataPrefix=oai_dc&set={set_id}"
    ids = []
    while url:
        root = fetch_xml(url, delay=delay)
        for h in root.findall(".//oai:header", NS):
            if h.get("status") == "deleted":
                continue
            ident = h.findtext("oai:identifier", namespaces=NS, default="")
            if ident:
                ids.append(ident)
        token = root.findtext(".//oai:resumptionToken", namespaces=NS)
        url = f"{OAI_BASE}?verb=ListIdentifiers&resumptionToken={token.strip()}" if token and token.strip() else None
    return ids

def get_record(identifier, delay=1.0):
    url = f"{OAI_BASE}?verb=GetRecord&metadataPrefix=oai_dc&identifier={identifier}"
    root = fetch_xml(url, delay=delay)
    meta = root.find(".//oai_dc:dc", NS)
    if meta is None:
        return None

    def all_text(tag):
        return " | ".join(e.text.strip() for e in meta.findall(f"dc:{tag}", NS) if e.text)
    def first_text(tag):
        el = meta.find(f"dc:{tag}", NS)
        return el.text.strip() if el is not None and el.text else ""

    persee_url = ""
    if "persee:article/" in identifier:
        persee_url = "https://www.persee.fr/doc/" + identifier.split("persee:article/")[-1]
    elif "persee:issue/" in identifier:
        persee_url = "https://www.persee.fr/issue/" + identifier.split("persee:issue/")[-1]

    return {
        "identifier_oai": identifier,
        "url_persee":     persee_url,
        "titre":          first_text("title"),
        "auteur":         all_text("creator"),
        "date":           first_text("date"),
        "description":    first_text("description"),
        "sujet":          all_text("subject"),
        "type":           first_text("type"),
        "source":         first_text("source"),
        "langue":         first_text("language"),
        "relation":       all_text("relation"),
        "couverture":     all_text("coverage"),
        "editeur":        first_text("publisher"),
    }

def records_to_csv(records):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDNAMES, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue().encode("utf-8-sig")  # utf-8-sig = BOM pour Excel

# ─── UI ───────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Persée OAI Harvester",
    page_icon="📚",
    layout="wide"
)

st.title("📚 Persée OAI-PMH Harvester")
st.caption("Extraction structurée des métadonnées d'articles — export CSV / Airtable-ready")

# ── Sidebar : paramètres ──────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Paramètres")

    prefix = st.text_input(
        "Préfixe de série",
        value="ephe",
        help="Filtre les sets OAI contenant ce texte. Ex : ephe, bsaf, bulmo"
    )

    delay = st.slider(
        "Délai entre requêtes (sec)",
        min_value=0.5, max_value=3.0, value=1.0, step=0.1,
        help="Plus bas = plus rapide, mais risque de surcharger le serveur"
    )

    max_records = st.number_input(
        "Limite d'articles (0 = illimité)",
        min_value=0, value=0, step=100,
        help="Pratique pour tester avant un harvest complet"
    )

    st.divider()
    st.markdown("""
    **Mode d'emploi**
    1. Entrez le préfixe de la série
    2. Cliquez **Découvrir les sets**
    3. Sélectionnez les sections voulues
    4. Cliquez **Lancer le harvest**
    5. Téléchargez le CSV
    """)

    st.divider()
    st.markdown("**Préfixes utiles**")
    st.code("""
ephe   → EPHE (toutes sections)
bsaf   → Bull. Soc. Antiquaires Fr.
bulmo  → Bulletin monumental
gba    → Gazette des Beaux-Arts
    """)

# ── Étape 1 : Découverte des sets ─────────────────────────────────────────────
st.header("1 · Découverte des séries")

if "sets" not in st.session_state:
    st.session_state.sets = []
if "selected_sets" not in st.session_state:
    st.session_state.selected_sets = []
if "records" not in st.session_state:
    st.session_state.records = []
if "identifiers" not in st.session_state:
    st.session_state.identifiers = []

col1, col2 = st.columns([1, 3])

with col1:
    if st.button("🔍 Découvrir les sets", use_container_width=True):
        with st.spinner(f"Recherche des sets contenant « {prefix} »..."):
            try:
                sets = list_sets(prefix)
                st.session_state.sets = sets
                st.session_state.selected_sets = [s["id"] for s in sets]
                if sets:
                    st.success(f"{len(sets)} série(s) trouvée(s)")
                else:
                    st.warning("Aucun set trouvé pour ce préfixe.")
            except Exception as e:
                st.error(f"Erreur : {e}")

with col2:
    if st.session_state.sets:
        st.session_state.selected_sets = st.multiselect(
            "Séries à harvester",
            options=[s["id"] for s in st.session_state.sets],
            default=st.session_state.selected_sets,
            format_func=lambda x: next((s["name"] for s in st.session_state.sets if s["id"] == x), x)
        )

# ── Étape 2 : Harvest ─────────────────────────────────────────────────────────
st.header("2 · Harvest")

if st.session_state.selected_sets:
    if st.button("🚀 Lancer le harvest", type="primary", use_container_width=False):

        st.session_state.records = []
        st.session_state.identifiers = []
        errors = []

        # Collecte des identifiants
        progress_ids = st.progress(0, text="Collecte des identifiants...")
        status_ids   = st.empty()
        all_ids = []

        for i, set_id in enumerate(st.session_state.selected_sets):
            set_name = next((s["name"] for s in st.session_state.sets if s["id"] == set_id), set_id)
            status_ids.info(f"Identifiants : {set_name}...")
            try:
                ids = list_identifiers(set_id, delay=delay)
                for ident in ids:
                    all_ids.append({"identifier": ident, "set_id": set_id, "set_name": set_name})
            except Exception as e:
                st.warning(f"Erreur sur {set_id} : {e}")
            progress_ids.progress((i + 1) / len(st.session_state.selected_sets))

        # Déduplication
        seen = set()
        unique_ids = []
        for item in all_ids:
            if item["identifier"] not in seen:
                seen.add(item["identifier"])
                unique_ids.append(item)

        st.session_state.identifiers = unique_ids
        status_ids.success(f"✓ {len(unique_ids)} identifiants uniques collectés")

        # Récupération des métadonnées
        total = len(unique_ids) if max_records == 0 else min(max_records, len(unique_ids))
        progress_rec  = st.progress(0, text="Récupération des métadonnées...")
        status_rec    = st.empty()
        live_table    = st.empty()
        counter       = st.empty()

        records = []
        for i, item in enumerate(unique_ids[:total]):
            try:
                rec = get_record(item["identifier"], delay=delay)
                if rec:
                    rec["set_name"] = item["set_name"]
                    records.append(rec)
            except Exception as e:
                errors.append({"identifier": item["identifier"], "error": str(e)})

            pct = (i + 1) / total
            progress_rec.progress(pct, text=f"{i+1}/{total} articles")
            counter.caption(f"✓ {len(records)} récupérés · ✗ {len(errors)} erreurs")

            # Aperçu live tous les 10 articles
            if len(records) % 10 == 0 and records:
                df_preview = pd.DataFrame(records[-10:])
                cols_show = [c for c in ["auteur", "titre", "date", "source"] if c in df_preview.columns]
                live_table.dataframe(df_preview[cols_show], use_container_width=True)

        st.session_state.records = records
        progress_rec.progress(1.0, text="Harvest terminé ✓")
        status_rec.success(f"✓ {len(records)} articles récupérés · {len(errors)} erreurs")

# ── Étape 3 : Résultats & Export ──────────────────────────────────────────────
if st.session_state.records:
    st.header("3 · Résultats & Export")

    df = pd.DataFrame(st.session_state.records)

    # Stats rapides
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Articles", len(df))
    c2.metric("Auteurs uniques", df["auteur"].nunique() if "auteur" in df else "—")
    c3.metric("Période", f"{df['date'].min()} – {df['date'].max()}" if "date" in df else "—")
    c4.metric("Sets", df["set_name"].nunique() if "set_name" in df else "—")

    # Filtres rapides
    with st.expander("🔎 Filtrer les résultats"):
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            filter_auteur = st.text_input("Filtrer par auteur")
        with col_f2:
            filter_mot    = st.text_input("Filtrer par mot-clé (titre ou sujet)")

        df_filtered = df.copy()
        if filter_auteur:
            df_filtered = df_filtered[df_filtered["auteur"].str.contains(filter_auteur, case=False, na=False)]
        if filter_mot:
            mask = (
                df_filtered["titre"].str.contains(filter_mot, case=False, na=False) |
                df_filtered["sujet"].str.contains(filter_mot, case=False, na=False)
            )
            df_filtered = df_filtered[mask]
        st.caption(f"{len(df_filtered)} résultats après filtrage")

    # Tableau complet
    cols_display = [c for c in ["auteur", "titre", "date", "source", "sujet", "url_persee", "set_name"] if c in df.columns]
    st.dataframe(
        df_filtered[cols_display],
        use_container_width=True,
        height=400,
        column_config={
            "url_persee": st.column_config.LinkColumn("URL Persée"),
        }
    )

    # Export
    st.divider()
    st.subheader("📥 Export")
    col_e1, col_e2 = st.columns(2)

    with col_e1:
        csv_data = records_to_csv(st.session_state.records)
        st.download_button(
            label="⬇️ Télécharger CSV (Airtable-ready)",
            data=csv_data,
            file_name=f"persee_{prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True,
            type="primary"
        )

    with col_e2:
        json_data = json.dumps(st.session_state.records, ensure_ascii=False, indent=2).encode("utf-8")
        st.download_button(
            label="⬇️ Télécharger JSON",
            data=json_data,
            file_name=f"persee_{prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
            mime="application/json",
            use_container_width=True,
        )

    st.caption("Le CSV est encodé UTF-8 BOM — compatible Excel, Google Sheets et import direct Airtable.")
