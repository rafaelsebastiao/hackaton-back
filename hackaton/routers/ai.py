import shutil
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Request, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import settings
from .processing.v2_builder import construir_base_mestra_v2
from .processing.v3_2_moritz import gerar_planilha_v3_2
from .processing.nc_auditoria import processar_nc_auditoria

APP_ROOT = Path(__file__).resolve().parent
STORAGE = (APP_ROOT / ".." / "storage").resolve()
INPUTS = STORAGE / "inputs"
OUTPUTS = STORAGE / "outputs"
INPUTS.mkdir(parents=True, exist_ok=True)
OUTPUTS.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Auditoria IA Leve", version="0.1.0")

# Integração Front (Vite/React) <-> Back (FastAPI)
# - Dev: o front roda em http://localhost:5173
# - Prod: você pode restringir depois (ou controlar via reverse proxy)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"] ,
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(APP_ROOT / "static")), name="static")
templates = Jinja2Templates(directory=str(APP_ROOT / "templates"))


# Utilidades datas presets

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizar os nomes de colunas comoremove espaços, padroniza."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _to_dt(s: Any) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def _data_ancora_from_outputs(run_id: str) -> pd.Timestamp | None:
    """Usa a ltima data disponível nos dados do run """
    out_dir = OUTPUTS / run_id
    p = out_dir / "BASE_AGREGADA_DIA_LINHA_PN.xlsx"
    if not p.exists():
        return None
    try:
        df = pd.read_excel(p, sheet_name="AGREGADA_DIA_LINHA_PN")
        df = _norm_cols(df)
        if "DATA" not in df.columns:
            return None
        d = _to_dt(df["DATA"])
        mx = d.max()
        if pd.isna(mx):
            return None
        return pd.Timestamp(mx).normalize()
    except Exception:
        return None


def _periodo_range(preset: str | None, anchor: pd.Timestamp | None) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str]:
    """Converte presets do front em intervalo de datas pra nao cagar."""
    preset = (preset or "desde_sempre").strip().lower()
    # normaliza alguns sinonimos pro front
    preset = preset.replace("-", "_")
    if preset in {"all", "desde", "desde_sempre", "desde sempre", "sempre", "since"}:
        return None, None, "Desde sempre"
    if anchor is None or pd.isna(anchor):
        # sem âncora ele não consegue filtrar
        return None, None, "Desde sempre"

    end = anchor.normalize()

    if preset in {"hoje", "today"}:
        return end, end, f"Hoje ({end.date()})"
    if preset in {"ultimos_7_dias", "ultimos 7 dias", "últimos 7 dias", "7d", "last7"}:
        start = end - pd.Timedelta(days=6)
        return start, end, f"Últimos 7 dias ({start.date()}–{end.date()})"
    if preset in {"ultimos_30_dias", "ultimos 30 dias", "últimos 30 dias", "ultimo_mes", "último mês", "30d", "last30"}:
        start = end - pd.Timedelta(days=29)
        return start, end, f"Últimos 30 dias ({start.date()}–{end.date()})"

    if preset in {"este_mes", "mtd", "month_to_date", "mes_atual", "mês atual"}:
        start = end.replace(day=1)
        return start, end, f"Este mês ({start.date()}–{end.date()})"

    # fallback
    return None, None, "Desde sempre"


def _filter_period(df: pd.DataFrame, col_data: str, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if start is None and end is None:
        return df
    d = _to_dt(df[col_data])
    m = pd.Series(True, index=df.index)
    if start is not None:
        m &= (d >= start)
    if end is not None:
        m &= (d <= end + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1))
    return df[m].copy()


def _normalize_0_100(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0)
    mx = s.max()
    if mx <= 0:
        return pd.Series([0.0] * len(s), index=s.index)
    return (s / mx) * 100.0


def _classificar(score: float) -> str:
    if score >= 80:
        return "Crítico"
    if score >= 55:
        return "Atenção"
    if score >= 30:
        return "Médio"
    return "Estável"


def _nivel_simples(score: float) -> str:
    """Rótulo simples Baixa/Média/Alta para leitura rápida do meu auditor."""
    if score >= 70:
        return "Alta"
    if score >= 45:
        return "Média"
    return "Baixa"

#puta merda que desgraça mecher nessa porra de run id ta slk eu att a pagina e saporra morre e nao armazaena inferno do caralho
def save_upload(run_id: str, up: UploadFile, name: str) -> str:
    run_dir = INPUTS / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    dest = run_dir / name
    with dest.open("wb") as f:
        shutil.copyfileobj(up.file, f)
    return str(dest)

def make_outputs_dir(run_id: str) -> Path:
    out_dir = OUTPUTS / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "use_moritz": settings.USE_MORITZ,
        "peso_ia_texto": settings.PESO_IA_TEXTO,
        "modelo": settings.MORITZ_MODEL
    })

@app.post("/process", response_class=HTMLResponse)
async def process(
    request: Request,
    reclamacoes: UploadFile = File(...),
    refugos: UploadFile = File(...),
    mapa_cc: UploadFile = File(...),
    auditoria_nc: UploadFile = File(...),
    start_date: str | None = Form(None),
    end_date: str | None = Form(None),
):
    run_id = str(uuid.uuid4())[:8]

    path_recl = save_upload(run_id, reclamacoes, "reclamacoes.xlsx")
    path_ref  = save_upload(run_id, refugos, "refugos.xlsx")
    path_map  = save_upload(run_id, mapa_cc, "mapa_cc.xlsx")
    path_nc = save_upload(run_id, auditoria_nc, "auditoria_nc.xlsx")
    out_dir = make_outputs_dir(run_id)

    # Importante o processamento pesado gera uma base completa não filtrada
    # para permitir filtros por período depois sem reprocessar.
    result_v2 = construir_base_mestra_v2(
        arquivo_refugo=path_ref,
        arquivo_reclamacoes=path_recl,
        arquivo_mapa_cc=path_map,
        codigos_excluir=settings.CODIGOS_EXCLUIR,
        start_date=None,
        end_date=None
    )


    # Auditoria NC   também sem filtro aqui filtro posterior em cima da base

    nc_pack = None
    try:
        nc_pack = processar_nc_auditoria(path_nc, start_date=None, end_date=None)
        # Enriquecer a base mestre por meio de LINHA mesma NC para todos PNs daquela linha
        if "nc_linhas" in nc_pack and not nc_pack["nc_linhas"].empty:
            mestre = result_v2["mestre"].merge(nc_pack["nc_linhas"], on="LINHA", how="left")
            result_v2["mestre"] = mestre
    except Exception as e:
        # não quebra o fluxo se a planilha vier diferente
        nc_pack = {"erro": str(e)}

    base_v2_path = out_dir / "BASE_MESTRA_AUDITORIA_V2.xlsx"
    rastreio_path = out_dir / "PN_RASTREIO_ORIGINAL_LIMPO.xlsx"
    colisoes_path = out_dir / "PN_COLISOES.xlsx"

    result_v2["mestre"].to_excel(base_v2_path, index=False)
    result_v2["rastreio"].to_excel(rastreio_path, index=False)
    result_v2["colisoes"].to_excel(colisoes_path, index=False)

    #  Saídas analíticas para filtros posteriores por data/período
    # Base de eventos unificada totalmente
    eventos = result_v2.get("eventos", pd.DataFrame()).copy()
    # Acoplar NC como eventos se existir
    if isinstance(nc_pack, dict) and "nc_raw" in nc_pack and isinstance(nc_pack["nc_raw"], pd.DataFrame) and not nc_pack["nc_raw"].empty:
        nc_raw = nc_pack["nc_raw"].copy()
        nc_ev = pd.DataFrame({
            "TIPO": "NC_AUDITORIA",
            "DATA_EVENTO": pd.to_datetime(nc_raw.get("Created"), errors="coerce"),
            "LINHA_ORIGINAL": nc_raw.get("LINHA_ORIGINAL", ""),
            "LINHA": nc_raw.get("LINHA", "SEM_LINHA"),
            "PN_ORIGINAL": "",
            "PN_LIMPO": "",
            "DESCRICAO": nc_raw.get("Description", ""),
            "QTD": 0,
            "FREQ": 1,
            "STATUS": nc_raw.get("Status", ""),
            "DUE_DATE": pd.to_datetime(nc_raw.get("Due date"), errors="coerce"),
            "CLOSING_DATE": pd.to_datetime(nc_raw.get("Closing date"), errors="coerce"),
            "Q14": nc_raw.get("14Q", ""),
        })
        eventos = pd.concat([eventos, nc_ev], ignore_index=True)

    eventos["DATA_EVENTO"] = pd.to_datetime(eventos.get("DATA_EVENTO"), errors="coerce")
    eventos["FLAG_RISCO_OCULTO"] = ((eventos.get("LINHA", "SEM_LINHA") == "SEM_LINHA") | (eventos.get("PN_LIMPO", "") == "DESCONHECIDO") | (eventos.get("PN_LIMPO", "") == "")).astype(int)

    base_eventos_path = out_dir / "BASE_EVENTOS_LONG.xlsx"
    risco_oculto_path = out_dir / "RISCO_OCULTO.xlsx"
    base_diaria_path  = out_dir / "BASE_AGREGADA_DIA_LINHA_PN.xlsx"
    colisoes_linha_path = out_dir / "COLISOES_LINHA.xlsx"

    # Base agregada por dia/linha/pn para  filtros rápidos
    base_diaria = eventos.copy()
    base_diaria["DATA"] = base_diaria["DATA_EVENTO"].dt.date
    # normaliza PN vazio pra n cagar no final
    base_diaria["PN_LIMPO"] = base_diaria.get("PN_LIMPO", "").fillna("")
    base_diaria["LINHA"] = base_diaria.get("LINHA", "SEM_LINHA").fillna("SEM_LINHA")

    def _cnt(mask):
        return mask.astype(int)

    base_diaria["REF_QTD"] = pd.to_numeric(base_diaria.get("QTD", 0), errors="coerce").fillna(0)
    base_diaria["REF_FREQ"] = _cnt(base_diaria["TIPO"] == "REFUGO")
    base_diaria["REC_FORMAL"] = _cnt(base_diaria["TIPO"] == "RECLAMACAO_FORMAL")
    base_diaria["REC_INFORMAL"] = _cnt(base_diaria["TIPO"] == "RECLAMACAO_INFORMAL")
    base_diaria["NC_TOTAL"] = _cnt(base_diaria["TIPO"] == "NC_AUDITORIA")
    base_diaria["NC_ABERTA"] = _cnt((base_diaria["TIPO"] == "NC_AUDITORIA") & (base_diaria.get("CLOSING_DATE").isna()))
    # vencida due existe, aberta, e due < hoje requisito pedido no ultimo feedback com a ana 
    hoje = pd.Timestamp.now().normalize().date()
    due_dt = pd.to_datetime(base_diaria.get("DUE_DATE"), errors="coerce").dt.date
    base_diaria["NC_VENCIDA"] = _cnt((base_diaria["TIPO"] == "NC_AUDITORIA") & (base_diaria.get("CLOSING_DATE").isna()) & (due_dt.notna()) & (due_dt < hoje))

    base_diaria_agg = base_diaria.groupby(["DATA","LINHA","PN_LIMPO"], dropna=False).agg(
        REF_QTD_SUM=("REF_QTD","sum"),
        REF_FREQ_SUM=("REF_FREQ","sum"),
        REC_FORMAL_SUM=("REC_FORMAL","sum"),
        REC_INFORMAL_SUM=("REC_INFORMAL","sum"),
        NC_TOTAL_SUM=("NC_TOTAL","sum"),
        NC_ABERTA_SUM=("NC_ABERTA","sum"),
        NC_VENCIDA_SUM=("NC_VENCIDA","sum"),
    ).reset_index()

    # Colisõesvariações de linha para auditoria do dado
    col_lin = eventos.copy()
    col_lin["LINHA_ORIGINAL"] = col_lin.get("LINHA_ORIGINAL", "").fillna("").astype(str)
    col_lin = col_lin[col_lin["LINHA_ORIGINAL"].str.strip() != ""].groupby("LINHA")["LINHA_ORIGINAL"].apply(lambda s: " | ".join(sorted(set(s))[:30])).reset_index(name="EXEMPLOS_LINHA_ORIGINAL")
    col_lin["QTD_VARIACOES"] = col_lin["EXEMPLOS_LINHA_ORIGINAL"].apply(lambda x: len([p for p in str(x).split("|") if p.strip()]))

    # Exportações normais
    risco_oculto = eventos[eventos["FLAG_RISCO_OCULTO"] == 1].copy()
    with pd.ExcelWriter(base_eventos_path, engine="openpyxl") as w:
        eventos.to_excel(w, sheet_name="EVENTOS", index=False)
    with pd.ExcelWriter(risco_oculto_path, engine="openpyxl") as w:
        risco_oculto.to_excel(w, sheet_name="RISCO_OCULTO", index=False)
    with pd.ExcelWriter(base_diaria_path, engine="openpyxl") as w:
        base_diaria_agg.to_excel(w, sheet_name="AGREGADA_DIA_LINHA_PN", index=False)
    with pd.ExcelWriter(colisoes_linha_path, engine="openpyxl") as w:
        col_lin.to_excel(w, sheet_name="COLISOES_LINHA", index=False)

    pesos = {
        "PESO_FORMAL": settings.PESO_FORMAL,
        "PESO_INFORMAL": settings.PESO_INFORMAL,
        "PESO_REF_QTD": settings.PESO_REF_QTD,
        "PESO_REF_FREQ": settings.PESO_REF_FREQ,
        "PESO_IA_TEXTO": settings.PESO_IA_TEXTO,
    }

    plan = gerar_planilha_v3_2(
        mestre=result_v2["mestre"],
        pesos=pesos,
        moritz_model=settings.MORITZ_MODEL,
        use_moritz=settings.USE_MORITZ,
        nc_linhas=(nc_pack.get("nc_linhas") if isinstance(nc_pack, dict) and "nc_linhas" in nc_pack else None)
    )

    ia_path = out_dir / "RESULTADO_AUDITORIA_V3_2_MORITZ.xlsx"
    with pd.ExcelWriter(ia_path, engine="openpyxl") as w:
        plan["reativo"].to_excel(w, sheet_name="RANKING_REATIVO", index=False)
        plan["preventivo"].to_excel(w, sheet_name="RANKING_PREVENTIVO", index=False)
        plan["top_linhas"].to_excel(w, sheet_name="TOP_LINHAS", index=False)


    # Export extra resumo Auditoria NC 
    if isinstance(nc_pack, dict) and "nc_linhas" in nc_pack:
        nc_path_out = out_dir / "RESUMO_AUDITORIA_NC.xlsx"
        with pd.ExcelWriter(nc_path_out, engine="openpyxl") as w:
            nc_pack["nc_linhas"].to_excel(w, sheet_name="NC_LINHAS", index=False)
            nc_pack["nc_raw"].to_excel(w, sheet_name="NC_RAW", index=False)

    return templates.TemplateResponse("result.html", {
        "request": request,
        "run_id": run_id,
        "files": [
            ("Base organizada (V2)", f"/download/{run_id}/BASE_MESTRA_AUDITORIA_V2.xlsx"),
            ("IA trabalhada (V3.2 Moritz)", f"/download/{run_id}/RESULTADO_AUDITORIA_V3_2_MORITZ.xlsx"),
            ("Rastreio PN", f"/download/{run_id}/PN_RASTREIO_ORIGINAL_LIMPO.xlsx"),
            ("Colisões PN", f"/download/{run_id}/PN_COLISOES.xlsx"),
            ("Resumo Auditoria/NC", f"/download/{run_id}/RESUMO_AUDITORIA_NC.xlsx"),
            ("Base de Eventos (para filtros por período)", f"/download/{run_id}/BASE_EVENTOS_LONG.xlsx"),
            ("Base agregada Dia/Linha/PN", f"/download/{run_id}/BASE_AGREGADA_DIA_LINHA_PN.xlsx"),
            ("Risco Oculto", f"/download/{run_id}/RISCO_OCULTO.xlsx"),
            ("Colisões de Linha", f"/download/{run_id}/COLISOES_LINHA.xlsx"),
        ]
    })


# API para integrar com o Front React/Vite - nota : estudar mais api e js pois essa merda foi feita na tentativa e erro dessa merda ai 
@app.post("/api/process")
async def api_process(
    reclamacoes: UploadFile = File(...),
    refugos: UploadFile = File(...),
    mapa_cc: UploadFile = File(...),
    auditoria_nc: UploadFile = File(...),
    start_date: str | None = Form(None),
    end_date: str | None = Form(None),
):
    """Mesmo pipeline do process mas retornando JSON pra leitura.

    Retorna o seguinte JSON:
       run_id
       links de download
       TOP_LINHAS em JSON para a Matriz de Risco do front
    """

    run_id = str(uuid.uuid4())[:8]

    path_recl = save_upload(run_id, reclamacoes, "reclamacoes.xlsx")
    path_ref  = save_upload(run_id, refugos, "refugos.xlsx")
    path_map  = save_upload(run_id, mapa_cc, "mapa_cc.xlsx")
    path_nc   = save_upload(run_id, auditoria_nc, "auditoria_nc.xlsx")
    out_dir = make_outputs_dir(run_id)

    result_v2 = construir_base_mestra_v2(
        arquivo_refugo=path_ref,
        arquivo_reclamacoes=path_recl,
        arquivo_mapa_cc=path_map,
        codigos_excluir=settings.CODIGOS_EXCLUIR,
        start_date=None,
        end_date=None
    )

    nc_pack = None
    try:
        nc_pack = processar_nc_auditoria(path_nc, start_date=None, end_date=None)
        if "nc_linhas" in nc_pack and not nc_pack["nc_linhas"].empty:
            mestre = result_v2["mestre"].merge(nc_pack["nc_linhas"], on="LINHA", how="left")
            result_v2["mestre"] = mestre
    except Exception as e:
        nc_pack = {"erro": str(e)}

    # Exportações principais
    base_v2_path = out_dir / "BASE_MESTRA_AUDITORIA_V2.xlsx"
    result_v2["mestre"].to_excel(base_v2_path, index=False)

    # Saídas analíticas para filtros posteriores por data
    # Base de eventos unificada V2 já retorna um "eventos" longo
    eventos = result_v2.get("eventos", pd.DataFrame()).copy()

    # Acoplar NC como eventos se existir
    if isinstance(nc_pack, dict) and "nc_raw" in nc_pack and isinstance(nc_pack["nc_raw"], pd.DataFrame) and not nc_pack["nc_raw"].empty:
        nc_raw = nc_pack["nc_raw"].copy()
        nc_ev = pd.DataFrame({
            "TIPO": "NC_AUDITORIA",
            "DATA_EVENTO": pd.to_datetime(nc_raw.get("Created"), errors="coerce"),
            "LINHA_ORIGINAL": nc_raw.get("LINHA_ORIGINAL", ""),
            "LINHA": nc_raw.get("LINHA", "SEM_LINHA"),
            "PN_ORIGINAL": "",
            "PN_LIMPO": "",
            "DESCRICAO": nc_raw.get("Description", ""),
            "QTD": 0,
            "FREQ": 1,
            "STATUS": nc_raw.get("Status", ""),
            "DUE_DATE": pd.to_datetime(nc_raw.get("Due date"), errors="coerce"),
            "CLOSING_DATE": pd.to_datetime(nc_raw.get("Closing date"), errors="coerce"),
            "Q14": nc_raw.get("14Q", ""),
        })
        eventos = pd.concat([eventos, nc_ev], ignore_index=True)

    eventos["DATA_EVENTO"] = pd.to_datetime(eventos.get("DATA_EVENTO"), errors="coerce")
    eventos["LINHA"] = eventos.get("LINHA", "SEM_LINHA").fillna("SEM_LINHA")
    eventos["PN_LIMPO"] = eventos.get("PN_LIMPO", "").fillna("")
    eventos["DESCRICAO"] = eventos.get("DESCRICAO", "").fillna("")

    eventos["FLAG_RISCO_OCULTO"] = (
        (eventos.get("LINHA", "SEM_LINHA") == "SEM_LINHA")
        | (eventos.get("PN_LIMPO", "") == "DESCONHECIDO")
        | (eventos.get("PN_LIMPO", "") == "")
    ).astype(int)

    base_eventos_path = out_dir / "BASE_EVENTOS_LONG.xlsx"
    base_diaria_path = out_dir / "BASE_AGREGADA_DIA_LINHA_PN.xlsx"

    # Base agregada por dia/linha/pn  para filtros 
    base_diaria = eventos.copy()
    base_diaria["DATA"] = base_diaria["DATA_EVENTO"].dt.normalize()

    def _cnt(mask: pd.Series) -> pd.Series:
        return mask.astype(int)

    base_diaria["REF_QTD"] = pd.to_numeric(base_diaria.get("QTD", 0), errors="coerce").fillna(0)
    base_diaria["REF_FREQ"] = _cnt(base_diaria["TIPO"] == "REFUGO")
    base_diaria["REC_FORMAL"] = _cnt(base_diaria["TIPO"] == "RECLAMACAO_FORMAL")
    base_diaria["REC_INFORMAL"] = _cnt(base_diaria["TIPO"] == "RECLAMACAO_INFORMAL")
    base_diaria["NC_TOTAL"] = _cnt(base_diaria["TIPO"] == "NC_AUDITORIA")
    base_diaria["NC_ABERTA"] = _cnt((base_diaria["TIPO"] == "NC_AUDITORIA") & (base_diaria.get("CLOSING_DATE").isna()))

    # Usa data âncora  nos dados evita filtro vazio
    anchor_tmp = base_diaria["DATA"].dropna().max()
    anchor_tmp = pd.Timestamp(anchor_tmp).normalize() if pd.notna(anchor_tmp) else pd.Timestamp.now().normalize()
    due_dt = pd.to_datetime(base_diaria.get("DUE_DATE"), errors="coerce").dt.normalize()
    base_diaria["NC_VENCIDA"] = _cnt(
        (base_diaria["TIPO"] == "NC_AUDITORIA")
        & (base_diaria.get("CLOSING_DATE").isna())
        & (due_dt.notna())
        & (due_dt < anchor_tmp)
    )

    base_diaria_agg = base_diaria.groupby(["DATA", "LINHA", "PN_LIMPO"], dropna=False).agg(
        REF_QTD_SUM=("REF_QTD", "sum"),
        REF_FREQ_SUM=("REF_FREQ", "sum"),
        REC_FORMAL_SUM=("REC_FORMAL", "sum"),
        REC_INFORMAL_SUM=("REC_INFORMAL", "sum"),
        NC_TOTAL_SUM=("NC_TOTAL", "sum"),
        NC_ABERTA_SUM=("NC_ABERTA", "sum"),
        NC_VENCIDA_SUM=("NC_VENCIDA", "sum"),
    ).reset_index()

    with pd.ExcelWriter(base_eventos_path, engine="openpyxl") as w:
        eventos.to_excel(w, sheet_name="EVENTOS", index=False)
    with pd.ExcelWriter(base_diaria_path, engine="openpyxl") as w:
        base_diaria_agg.to_excel(w, sheet_name="AGREGADA_DIA_LINHA_PN", index=False)

    pesos = {
        "PESO_FORMAL": settings.PESO_FORMAL,
        "PESO_INFORMAL": settings.PESO_INFORMAL,
        "PESO_REF_QTD": settings.PESO_REF_QTD,
        "PESO_REF_FREQ": settings.PESO_REF_FREQ,
        "PESO_IA_TEXTO": settings.PESO_IA_TEXTO,
    }

    plan = gerar_planilha_v3_2(
        mestre=result_v2["mestre"],
        pesos=pesos,
        moritz_model=settings.MORITZ_MODEL,
        use_moritz=settings.USE_MORITZ,
        nc_linhas=(nc_pack.get("nc_linhas") if isinstance(nc_pack, dict) and "nc_linhas" in nc_pack else None)
    )

    ia_path = out_dir / "RESULTADO_AUDITORIA_V3_2_MORITZ.xlsx"
    with pd.ExcelWriter(ia_path, engine="openpyxl") as w:
        plan["reativo"].to_excel(w, sheet_name="RANKING_REATIVO", index=False)
        plan["preventivo"].to_excel(w, sheet_name="RANKING_PREVENTIVO", index=False)
        plan["top_linhas"].to_excel(w, sheet_name="TOP_LINHAS", index=False)

    # Serializa TOP_LINHAS para o front
    top_linhas_df = plan.get("top_linhas", pd.DataFrame()).copy()
    # Garantir que NaN não vire 'NaN' no JSON pra n quebrar com a logica da leitura no front
    top_linhas_df = top_linhas_df.where(pd.notnull(top_linhas_df), None)

    files = {
        "base_v2": f"/download/{run_id}/BASE_MESTRA_AUDITORIA_V2.xlsx",
        "resultado": f"/download/{run_id}/RESULTADO_AUDITORIA_V3_2_MORITZ.xlsx",
    }
    return {
        "ok": True,
        "run_id": run_id,
        "files": files,
        "top_linhas": top_linhas_df.to_dict(orient="records"),
        "use_moritz": settings.USE_MORITZ,
        "model": settings.MORITZ_MODEL,
        "anchor_date": (pd.Timestamp(anchor_tmp).date().isoformat() if 'anchor_tmp' in locals() and pd.notna(anchor_tmp) else None),
    }


@app.get("/api/context/{run_id}")
def api_context(run_id: str):
    """Metadados do run (principalmente datas disponíveis)."""
    out_dir = OUTPUTS / run_id
    p = out_dir / "BASE_AGREGADA_DIA_LINHA_PN.xlsx"
    if not p.exists():
        return {"ok": False, "error": "run_id não encontrado"}
    df = pd.read_excel(p, sheet_name="AGREGADA_DIA_LINHA_PN")
    df = _norm_cols(df)
    df["DATA"] = _to_dt(df.get("DATA"))
    mn = df["DATA"].min()
    mx = df["DATA"].max()
    anchor = pd.Timestamp(mx).normalize() if pd.notna(mx) else None
    return {
        "ok": True,
        "run_id": run_id,
        "min_date": (pd.Timestamp(mn).date().isoformat() if pd.notna(mn) else None),
        "max_date": (pd.Timestamp(mx).date().isoformat() if pd.notna(mx) else None),
        "anchor_date": (anchor.date().isoformat() if anchor is not None else None),
    }


@app.get("/api/top_linhas/{run_id}")
def api_top_linhas(run_id: str, preset: str | None = None, limit: int = 15):
    """Ranking de linhas por período sem reprocessar (usa base agregada salva no run)."""
    out_dir = OUTPUTS / run_id
    p = out_dir / "BASE_AGREGADA_DIA_LINHA_PN.xlsx"
    if not p.exists():
        return {"ok": False, "error": "run_id não encontrado"}

    df = pd.read_excel(p, sheet_name="AGREGADA_DIA_LINHA_PN")
    df = _norm_cols(df)
    df["DATA"] = _to_dt(df.get("DATA"))

    anchor = _data_ancora_from_outputs(run_id)
    if anchor is None and df["DATA"].notna().any():
        anchor = pd.Timestamp(df["DATA"].max()).normalize()
    start, end, label = _periodo_range(preset, anchor)
    dfp = _filter_period(df, "DATA", start, end)

    if dfp is None or dfp.empty:
        return {
            "ok": True,
            "run_id": run_id,
            "period_label": label,
            "anchor_date": (anchor.date().isoformat() if anchor is not None else None),
            "top_linhas": [],
        }

    g = dfp.groupby("LINHA", dropna=False).agg(
        REF_QTD=("REF_QTD_SUM", "sum"),
        REF_FREQ=("REF_FREQ_SUM", "sum"),
        REC_FORMAL=("REC_FORMAL_SUM", "sum"),
        REC_INFORMAL=("REC_INFORMAL_SUM", "sum"),
        NC_TOTAL=("NC_TOTAL_SUM", "sum"),
        NC_ABERTA=("NC_ABERTA_SUM", "sum"),
        NC_VENCIDA=("NC_VENCIDA_SUM", "sum"),
    ).reset_index()

    g["TOTAL_RECLAMACOES"] = g["REC_FORMAL"] + g["REC_INFORMAL"]

    # Score simples  de 0 ao100
    s_rec = _normalize_0_100(g["TOTAL_RECLAMACOES"])
    s_nc = _normalize_0_100(g["NC_TOTAL"])
    s_refq = _normalize_0_100(g["REF_QTD"])
    s_reff = _normalize_0_100(g["REF_FREQ"])

    score = (0.40 * s_rec) + (0.30 * s_nc) + (0.20 * s_refq) + (0.10 * s_reff)
    g["Score_Linha"] = score.round(2)
    g["Classe_Linha"] = g["Score_Linha"].apply(_classificar)

    g = g.sort_values("Score_Linha", ascending=False).head(max(1, int(limit)))

    out = []
    for _, r in g.iterrows():
        out.append({
            "LINHA": r.get("LINHA", "SEM_LINHA"),
            "Score_Linha": float(r.get("Score_Linha", 0) or 0),
            "Classe_Linha": r.get("Classe_Linha", "Médio"),
            "Nivel": _nivel_simples(float(r.get("Score_Linha", 0) or 0)),
            "Total_Reclamacoes": int(r.get("TOTAL_RECLAMACOES", 0) or 0),
            "Reclamacoes_Formais": int(r.get("REC_FORMAL", 0) or 0),
            "Reclamacoes_Informais": int(r.get("REC_INFORMAL", 0) or 0),
            "NC_Total": int(r.get("NC_TOTAL", 0) or 0),
            "NC_Aberta": int(r.get("NC_ABERTA", 0) or 0),
            "NC_Vencida": int(r.get("NC_VENCIDA", 0) or 0),
            "Refugo_Qtd": float(r.get("REF_QTD", 0) or 0),
            "Refugo_Freq": int(r.get("REF_FREQ", 0) or 0),
        })

    return {
        "ok": True,
        "run_id": run_id,
        "period_label": label,
        "anchor_date": (anchor.date().isoformat() if anchor is not None else None),
        "top_linhas": out,
    }


@app.post("/api/chat")
async def api_chat(payload: dict):
    """Assistente baseado em dados (sem LLM)."""
    run_id = str(payload.get("run_id") or "").strip()
    msg = str(payload.get("message") or "").strip()
    preset = payload.get("preset")
    if not run_id:
        return {"ok": False, "reply": "Nenhum run_id encontrado. Processe as planilhas primeiro na Matriz de Risco."}
    if not msg:
        return {"ok": False, "reply": "Mensagem vazia."}

    anchor = _data_ancora_from_outputs(run_id)

    # Carrega base agregada e eventos
    out_dir = OUTPUTS / run_id
    p_base = out_dir / "BASE_AGREGADA_DIA_LINHA_PN.xlsx"
    p_ev = out_dir / "BASE_EVENTOS_LONG.xlsx"
    if not p_base.exists() or not p_ev.exists():
        return {"ok": False, "reply": "Não encontrei as bases do run. Reprocesse as planilhas."}

    df = pd.read_excel(p_base, sheet_name="AGREGADA_DIA_LINHA_PN")
    df = _norm_cols(df)
    df["DATA"] = _to_dt(df.get("DATA"))

    ev = pd.read_excel(p_ev, sheet_name="EVENTOS")
    ev = _norm_cols(ev)
    ev["DATA_EVENTO"] = _to_dt(ev.get("DATA_EVENTO"))

    # Se não há âncora formal usar a última data real disponível nos dados pra pelo menos ter uma base çegal
    if anchor is None:
        cands = []
        if df["DATA"].notna().any():
            cands.append(pd.Timestamp(df["DATA"].max()).normalize())
        if ev["DATA_EVENTO"].notna().any():
            cands.append(pd.Timestamp(ev["DATA_EVENTO"].max()).normalize())
        anchor = max(cands) if cands else None

    start, end, label = _periodo_range(preset, anchor)

    dfp = _filter_period(df, "DATA", start, end)
    evp = _filter_period(ev, "DATA_EVENTO", start, end)

    low = msg.lower()

    # intents simples 
    if "top" in low and "linha" in low:
        top = api_top_linhas(run_id, preset=preset, limit=5)
        linhas = top.get("top_linhas", [])
        if not linhas:
            return {"ok": True, "reply": f"Não encontrei eventos no período **{label}**.\n\nDica: use *Desde sempre* para validar se há histórico."}
        txt = [f"Top linhas por risco — **{label}**:"]
        for i, r in enumerate(linhas, start=1):
            txt.append(
                f"{i}. {r['LINHA']} — Score {r['Score_Linha']:.1f} | NC {r['NC_Total']} | Reclamações {r['Total_Reclamacoes']} | Refugo {r['Refugo_Qtd']:.0f}"
            )
        return {"ok": True, "reply": "\n".join(txt)}

    # descrição técnica da linha filtrada
    if "descr" in low or "tecn" in low or "resumo" in low:
        # extrai identificador da linha
        import re
        m = re.search(r"linha\s*([0-9]+)", low)
        if not m:
            return {"ok": True, "reply": "Para eu gerar a descrição técnica, mande algo como: **descrição técnica da linha 2**."}
        linha = f"LINHA {m.group(1)}"

        # agrega métricas gerais 
        g = dfp.groupby("LINHA", dropna=False).agg(
            REF_QTD=("REF_QTD_SUM", "sum"),
            REF_FREQ=("REF_FREQ_SUM", "sum"),
            REC_FORMAL=("REC_FORMAL_SUM", "sum"),
            REC_INFORMAL=("REC_INFORMAL_SUM", "sum"),
            NC_TOTAL=("NC_TOTAL_SUM", "sum"),
            NC_ABERTA=("NC_ABERTA_SUM", "sum"),
            NC_VENCIDA=("NC_VENCIDA_SUM", "sum"),
        ).reset_index()
        row = g[g["LINHA"].astype(str).str.upper() == linha.upper()]
        if row.empty:
            return {"ok": True, "reply": f"Não encontrei dados para **{linha}** no período **{label}**."}
        r = row.iloc[0]
        total_rec = int((r.get("REC_FORMAL", 0) or 0) + (r.get("REC_INFORMAL", 0) or 0))

        # top descrições
        ev_line = evp[evp.get("LINHA", "SEM_LINHA").astype(str).str.upper() == linha.upper()].copy()
        motivos = []
        if not ev_line.empty and "DESCRICAO" in ev_line.columns:
            vc = ev_line["DESCRICAO"].fillna("").astype(str)
            vc = vc[vc.str.strip() != ""]
            if not vc.empty:
                motivos = vc.value_counts().head(5).index.tolist()

        partes = [
            f"**Descrição técnica — {linha} ({label})**",
            f"• Reclamações: **{total_rec}** (Formais {int(r.get('REC_FORMAL',0) or 0)} | Informais {int(r.get('REC_INFORMAL',0) or 0)})",
            f"• NCs: **{int(r.get('NC_TOTAL',0) or 0)}** (Abertas {int(r.get('NC_ABERTA',0) or 0)} | Vencidas {int(r.get('NC_VENCIDA',0) or 0)})",
            f"• Refugo: **{float(r.get('REF_QTD',0) or 0):.0f}** (freq {int(r.get('REF_FREQ',0) or 0)})",
        ]
        if motivos:
            partes.append("\nPrincipais descrições/motivos (amostra do período):")
            for mtx in motivos:
                partes.append(f"- {mtx[:120]}")
        partes.append("\nFoco sugerido de auditoria (check rápido):")
        partes.append("- Evidência de execução do método (IT / setup / parâmetros)")
        partes.append("- Contenção e eficácia de ações corretivas (se NC aberta/vencida)")
        partes.append("- Ponto de controle relacionado ao motivo mais recorrente")

        return {"ok": True, "reply": "\n".join(partes)}

    # fallback de ajuda
    return {
        "ok": True,
        "reply": (
            "Eu consigo responder com base nas planilhas deste run. Tente comandos prontos:\n"
            "- **top 5 linhas críticas**\n"
            "- **descrição técnica da linha 2**\n"
            "- **hoje / últimos 7 dias / último mês / desde sempre** (use no seletor de período no chat)"
        ),
    }

@app.get("/download/{run_id}/{filename}")
def download(run_id: str, filename: str):
    out_dir = OUTPUTS / run_id
    path = out_dir / filename
    if not path.exists():
        return HTMLResponse(f"<h3>Arquivo não encontrado.</h3><p>{path}</p>", status_code=404)
    return FileResponse(path, filename=filename)

@app.get("/health")
def health():
    return {"ok": True, "use_moritz": settings.USE_MORITZ, "model": settings.MORITZ_MODEL}
