import os, json
import pandas as pd

def norm_str(s: str) -> str:
    return (s or "").lower()\
        .replace("á","a").replace("ã","a").replace("â","a").replace("à","a")\
        .replace("é","e").replace("ê","e").replace("è","e")\
        .replace("í","i").replace("ì","i")\
        .replace("ó","o").replace("õ","o").replace("ô","o").replace("ò","o")\
        .replace("ú","u").replace("ù","u").replace("û","u")\
        .replace("ç","c")

def montar_pasta_incentivo(cia: str, competencia: str) -> str | None:
    from extrato_app.CoreData.ds4 import obter_mes_ano, parse_meses_opt
    ROOT_NUMS = os.getenv("ROOT_NUMS", "")
    if not ROOT_NUMS:
        print("🚨 ROOT_NUMS não definido no .env")
        return None
    
    try:
        mes, ano = obter_mes_ano(competencia)
    except Exception:
        print(f"🚨 Competência inválida: {competencia}")
        return None

    MESES_PT = parse_meses_opt(os.getenv("MESES_OPT", ""))
    nome_mes = MESES_PT.get(mes)
    if not nome_mes:
        print(f"🚨 Nome do mês não encontrado em MESES_OPT para mes={mes}")
        return None
    
    pasta = os.path.join(ROOT_NUMS, str(ano), "Controle de produção", f"{mes} - {nome_mes}", cia)
    if not os.path.isdir(pasta):
        print(f"❌ Pasta não encontrada: {pasta}")
        return None
    return pasta

def encontrar_arquivo(pasta: str, padroes: list[str]) -> str | None:
    candidatos = [
        f for f in os.listdir(pasta)
        if f.lower().endswith(('.xls', '.xlsx'))
        and any(norm_str(p) in norm_str(f) for p in padroes)
        and not f.startswith("~$")
    ]
    if not candidatos:
        print(f"❌ Nenhum arquivo encontrado em {pasta} para padrões {padroes}")
        return None
    return max(candidatos, key=lambda f: os.path.getmtime(os.path.join(pasta, f)))

def get_ref_nom(df: pd.DataFrame, candidatos: list[str]) -> tuple[pd.DataFrame, str]:
    ref_nom = None
    try:
        with open(os.path.join(os.getcwd(), "config.json"), "r", encoding="utf-8") as cf:
            ref_nom = json.load(cf).get("ref_nom")
    except Exception:
        pass

    if ref_nom and ref_nom not in df.columns:
        for c in candidatos:
            if c in df.columns:
                df[ref_nom] = df[c]
                print(f"ℹ️ ref_nom='{ref_nom}' ausente; usando '{c}' como origem.")
                break
    elif not ref_nom:
        for c in candidatos:
            if c in df.columns:
                df[f"{c}_ref"] = df[c]
                ref_nom = f"{c}_ref"
                print(f"ℹ️ config sem ref_nom; usando '{c}' como referência provisória.")
                break
    return df, ref_nom
