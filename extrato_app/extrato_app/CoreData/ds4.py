import os
import json
import sys
from typing import Dict, Tuple, Any

def garantir_config_json():

    print('started garantir_config_json')
    config_path = os.path.join(os.getcwd(), "config.json")
    if not os.path.exists(config_path):
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump({"cia_corresp": "", "competencia": "", "ref_nom": "", "input_history_tables": ""}, f, indent=2)
    else:
        if os.path.getsize(config_path) == 0:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump({"cia_corresp": "", "competencia": ""}, f, indent=2)

def parse_meses_opt(meses_str: str) -> Dict[int, str]:

    meses_dict = {}
    if not meses_str:
        return meses_dict
        
    pares = meses_str.split(',')
    for par in pares:
        try:
            num, nome = par.split(':')
            meses_dict[int(num)] = nome.strip()
        except ValueError:
            continue
    return meses_dict

def atualizar_config(chave: str, valor: Any) -> bool:

    garantir_config_json()
    config_path = os.path.join(os.getcwd(), "config.json")
    try:
        with open(config_path, "r+", encoding="utf-8") as f:
            try:
                config = json.load(f)
            except json.JSONDecodeError:
                config = {"cia_corresp": "", "competencia": ""}
            
            config[chave] = valor
            f.seek(0)
            json.dump(config, f, indent=2, ensure_ascii=False)
            f.truncate()
        print(f"✅ '{chave}' atualizado para: {valor}")
        return True
    except Exception as e:
        print(f"❌ Erro ao atualizar config.json: {e}")
        return False

def escolher_cia_e_atualizar_config() -> str:
    cias_opt = os.getenv("CIAS_OPT", "")
    cias_lista = [cia.strip() for cia in cias_opt.split(",") if cia.strip()]

    if not cias_lista:
        print("🚨 Nenhuma opção de CIA encontrada em CIAS_OPT no .env")
        return ""

    coluna_referencia_map = {
        "Bradesco": "Razão Social Corretor",
        "Bradesco Saude": "Razão Social Corretor",
        "Suhai": "Nome Corretor", 
        "Allianz": "CORRETOR",
        "Junto Seguradora": "Nome Da Conta",
        "Hdi": "descrição corretor coligado",
        "Porto": "nome_corretor",
        "Yelum": "nome",
        "Axa": "nome_corretor",
        "Zurich": "NOME CORRETOR",
        "Chubb": "Corretor",
        "Tokio":"Nome do Corretor",
        "Ezze": "nm_corretor",
        "Sompo": 'corretor',
        "Mapfre": 'Prêmios Emitidos como valores'
    }

    print("\n📋 Escolha uma das CIAs:")
    for i, cia in enumerate(cias_lista, 1):
        print(f"{i}. {cia}")

    while True:
        try:
            escolha = int(input("\nDigite o número correspondente à CIA: "))
            if 1 <= escolha <= len(cias_lista):
                cia_escolhida = cias_lista[escolha - 1]

                if cia_escolhida not in coluna_referencia_map:
                    print(f"❌ CIA '{cia_escolhida}' não possui mapeamento de coluna de referência")
                    print("ℹ️ CIAs mapeadas: " + ", ".join(coluna_referencia_map.keys()))
                    return ""

                if not atualizar_config("cia_corresp", cia_escolhida):
                    print("❌ Falha ao atualizar a configuração.")
                    return ""

                return cia_escolhida
            else:
                print("❌ Número fora do intervalo. Tente novamente.")
        except ValueError:
            print("❌ Entrada inválida. Digite apenas um número.")


def obter_mes_ano(competencia: str) -> Tuple[int, int]:
    """Recebe uma string no formato 'MM-AAAA' e retorna uma tupla (mes, ano)."""
    try:
        mes_str, ano_str = competencia.split('-')
        return int(mes_str), int(ano_str)
    except ValueError:
        raise ValueError(f"Formato inválido para competência: {competencia}. Esperado 'MM-AAAA'.")


def processar_automaticamente(cia: str, competencia: str):
    coluna_referencia_map = {
        "Bradesco": "Razão Social Corretor",
        "Bradesco Saude": "Razão Social Corretor",
        "Suhai": "Nome Corretor", 
        "Allianz": "CORRETOR",
        "Junto Seguradora": "Nome Da Conta",
        "Hdi": "descrição corretor coligado",
        "Porto": "nome_corretor",
        "Yelum": "nome",
        "Axa": "nome_corretor",
        "Zurich": "NOME CORRETOR",
        "Chubb": "Corretor",
        "Tokio": "Nome do Corretor",
        "Ezze": "nm_corretor",
        "Sompo": 'corretor',
        "Mapfre": 'Prêmios Emitidos como valores',
        "Swiss": 'Nome Corretor',
    }


    input_history_tables_map = {
        "Bradesco": "cont_prod_bradesco",
        "Bradesco Saude": "cont_prod_bradesco_saude",
        "Suhai": "cont_prod_suhai",
        "Allianz": "cont_prod_allianz",
        "Junto Seguradora": "cont_prod_junto",
        "Hdi": "cont_prod_hdi",
        "Porto": "cont_prod_porto",
        "Yelum": "cont_prod_yelum",
        "Axa": "cont_prod_axa",
        "Zurich": "cont_prod_zurich",
        "Chubb": "cont_prod_chubb",
        "Tokio": "cont_prod_tokio",
        "Ezze": "cont_prod_ezze",
        "Sompo": "cont_prod_sompo",
        "Mapfre": "cont_prod_mapfre",
        "Swiss": "cont_prod_swiss"
    }


    if cia not in coluna_referencia_map:
        print(f"❌ CIA '{cia}' não está mapeada em coluna_referencia_map")
        return

    atualizar_config("cia_corresp", cia)
    atualizar_config("competencia", competencia)
    atualizar_config("ref_nom", coluna_referencia_map[cia])

    if cia in input_history_tables_map:
        atualizar_config("input_history_tables", input_history_tables_map[cia])
    else:
        print(f"⚠️ Sem mapeamento de 'input_history_tables' para {cia}")



if __name__ == "__main__":
    if len(sys.argv) == 3:
        cia_arg = sys.argv[1]
        competencia_arg = sys.argv[2]
        processar_automaticamente(cia_arg, competencia_arg)
    else:
        print("🔁 Iniciando modo interativo...")
        cia = escolher_cia_e_atualizar_config()
        if cia:
            obter_mes_ano()
