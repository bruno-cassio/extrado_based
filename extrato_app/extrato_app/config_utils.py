import json
from pathlib import Path

def update_config_json(config_data: dict):
    config_path = Path(__file__).parent.parent / 'ConstrucaoTabelas' / 'configs.json'
    config_path.parent.mkdir(parents=True, exist_ok=True)

    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, ensure_ascii=False, indent=2)

    print(f"Arquivo configs.json atualizado com: {config_data}")
