import os
import pandas as pd
import re
import getpass

class EzzeHandler:

    def padronizar_nomes(nome: str) -> str:
        nome = nome.strip().lower().replace(" ", "_")
        nome = re.sub(r'[áàâãä]', 'a', nome)
        nome = re.sub(r'[éèêë]', 'e', nome)
        nome = re.sub(r'[íìîï]', 'i', nome)
        nome = re.sub(r'[óòôõö]', 'o', nome)
        nome = re.sub(r'[úùûü]', 'u', nome)
        nome = re.sub(r'[ç]', 'c', nome)
        nome = re.sub(r'_/_', '_', nome)
        nome = re.sub(r'ramo_inteno', 'ramo_interno', nome)
        nome = re.sub(r'nº_form._venda', 'numero_form_venda', nome)
        nome = re.sub(r'cpf\\cnpj_do_segurado', 'cpf_cnpj_do_segurado', nome)
        nome = re.sub(r'r\$_premio_liquido', 'valor_premio_liquido', nome)
        nome = re.sub(r'%', 'pct', nome)
        nome = re.sub(r'r\$_comissao', 'valor_comissao', nome)
        nome = re.sub(r'novo?', 'novo', nome)
        nome = re.sub(r'_-_', '_', nome)
        return nome

    def treat(self, folder_path: str) -> pd.DataFrame:
        print(f"📂 Verificando arquivos em: {folder_path}")

        files = [
            f for f in os.listdir(folder_path)
            
            if f.lower().endswith(('.xls', '.xlsx', '.xlsb'))
        ]
        print('check abaixo')
        print(files)
        if not files:
            print("❌ Nenhum arquivo encontrado contendo 'apuração'")
            return pd.DataFrame()

        print(f"📂 Arquivos encontrados: {files}")
        file = max(files, key=lambda f: os.path.getmtime(os.path.join(folder_path, f)))
        file_path = os.path.join(folder_path, file)
        print(f"📂 Arquivo selecionado: {file}")

        abas = [
            'GC - Demais Ramos',
            'GC - Frota e Transporte ',
            'GC - Frota e Transportes',
            'GC - Auto Individual'
        ]
        dfs = []
        for aba in abas:
            try:
                temp_df = pd.read_excel(file_path, sheet_name=aba, header=None)
                header_row = temp_df[temp_df.iloc[:, 0].astype(str).str.lower() == 'cd_apolice'].index
                if not header_row.empty:
                    header_idx = header_row[0]
                    df_aba = pd.read_excel(file_path, sheet_name=aba, header=header_idx)
                    df_aba['aba_origem'] = aba
                    dfs.append(df_aba)
                else:
                    print(f"⚠️ Cabeçalho 'cd_apolice' não encontrado na aba '{aba}'")
            except Exception as e:
                print(f"❌ Erro ao ler a aba '{aba}': {e}")
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            df['origem_arquivo'] = file
            print(df)
            print(f"✅ DataFrame carregado ({df.shape[0]} linhas) das abas {abas}")
            
            print('========================================================= la vai =========================================================')
            df.columns = [EzzeHandler.padronizar_nomes(str(col)) for col in df.columns]
            print('sanitized')
            print(df.columns.tolist)
            return df
        else:
            print("❌ Nenhuma aba válida encontrada ou erro ao processar abas.")
            return pd.DataFrame()

    def process(self, df: pd.DataFrame, file_name: str, premio_exec: str, fator_melchiori: float, premio_db=None):
        
        print('================================================ started processamento EZZE ================================================')
        fator_melchiori = 0.965
        print(f'Fator Melchiori:{fator_melchiori}')
        print(f'premio exec: {premio_exec}')
        coluna = premio_exec if premio_exec in df.columns else 'premio_total'
        if coluna not in df.columns:
            print(f"⚠️ Coluna '{coluna}' não encontrada no arquivo {file_name}.")
            return

        df = df.copy()

        df['premio_rec'] = 0.0
        df['valor_cv'] = 0.0
        df['valor_vi'] = 0.0
        df['valor_as'] = 0.0

        mask_drs = df['aba_origem'].str.contains("Demais Ramos", case=False, na=False)
        df.loc[mask_drs, 'premio_rec'] = df.loc[mask_drs, coluna] * fator_melchiori
        df.loc[mask_drs, 'valor_cv'] = df.loc[mask_drs, 'premio_rec'] * 0.03
        df.loc[mask_drs, 'valor_as'] = df.loc[mask_drs, 'premio_rec'] * 0.02

        #aprimorar aqui a leitura destes dados, criar normatizaçao a partir da remoção de plurais por singular, afim de superar alterações / erros na emissão do relatório por parte da cia.

        mask_frota = df['aba_origem'].str.contains("Frota e Transportes", case=False, na=False)
        if 'valor_comissao_corretor' in df.columns and 'valor_comissao_gc' in df.columns:
            df.loc[mask_frota, 'valor_as'] = (df.loc[mask_frota, 'valor_comissao_corretor'] + df.loc[mask_frota, 'valor_comissao_gc']) * fator_melchiori

        mask_auto = df['aba_origem'].str.contains("Auto Individual", case=False, na=False)
        df.loc[mask_auto, 'premio_rec'] = df.loc[mask_auto, coluna] * fator_melchiori
        df.loc[mask_auto, 'valor_cv'] = df.loc[mask_auto, 'premio_rec'] * 0.04
        df.loc[mask_auto, 'valor_vi'] = df.loc[mask_auto, 'premio_rec'] * 0.01

        user = getpass.getuser()
        downloads_path = os.path.join("C:\\Users", user, "Downloads")
        output_file = os.path.join(downloads_path, f"resultado_{file_name}.xlsx")
        df.to_excel(output_file, index=False)

        print(df[[coluna, 'aba_origem', 'premio_rec', 'valor_cv', 'valor_vi', 'valor_as']].head())
        print(f"✅ DataFrame salvo em: {output_file}")
