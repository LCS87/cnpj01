import re
import pandas as pd

def organizarCNPJeTELEFONES(arquivoXLSX):
    def corrigir_telefone(telefone):
        telefone = re.sub(r'\D', '', str(telefone))
        if len(telefone) == 11 and telefone[2] == '0':
            telefone = telefone[:2] + '9' + telefone[3:]
        elif len(telefone) == 10:
            telefone = telefone[:2] + '9' + telefone[2:]
        elif len(telefone) != 11:
            return None
        return telefone

    try:
        df = pd.read_excel(arquivoXLSX, engine='openpyxl')

        if 'TELEFONES' in df.columns:
            df['TELEFONE_CORRIGIDO'] = df['TELEFONES'].apply(corrigir_telefone)

            df_validos = df[df['TELEFONE_CORRIGIDO'].notna()]
            df_invalidos = df[df['TELEFONE_CORRIGIDO'].isna()]
            df_final = pd.concat([df_validos, df_invalidos], ignore_index=True)

           
            with pd.ExcelWriter(arquivoXLSX, engine='openpyxl', mode='w') as writer:
                df_final.to_excel(writer, index=False)
            print("✅ Arquivo processado e salvo com sucesso.")
        else:
            print("🚫 A coluna 'TELEFONES' não existe no arquivo.")

    except Exception as e:
        print(f"❌ Erro ao processar o arquivo: {e}")


organizarCNPJeTELEFONES(r"D:\PegarOperadora\MeiME 202507.xlsx")
