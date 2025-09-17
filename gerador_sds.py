import pandas as pd
import os
import numpy as np
from time import sleep
import unicodedata
import csv

SAIDA = 'sds_arquivos_final'

ARQ_ADMIN = 'Sync_Administrativo_01.csv'
ARQ_ALUNO = 'Sync_Aluno_01.csv'
ARQ_AULA = 'Sync_Aula_01.csv'

ORG_ID_1 = 'unidade1'
ORG_NAME_1 = 'Organização'
ORG_ID_2 = 'unidade2'
ORG_NAME_2  = 'Organização 2'

DOMINIO_PROF = 'dominio'
DOMINIO_ALUNO = 'dominio2'

def detectar_separador(arquivo):
    with open(arquivo, 'r', encoding='latin-1') as f:
        sniffer = csv.Sniffer()
        return sniffer.sniff(f.readline()).delimiter

def carregar_csv_robusto(arquivo, separador):
    try:
        return pd.read_csv(arquivo, sep=separador, on_bad_lines='skip', dtype='str', encoding='utf-8')
    except UnicodeDecodeError:
        print(f"AVISO: Arquivo '{arquivo}' não está em UTF-8. Tentando com latin-1.")
        return pd.read_csv(arquivo, sep=separador, on_bad_lines='skip', dtype='str', encoding='latin-1')
    
def normalizar_texto(texto):
    if not isinstance(texto, str): return ""
    texto = texto.lower().replace('ª', 'a').replace('º', 'o')
    texto_sem_acento = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto_alfanumerico = ''.join(c for c in texto_sem_acento if c.isalnum() or c.isspace())
    texto_final = '-'.join(texto_alfanumerico.split())
    return texto_final

def preencher_email(nome_completo, dominio):
    if not isinstance(nome_completo, str): return None
    partes_nome = nome_completo.strip().split()
    if not partes_nome: return None
    primeiro_nome = normalizar_texto(partes_nome[0])
    ultimo_nome = normalizar_texto(partes_nome[-1]) if len(partes_nome) > 1 else primeiro_nome
    return f"{primeiro_nome}.{ultimo_nome}@{dominio}"
    

print("Iniciando a geração de arquivos SDS (Versão de Produção)...")
if not os.path.exists(SAIDA): os.makedirs(SAIDA)

try:
    sep_admin = detectar_separador(ARQ_ADMIN)
    sep_aluno = detectar_separador(ARQ_ALUNO)
    sep_aula = detectar_separador(ARQ_AULA)
    
    df_admin = carregar_csv_robusto(ARQ_ADMIN, sep_admin)
    df_aluno = carregar_csv_robusto(ARQ_ALUNO, sep_aluno)
    df_aula = carregar_csv_robusto(ARQ_AULA, sep_aula)
    print("Arquivos do SIS carregados com sucesso.")
except Exception as e:
    print(f"ERRO CRÍTICO ao carregar os arquivos do SIS: {e}"); exit()


# Gerando users.csv
df_professores = df_admin[df_admin['NomeCargo'] == 'Professor(a)'].copy()
df_professores.dropna(subset=['CodigoFuncionario', 'NomeFuncionario'], inplace=True)

df_aluno.dropna(subset=['NumeroMatricula', 'NomeCompleto', 'EscolaID', 'NomeSerie', 'NomeTurma'], inplace=True)
df_aula.dropna(subset=['CodigoProfessor', 'EscolaID', 'NomeSerie', 'NomeTurma'], inplace=True)
print("Limpeza inicial dos dados de origem concluída.")

# 2. Geração de usernames (agora opera sobre dados já limpos)
df_professores['username'] = df_professores['NomeFuncionario'].apply(lambda x: preencher_email(x, DOMINIO_PROF))
df_aluno['username'] = df_aluno['NomeCompleto'].apply(lambda x: preencher_email(x, DOMINIO_ALUNO))
df_professores.dropna(subset=['username'], inplace=True)
df_aluno.dropna(subset=['username'], inplace=True)

# 3. Geração de IDs e tratamento de homônimos (agora opera sobre dados limpos)
df_professores['sourcedId'] = 'PROF_' + df_professores['CodigoFuncionario']
df_aluno['sourcedId'] = 'ALUNO_' + df_aluno['NumeroMatricula']

users_prof_prov = df_professores[['sourcedId', 'NomeFuncionario', 'username']].rename(columns={'NomeFuncionario': 'NomeCompleto'})
users_aluno_prov = df_aluno[['sourcedId', 'NomeCompleto', 'username']]
df_users_final = pd.concat([users_prof_prov, users_aluno_prov]).drop_duplicates(subset=['username'])
df_users_final[['givenName', 'familyName']] = df_users_final['NomeCompleto'].str.split(' ', n=1, expand=True)
print("Geração de usuários concluída.")


# Gerando users.csv
df_users_final[['sourcedId', 'username', 'givenName', 'familyName']].to_csv(os.path.join(SAIDA, 'users.csv'), index=False, encoding='utf-8-sig')
print(f"'users.csv' gerado -> {len(df_users_final)} registros.")

# Gerando orgs.csv 
pd.DataFrame([{'sourcedId': ORG_ID_1, 'name': ORG_NAME_1, 'type': 'school'}, {'sourcedId': ORG_ID_2, 'name': ORG_NAME_2, 'type': 'school'}]
            ).to_csv(os.path.join(SAIDA, 'orgs.csv'), index=False, encoding='utf-8-sig')
print("'orgs.csv' gerado.")

# Gerando roles.csv
map_org = {'1': ORG_ID_1, '2': ORG_ID_2}
roles_aluno = df_aluno[['sourcedId', 'EscolaID']].rename(columns={'sourcedId': 'userSourcedId'})
roles_aluno['orgSourcedId'] = roles_aluno['EscolaID'].map(map_org)
roles_aluno['role'] = 'student'
prof_escolas = df_aula[['CodigoProfessor', 'EscolaID']].drop_duplicates()
roles_prof = pd.merge(df_professores, prof_escolas, left_on='CodigoFuncionario', right_on='CodigoProfessor')
roles_prof = roles_prof[['sourcedId', 'EscolaID']].rename(columns={'sourcedId': 'userSourcedId'})
roles_prof['orgSourcedId'] = roles_prof['EscolaID'].map(map_org)
roles_prof['role'] = 'teacher'
df_roles_final = pd.concat([roles_aluno[['userSourcedId', 'orgSourcedId', 'role']], roles_prof[['userSourcedId', 'orgSourcedId', 'role']]])
df_roles_final.dropna(subset=['orgSourcedId'], inplace=True)
df_roles_final['sourcedId'] = 'ROLE_' + df_roles_final['userSourcedId'] + '_' + df_roles_final['orgSourcedId']
df_roles_final['isPrimary'] = 'true'
df_roles_final = df_roles_final[['sourcedId', 'userSourcedId', 'orgSourcedId', 'role', 'isPrimary']].drop_duplicates(subset=['userSourcedId', 'orgSourcedId'])
df_roles_final.to_csv(os.path.join(SAIDA, 'roles.csv'), index=False, encoding='utf-8-sig')
print(f"'roles.csv' gerado -> {len(df_roles_final)} registros.")

print(f"'roles.csv' gerado -> {len(df_roles_final)} registros.")

# 1. Pega as informações essenciais de turmas de ambos os arquivos de origem
turmas_info_alunos = df_aluno[['EscolaID', 'NomeSerie', 'NomeTurma', 'AnoLetivo', 'CodigoTurma']]
turmas_info_profs = df_aula[['EscolaID', 'NomeSerie', 'NomeTurma', 'CodigoTurma']]

# 2. Concatena as duas listas para criar uma lista mestra bruta
df_turmas_mestra = pd.concat([turmas_info_alunos, turmas_info_profs])

# 3. Remove quaisquer linhas sem as chaves essenciais e depois remove as duplicatas
df_turmas_mestra.dropna(subset=['EscolaID', 'CodigoTurma'], inplace=True)
df_turmas_mestra.drop_duplicates(subset=['EscolaID', 'CodigoTurma'], inplace=True)


ano_letivo_padrao = df_aluno['AnoLetivo'].mode()[0]
df_turmas_mestra['AnoLetivo'].fillna(ano_letivo_padrao, inplace=True)
df_turmas_mestra.reset_index(drop=True, inplace=True)

# 5. Com a lista mestra agora limpa e completa, gera o arquivo classes.csv
df_turmas_mestra['orgSourcedId'] = df_turmas_mestra['EscolaID'].map(map_org)
df_turmas_mestra['title'] = df_turmas_mestra['NomeTurma'] + ' - ' + df_turmas_mestra['NomeSerie'] + ' ' + df_turmas_mestra['AnoLetivo']
# O sourcedId da turma é simplesmente o CodigoTurma do SIS, como descobrimos.
df_turmas_mestra['sourcedId'] = df_turmas_mestra['CodigoTurma']

df_classes_final = df_turmas_mestra[['sourcedId', 'title', 'orgSourcedId']]
df_classes_final.to_csv(os.path.join(SAIDA, 'classes.csv'), index=False, encoding='utf-8-sig')
print(f"'classes.csv' gerado -> {len(df_classes_final)} registros.")

# Gerando enrollments.csv
map_turmas = df_turmas_mestra[['EscolaID', 'CodigoTurma', 'sourcedId']].rename(columns={'sourcedId': 'classSourcedId'})

# Matrículas de alunos
# A junção é feita pela chaves: EscolaID e CodigoTurma
enroll_alunos = pd.merge(df_aluno, map_turmas, on=['EscolaID', 'CodigoTurma'])
enroll_alunos = enroll_alunos[['sourcedId', 'classSourcedId']].rename(columns={'sourcedId': 'userSourcedId'}).assign(role='student')

# Matrículas de professores
map_profs = df_professores[['CodigoFuncionario', 'sourcedId']].rename(columns={'sourcedId': 'userSourcedId'})
enroll_profs = pd.merge(df_aula, map_profs, left_on='CodigoProfessor', right_on='CodigoFuncionario')
enroll_profs = pd.merge(enroll_profs, map_turmas, on=['EscolaID', 'CodigoTurma'])
enroll_profs = enroll_profs[['userSourcedId', 'classSourcedId']].assign(role='teacher')

# Junta tudo
df_enroll_final = pd.concat([enroll_alunos, enroll_profs]).drop_duplicates()
df_enroll_final.reset_index(drop=True, inplace=True)
df_enroll_final['sourcedId'] = 'ENROLL_' + df_enroll_final.index.astype(str)
df_enroll_final = df_enroll_final[['sourcedId', 'classSourcedId', 'userSourcedId', 'role']]
df_enroll_final.to_csv(os.path.join(SAIDA, 'enrollments.csv'), index=False, encoding='utf-8-sig')
print(f"'enrollments.csv' gerado -> {len(df_enroll_final)} registros.")

print(f"\nProcesso finalizado! Os 5 arquivos para o SDS estão na pasta '{SAIDA}'.")