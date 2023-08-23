# Importar bibliotecas
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup # Trabalha com o parsing do conteúdo HTML
from deep_translator import GoogleTranslator
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

###################################################### Funções #################################################################################

# Obtém a lista de todos os países do mundo
def get_all_countries_of_the_world():
    # Definir o URL para fazer a requisição
    url = "https://www.indexmundi.com/pt/"
    # Obtem o result da requisição
    response = requests.get(url)
    if response.status_code == 200:
        # Criar uma lista para armazenar os países
        list_countries = []
        # Instancia a biblioteca translator
        translator = GoogleTranslator(source='auto', target='en')
        # Fazer o parser do html content
        soup = BeautifulSoup(response.content, "html.parser")
        element_li = soup.find_all('li')
        # Loop para preencher a lista com todos os países
        for country in element_li:
            list_countries.append(translator.translate(country.get_text()))
    return list_countries

# Obter os dados dos paises
def get_country_data(countries):
    # Criar um DataFrame vazio para armazenar os dados
    final_df = pd.DataFrame(columns=['country_common_name', 'capital_name',
                                     'independent', 'currency_code', 'currency_name',
                                     'currency_symbol', 'language_name', 'population',
                                     'area', 'continents', 'flag'])
    # Loop pelos países
    for country in countries:
        url = f"https://restcountries.com/v3.1/name/{country.replace(' ','%20')}"
        response = requests.get(url)
        # Verifica o status code para dar continuidado ao código
        if response.status_code == 200:
            country_data = response.json()
            country_common_name = country_data[0]['name']['common']
            print(country_common_name)
            capital_name = ', '.join(country_data[0].get('capital', ['Capital Desconhecida']))
            if 'independent' in country_data[0]:
                if not country_data[0]['independent']:
                    independent = 'Não independente'
                else:
                    independent = 'Independente'
            else:
                independent = 'Informação não disponível'
            # Loop pelas moedas do país
            # declara variaveis que podem receber mais de um item
            if 'currencies' in country_data[0]:
                currency_codes, currency_names, currency_symbols = [], [], []
                for currency_code, currency_details in country_data[0]['currencies'].items():
                    currency_codes.append(currency_code)
                    currency_names.append(currency_details['name'])
                    currency_symbol = currency_details.get('symbol', 'None')
                    currency_symbols.append(currency_symbol)  # Adicione isso aqui
                currency_code_str = ', '.join(currency_codes)
                currency_name_str = ', '.join(currency_names)
                currency_symbol_str = ', '.join(currency_symbols)  # Adicione isso aqui
            else:
                currency_codes, currency_names, currency_symbols = 'N/A'
            # Faz um join para transformar em string
            if 'languages' in country_data[0]:
                language_name = ', '.join(country_data[0]['languages'].values())
            else:
                language_name = 'N/A'
            population = country_data[0]['population']
            area = country_data[0]['area']
            continents = ', '.join(country_data[0]['continents'])
            flag = country_data[0]['flags']['png']
            # Adiciona todos os dados ao dataframe
            final_df.loc[len(final_df)] = [country_common_name, capital_name,
                                           independent, currency_code_str, currency_name_str,
                                           currency_symbol_str, language_name, population,
                                           area, continents, flag]
        # Intervalo de espera de 10 segundos para a próxima consulta
        # Obs: Evita sobrecarregar o servidor, a API.
        time.sleep(5)
    return final_df

# Subir os dados para o BQ
def load_data_in_bq(df):
    # Cria um cliente do BigQuery
    client = bigquery.Client()

    # Definir o ID do projeto e o ID do conjunto de dados criado no BigQuery
    dataset_id = 'countries_infomation'

    # Nome da tabela no BigQuery (sem o caminho completo)
    table_name = 'countries_infomation_data'

    # Cria um schema para a tabela do big query de forma dinâmica
    schema = []

    for column_name, data_type in zip(df.columns, df.dtypes):
        if "int" in str(data_type):
            bq_data_type = bigquery.SchemaField(column_name, "INTEGER")
        elif "float" in str(data_type):
            bq_data_type = bigquery.SchemaField(column_name, "FLOAT")
        elif "datetime" in str(data_type):
            bq_data_type = bigquery.SchemaField(column_name, "TIMESTAMP")
        elif "bool" in str(data_type):
            bq_data_type = bigquery.SchemaField(column_name, "BOOL")
        else:
            bq_data_type = bigquery.SchemaField(column_name, "STRING")
        schema.append(bq_data_type)
            
    print(schema)

    # Verifica se o conjunto de dados já existe
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"O conjunto de dados {dataset_id} já existe.")
    except NotFound:
        # Cria o conjunto de dados no BigQuery
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f"O conjunto de dados {dataset_id} foi criado com sucesso.")

    # Cria uma tabela no BigQuery (se ainda não existir)
    table_ref = dataset_ref.table(table_name)

    # Verifica se a tabela existe, se existir ele deleta para garantir que estamos subindo os dados mais atualizados
    try:
        table_ref = dataset_ref.table(table_name)
        table = client.get_table(table_ref)
        print(f"A tabela {table_name} já existe.")
        print(f"Iremos deletar a tabela para subir os dados")
        client.delete_table(table_ref)
        print(f"Tabela deletada com sucesso!")
    except NotFound:
        print(f"A tabela {table_name} não existe. Vamos criar a tabela.")
    
    # Cria a tabela no BigQuery
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f"A tabela {table_name} foi criada com sucesso.")
    
    # Carrega os dados no BigQuery
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()
    print(f"Os dados foram carregados com sucesso na tabela {table_name} no BigQuery.")


###################################################### DATA QUALITY #################################################################################

# Verifica a qualidade dos dados em questão de colunas e dados vazios
def analyze_null_columns(df):
    
    df = df
    column_names = df.columns.tolist()
    
    empty_columns = {}
    
    for column in column_names:
        empty_cells = df[column].loc[df[column].isnull()].tolist()
        if empty_cells:
            empty_columns[column] = empty_cells
            
    total_columns = len(column_names)
    empty_column_names = list(empty_columns.keys())
    empty_column_percentage = (len(empty_column_names) / total_columns) * 100
    
    empty_rows = {}
        
    for col in empty_column_names:
        empty_rows[col] = []
        for index, value in enumerate(df[col]):
            if pd.isna(value):
                empty_rows[col].append(index)
                
    count_rows_null = {}
    count_rows_not_null = {}
    
    for col in empty_column_names:
        count_rows_null[col] = df[col].isna().sum()
        count_rows_not_null[col] = len(df) - count_rows_null[col]
    
    return {
        "total_columns": total_columns,
        "empty_column_count": len(empty_column_names),
        "empty_column_names": empty_column_names,
        "empty_column_percentage": empty_column_percentage,
        "count_rows_null": count_rows_null,
        "count_rows_not_null": count_rows_not_null,
        "rows_in_empty_columns": empty_rows
    }

# Verificar se tem dados duplicados em uma coluna especifica
def get_data_duplicated(result_df, primary_key):
    seen = {}
    duplicates = {}
    unique_values = {}
    for index, value in enumerate(result_df[primary_key]):
        if value in seen:
            if value in duplicates:
                duplicates[value].append(seen[value])
                duplicates[value].append(index)
            else:
                duplicates[value] = [seen[value], index]
        else:
            seen[value] = index
    return {
        "Duplicated_Values": duplicates,
    }


###################################################### MAIN CODE #################################################################################

# Executa a função para obter a lista de todos os países do mundo
countries = get_all_countries_of_the_world()

# Executa a função passando como parâmetro a lista dos paises para obter todos os dados dos respectivos países que estão na lista
result_df = get_country_data(countries)

# Executa a verificação na qualidade dos daods
print(analyze_null_columns(result_df))

# Verifica se tem dados duplicados em uma coluna
print(get_data_duplicated(result_df, 'country_common_name'))


###################################################### TRATAMENTO DOS DADOS #################################################################################

# Faz um tratamento para dados duplicados removendo todas as linhas que estão duplicadas
df = result_df.drop_duplicates()

################################################## Salvar os dados no BigQuery ##############################################################################

# Subir os dados para o bigquery
load_data_in_bq(df)