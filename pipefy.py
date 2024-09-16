import requests
import csv
import time

# Define sua chave de API e o ID do pipe
api_token = "eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJQaXBlZnkiLCJpYXQiOjE3MjYzNDE2NjcsImp0aSI6IjliODRiYzYxLTQzYjctNDc0Zi1hNWE3LTgyYWZiMTk5YzVkMiIsInN1YiI6MzAxMDM5ODU4LCJ1c2VyIjp7ImlkIjozMDEwMzk4NTgsImVtYWlsIjoiZGlyZXRvcmlhLnZlbmRhc0BmY2FwanIuY29tLmJyIn19.9BcwY0LR-cfJ3uwjOUZzAHr5CEUfJHIzjbgriP7DTDL81Ycv2fEUshZjnSy_0LXtZLNbJ28U2CvErhocOM_ptQ"
pipe_id = "303931219"

# Cabeçalhos da requisição
headers = {
    "Authorization": f"Bearer {api_token}"
}

# Query GraphQL para paginação
def make_query(pipe_id, after_cursor=None):
    pagination_clause = f', after: "{after_cursor}"' if after_cursor else ''
    query = """
    {
      allCards(pipeId: %s, first: 100%s) {
        pageInfo {
          endCursor
          hasNextPage
        }
        edges {
          node {
            id
            title
            fields {
              name
              value
            }
            current_phase {
              name
            }
          }
        }
      }
    }
    """ % (pipe_id, pagination_clause)
    return query

# Função para buscar dados do Pipefy com paginação
def fetch_pipefy_data():
    has_next_page = True
    after_cursor = None
    all_cards = []

    while has_next_page:
        query = make_query(pipe_id, after_cursor)
        response = requests.post(
            'https://api.pipefy.com/graphql',
            json={'query': query},
            headers=headers
        )
        if response.status_code == 200:
            data = response.json()
            all_cards.extend(data['data']['allCards']['edges'])
            page_info = data['data']['allCards']['pageInfo']
            has_next_page = page_info['hasNextPage']
            after_cursor = page_info['endCursor']
        else:
            raise Exception(f"Erro na requisição: {response.status_code}, Detalhes: {response.text}")
    
    return all_cards

# Processa os dados e salva no CSV
def process_and_save_data(cards):
    # Cria o arquivo CSV e adiciona cabeçalhos
    with open('dados_pipefy.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Título", "Nome do Cliente", "Nome do SR", "Data de Início", "Nome da Empresa", "Setor da Empresa", "Contato do Cliente", "Fase Atual"])

        for card in cards:
            card_info = {}
            card_info["Título"] = card['node']['title']
            card_info["Fase Atual"] = card['node']['current_phase']['name'] if card['node']['current_phase'] else "N/A"
            
            # Extrair campos específicos
            for field in card['node']['fields']:
                if field['name'] == "Nome do cliente":
                    card_info["Nome do Cliente"] = field['value']
                elif field['name'] == "Nome do SR":
                    card_info["Nome do SR"] = field['value']
                elif field['name'] == "Data de inicio":
                    card_info["Data de Início"] = field['value']
                elif field['name'] == "Nome da Empresa":
                    card_info["Nome da Empresa"] = field['value']
                elif field['name'] == "Setor da Empresa":
                    card_info["Setor da Empresa"] = field['value']
                elif field['name'] == "Contato do cliente":
                    card_info["Contato do Cliente"] = field['value']

            # Adiciona os valores ao CSV
            writer.writerow([
                card_info.get("Título", "N/A"),
                card_info.get("Nome do Cliente", "N/A"),
                card_info.get("Nome do SR", "N/A"),
                card_info.get("Data de Início", "N/A"),
                card_info.get("Nome da Empresa", "N/A"),
                card_info.get("Setor da Empresa", "N/A"),
                card_info.get("Contato do Cliente", "N/A"),
                card_info.get("Fase Atual", "N/A")
            ])            

# Função para buscar dados e atualizar CSV
def fetch_and_update_data():
    # Busca os dados do Pipefy
    cards = fetch_pipefy_data()
    # Processa e salva os dados no CSV
    process_and_save_data(cards)

# Loop para rodar a cada 20 minutos
while True:
    print("Iniciando atualização de dados...")
    try:
        fetch_and_update_data()
        print("Atualização concluída")
    except Exception as e:
        print(f"Erro: {e}")
    
    # Aguarda 20 minutos (1200 segundos) antes de rodar de novo
    time.sleep(1200)
