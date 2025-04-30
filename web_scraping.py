import requests
from bs4 import BeautifulSoup
import logging
import csv
import time

logging.basicConfig(
    filename='scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'  
)

# para medição de tempo das etapas
global_start = time.time()
stage_start = time.time()
etapas = {}

def log_stage_time(stage_name):
    global stage_start, etapas
    elapsed = time.time() - stage_start
    etapas[stage_name] = elapsed  # para armazenar o tempo
    logging.info(f"ETAPA '{stage_name}' concluida em {elapsed:.2f} segundos")
    stage_start = time.time()

logging.info("Iniciando o script de scraping...")

try:
    url = "https://books.toscrape.com/"
    logging.info(f"Acessando a pagina: {url}")
    response = requests.get(url)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    logging.error(f"Falha ao acessar {url}: {e}")
    exit()
else:
    logging.info("Pagina acessada com sucesso!")
    log_stage_time("DOWNLOAD")

soup = BeautifulSoup(response.text, 'html.parser')
log_stage_time("PARSING")

books = soup.find_all('article', class_='product_pod')

if not books:
    logging.warning("Nenhum livro encontrado na pagina!")
else:
    logging.info(f"{len(books)} livros encontrados na pagina.")


for book in books:
    try:
        title = book.h3.a['title']
        price = book.find('p', class_='price_color').text
        link = url + book.h3.a['href']
        rating = book.p['class'][1]
        
        logging.info(f"Livro coletado: {title} | Preco: {price} | Avaliacao: {rating}")
        
    except Exception as e:
        logging.error(f"Erro ao extrair livro: {e}")

log_stage_time("EXTRACAO")

try:
    with open('livros.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Título', 'Preço', 'Link', 'Avaliação'])
        
        for book in books:
            title = book.h3.a['title']
            price = book.find('p', class_='price_color').text
            link = url + book.h3.a['href']
            rating = book.p['class'][1]
            
            writer.writerow([title, price, link, rating])
    
    logging.info("Dados salvos em 'livros.csv'!")

except Exception as e:
    logging.error(f"Falha ao salvar CSV: {e}")

log_stage_time("SALVAMENTO CSV")


total_time = time.time() - global_start
print(f"\nScraping concluído! Verifique os arquivos:")
print(f"- Log completo: scraping.log")
print(f"- Dados extraídos: livros.csv")
logging.info(f"\nRELATORIO DETALHADO:\n"
             f"TOTAL: {total_time:.2f} segundos\n"
             f"DOWNLOAD: {etapas.get('DOWNLOAD', 0):.2f}s\n"
             f"PARSING: {etapas.get('PARSING', 0):.2f}s\n"
             f"EXTRACAO: {etapas.get('EXTRACAO', 0):.2f}s\n"
             f"SALVAMENTO: {etapas.get('SALVAMENTO CSV', 0):.2f}s\n"
             f"Concluido!")