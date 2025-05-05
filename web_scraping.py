import requests
from bs4 import BeautifulSoup
import logging
import csv
import time
from collections import defaultdict
import socket

logging.basicConfig(
    filename='scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S'
)

def log_execution_time(stages):
    """Gera o relatório de tempo de execução no formato desejado."""
    total_time = sum(stages.values())
    report = (
        f"\nRELATORIO DE TEMPO:\n"
        f"TOTAL: {total_time:.2f} segundos\n"
        f"DOWNLOAD: {stages.get('DOWNLOAD', 0):.2f}s\n"
        f"PARSING: {stages.get('PARSING', 0):.2f}s\n"
        f"EXTRACAO: {stages.get('EXTRACAO', 0):.2f}s\n"
        f"SALVAMENTO: {stages.get('SALVAMENTO', 0):.2f}s\n"
        f"Concluido!\n"
    )
    return report

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
REQUEST_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 2
MAX_RETRIES = 3
SKIP_FAILED_CATEGORIES = True
MAX_PAGES_PER_CATEGORY = 5


socket.setdefaulttimeout(REQUEST_TIMEOUT)

def get_categories(url_home):
    """Coleta os nomes e URLs de todas as categorias."""
    try:
        response = requests.get(url_home, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        menu_categorias = soup.find('ul', class_='nav nav-list')
        if not menu_categorias:
            raise ValueError("Menu de categorias nao encontrado!")
            
        links_categorias = menu_categorias.find_all('a')
        categories = []
        
        for link in links_categorias[1:]:  #ignora o primeiro link ("Books")
            nome = link.text.strip()
            url_relativa = link['href']
            
            base_url = url_home.rsplit('/', 1)[0] 
            url_absoluta = base_url + '/' + url_relativa
            
            categories.append((nome, url_absoluta))
            time.sleep(DELAY_BETWEEN_REQUESTS/2)
        
        return categories
    
    except Exception as e:
        logging.error(f"Erro ao coletar categorias: {str(e)}")
        return []

def generate_category_report(all_books):
    """Gera um relatório com a contagem de livros por categoria."""
    category_counts = defaultdict(int)
    for book in all_books:
        category_counts[book['Categoria']] += 1
    
    sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
    
    report = "\n=== RELATORIO POR CATEGORIA ===\n"
    for category, count in sorted_categories:
        report += f"- {category.ljust(25)}: {count} livros\n"
    report += "==============================="
    
    return report

def scrape_books_from_category(category_name, category_url):
    """Coleta todos os livros de uma categoria (incluindo paginacao)."""
    books = []
    page_url = category_url
    page_count = 1
    retry_count = 0
    
    while page_url and page_count <= MAX_PAGES_PER_CATEGORY and retry_count < MAX_RETRIES:
        try:
            logging.info(f"Acessando pagina {page_count} da categoria '{category_name}'")
            response = requests.get(page_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            
           
            if response.status_code == 404:
                logging.warning(f"Categoria '{category_name}' nao encontrada (404)")
                if SKIP_FAILED_CATEGORIES:
                    return []
                else:
                    raise requests.exceptions.HTTPError(f"404 - Pagina nao encontrada: {page_url}")
            
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            page_books = soup.find_all('article', class_='product_pod')
            
            if not page_books:
                logging.warning(f"Nenhum livro encontrado na pagina {page_count} de '{category_name}'")
                break
            
            for book in page_books:
                try:
                    title = book.h3.a['title']
                    price = book.find('p', class_='price_color').text
                    link = book.h3.a['href']
                    
                
                    if link.startswith('../../../'):
                        link = "https://books.toscrape.com/catalogue/" + link[9:]
                    elif link.startswith('../'):
                        link = "https://books.toscrape.com/catalogue/" + link[3:]
                    elif not link.startswith('http'):
                        link = "https://books.toscrape.com/catalogue/" + link
                    
                    rating = book.p['class'][1] if book.p and 'class' in book.p.attrs else 'N/A'
                    
                    books.append({
                        'Categoria': category_name,
                        'Título': title,
                        'Preço': price,
                        'Link': link,
                        'Avaliação': rating
                    })
                except Exception as e:
                    logging.error(f"Erro ao processar livro: {str(e)}")
                    continue
            
            
            next_button = soup.find('li', class_='next')
            if next_button:
                next_page = next_button.a['href']
                if page_url.endswith('/'):
                    page_url = page_url + next_page
                else:
                    page_url = page_url.rsplit('/', 1)[0] + '/' + next_page
                page_count += 1
                retry_count = 0
                time.sleep(DELAY_BETWEEN_REQUESTS)
            else:
                page_url = None
                
        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count >= MAX_RETRIES:
                logging.error(f"Falha apos {MAX_RETRIES} tentativas na categoria '{category_name}'")
                if SKIP_FAILED_CATEGORIES:
                    return []
                else:
                    raise
            logging.warning(f"Tentativa {retry_count}/{MAX_RETRIES} - Erro: {str(e)}")
            time.sleep(DELAY_BETWEEN_REQUESTS * 2)
            continue
        except Exception as e:
            logging.error(f"Erro inesperado: {str(e)}")
            if SKIP_FAILED_CATEGORIES:
                return []
            else:
                raise
    
    logging.info(f"Categoria '{category_name}': {len(books)} livros coletados")
    return books

def save_to_csv(books, filename='livros.csv'):
    """Salva os livros em um arquivo CSV."""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['Categoria', 'Título', 'Preço', 'Link', 'Avaliação'])
            writer.writeheader()
            writer.writerows(books)
        logging.info(f"Dados salvos em '{filename}'")
        return True
    except Exception as e:
        logging.error(f"Falha ao salvar CSV: {str(e)}")
        return False

def main():
    try:
        # Dicionário para armazenar os tempos
        etapas_tempo = {}
        global_start = time.time()
        
        # ETAPA 1: DOWNLOAD
        stage_start = time.time()
        logging.info("Iniciando processo de scraping...")
        categorias = get_categories("https://books.toscrape.com/catalogue/category/books_1/index.html")
        if not categorias:
            raise ValueError("Nenhuma categoria encontrada!")
        etapas_tempo['DOWNLOAD'] = time.time() - stage_start
        
        # ETAPA 2: PARSING/COLETA
        stage_start = time.time()
        logging.info(f"Encontradas {len(categorias)} categorias para processar")
        all_books = []
        processed_categories = 0
        
        for nome, url in categorias:
            try:
                logging.info(f"Processando categoria ({processed_categories+1}/{len(categorias)}): {nome}")
                stage_cat_start = time.time()
                category_books = scrape_books_from_category(nome, url)
                
                if category_books:
                    all_books.extend(category_books)
                    processed_categories += 1
                    logging.info(f"Categoria '{nome}' concluida: {len(category_books)} livros")
                else:
                    logging.warning(f"Categoria '{nome}' nao retornou livros")
                
                time.sleep(DELAY_BETWEEN_REQUESTS)
                
                if processed_categories % 5 == 0:
                    logging.info(f"Progresso: {processed_categories}/{len(categorias)} categorias processadas")
            
            except Exception as e:
                logging.error(f"Erro fatal na categoria {nome}: {str(e)}")
                if not SKIP_FAILED_CATEGORIES:
                    raise
                continue
        
        etapas_tempo['EXTRACAO'] = time.time() - stage_start
        
        if not all_books:
            logging.warning("Nenhum livro foi coletado em nenhuma categoria!")
            if not SKIP_FAILED_CATEGORIES:
                raise ValueError("Nenhum livro foi coletado!")
            return
        
        # ETAPA 3: SALVAMENTO
        stage_start = time.time()
        if save_to_csv(all_books):
            report = generate_category_report(all_books)
            print(report)
            logging.info(report)
            
            with open('relatorio_categorias.txt', 'w', encoding='utf-8') as f:
                f.write(report)
            
            etapas_tempo['SALVAMENTO'] = time.time() - stage_start
            
            # Gera e exibe o relatório de tempo
            time_report = log_execution_time(etapas_tempo)
            print(time_report)
            logging.info(time_report)
            
            logging.info(f"Processo concluido! Total de {len(all_books)} livros coletados em {time.time() - global_start:.2f} segundos")
        else:
            raise Exception("Falha ao salvar os dados em CSV")
    
    except Exception as e:
        logging.error(f"Erro no processo principal: {str(e)}")
        raise
    finally:
        logging.info("Script finalizado com sucesso!")


if __name__ == "__main__":
    main()