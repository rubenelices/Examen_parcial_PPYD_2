"""
Sistema Distribuido de Análisis de Noticias en Tiempo Real
---------------------------------------------------------
Este script implementa un nodo de adquisición para un sistema distribuido
que obtiene artículos de múltiples fuentes de noticias de forma asíncrona,
los estandariza y los envía a un servidor central.
"""

import asyncio
import aiohttp
import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import feedparser  # Para procesar RSS
import traceback
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Clase para modelar un artículo estandarizado
class Article:
    def __init__(self, id: str, title: str, content: str, published_date: datetime,
                 source_name: str, source_url: str, metadata: Dict[str, Any] = None):
        self.id = id
        self.title = title
        self.content = content
        self.published_date = published_date
        self.source_name = source_name
        self.source_url = source_url
        self.metadata = metadata or {}
    
    def to_dict(self):
        """Convierte el artículo a un diccionario serializable en JSON."""
        return {
            "id": self.id,
            "title": self.title,
            "content": self.content,
            "published_date": self.published_date.isoformat(),  # Convertir a formato ISO para JSON
            "source_name": self.source_name,
            "source_url": self.source_url,
            "metadata": self.metadata
        }
    
    def __str__(self):
        return f"Article({self.id}, {self.title}, from {self.source_name})"

# Clase base para fuentes de noticias
class NewsSource:
    def __init__(self, source_id: str, url: str, polling_interval: int = 300):
        self.source_id = source_id
        self.url = url
        self.polling_interval = polling_interval
        self.last_fetch_time = None
        self.session = None
        self.logger = logging.getLogger(f"source.{source_id}")
    
    async def initialize(self):
        """Inicializa la conexión HTTP."""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
    
    async def close(self):
        """Cierra la conexión HTTP."""
        if self.session:
            await self.session.close()
    
    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        before_sleep=lambda retry_state: logging.warning(
            f"Reintentando conexión a {retry_state.kwargs['self'].source_id}, "
            f"intento {retry_state.attempt_number}"
        )
    )
    async def fetch_articles(self) -> List[Article]:
        """Método abstracto que cada fuente específica debe implementar."""
        raise NotImplementedError("Cada fuente debe implementar este método")
    
    async def process_source(self, queue: asyncio.Queue):
        """Procesa continuamente la fuente y envía artículos a la cola."""
        try:
            await self.initialize()
            while True:
                try:
                    self.logger.info(f"Obteniendo artículos de {self.source_id}")
                    articles = await self.fetch_articles()
                    
                    for article in articles:
                        # Añadir a la cola para envío al servidor central
                        await queue.put(article)
                        self.logger.debug(f"Artículo añadido a la cola: {article.title}")
                    
                    self.last_fetch_time = datetime.now()
                    self.logger.info(f"Obtenidos {len(articles)} artículos de {self.source_id}")
                
                except Exception as e:
                    self.logger.error(f"Error procesando {self.source_id}: {str(e)}")
                    self.logger.error(traceback.format_exc())
                    # No rompemos el bucle - seguimos intentando
                
                # Esperamos el intervalo de polling antes de la siguiente consulta
                await asyncio.sleep(self.polling_interval)
        
        except Exception as e:
            self.logger.critical(f"Error crítico en la fuente {self.source_id}: {str(e)}")
            self.logger.critical(traceback.format_exc())
        finally:
            await self.close()

# Implementación específica para una fuente RSS
class RssNewsSource(NewsSource):
    async def fetch_articles(self) -> List[Article]:
        async with self.session.get(self.url) as response:
            response.raise_for_status()
            content = await response.text()
            
            # Usar feedparser para procesar el contenido RSS
            feed = feedparser.parse(content)
            
            articles = []
            for entry in feed.entries:
                try:
                    # Manejar diferentes formatos de fecha
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        published_date = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        published_date = datetime(*entry.updated_parsed[:6])
                    else:
                        published_date = datetime.now()
                    
                    # Obtener ID único o generar uno
                    entry_id = getattr(entry, 'id', entry.link)
                    
                    article = Article(
                        id=f"{self.source_id}:{entry_id}",
                        title=entry.title,
                        content=getattr(entry, 'description', getattr(entry, 'summary', '')),
                        published_date=published_date,
                        source_name=self.source_id,
                        source_url=entry.link
                    )
                    articles.append(article)
                except Exception as e:
                    self.logger.warning(f"Error procesando entrada RSS: {str(e)}")
            
            return articles

# Implementación específica para una API JSON
class JsonApiNewsSource(NewsSource):
    async def fetch_articles(self) -> List[Article]:
        async with self.session.get(self.url) as response:
            response.raise_for_status()
            data = await response.json()
            
            articles = []
            # La estructura específica dependerá de la API
            for item in data.get("articles", []):
                try:
                    # Parse date with error handling
                    try:
                        published_date = datetime.fromisoformat(item['publishedAt'].replace('Z', '+00:00'))
                    except (ValueError, KeyError):
                        try:
                            published_date = datetime.strptime(item.get('publishedAt', ''), '%Y-%m-%dT%H:%M:%SZ')
                        except ValueError:
                            published_date = datetime.now()
                    
                    article = Article(
                        id=f"{self.source_id}:{item.get('id', hash(item.get('url', '') + item.get('title', '')))}",
                        title=item.get('title', 'Sin título'),
                        content=item.get('content', item.get('description', 'Sin contenido')),
                        published_date=published_date,
                        source_name=self.source_id,
                        source_url=item.get('url', ''),
                        metadata=item.get('metadata', {})
                    )
                    articles.append(article)
                except Exception as e:
                    self.logger.warning(f"Error procesando artículo JSON: {str(e)}")
            
            return articles

# Cliente para enviar datos al servidor central
class CentralServerClient:
    def __init__(self, server_url: str, node_id: str):
        self.server_url = server_url
        self.node_id = node_id
        self.session = None
        self.logger = logging.getLogger("central_client")
    
    async def initialize(self):
        """Inicializa la conexión HTTP."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"X-Node-ID": self.node_id}
        )
    
    async def close(self):
        """Cierra la conexión HTTP."""
        if self.session:
            await self.session.close()
    
    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(10),
        before_sleep=lambda retry_state: logging.warning(
            f"Reintentando envío al servidor central, "
            f"intento {retry_state.attempt_number}"
        )
    )
    async def send_article(self, article: Article):
        """Envía un artículo al servidor central."""
        try:
            # Usamos to_dict para serializar correctamente el datetime
            article_data = article.to_dict()
            
            async with self.session.post(
                f"{self.server_url}/api/articles",
                json=article_data
            ) as response:
                response.raise_for_status()
                result = await response.json()
                return result.get("id")
        except Exception as e:
            self.logger.error(f"Error enviando artículo al servidor central: {str(e)}")
            raise

# Clase principal del nodo de adquisición
class AcquisitionNode:
    def __init__(self, node_id: str, central_server_url: str):
        self.node_id = node_id
        self.central_server_url = central_server_url
        self.sources = []
        self.article_queue = asyncio.Queue(maxsize=10000)
        self.central_client = CentralServerClient(central_server_url, node_id)
        self.logger = logging.getLogger(f"node.{node_id}")
    
    def add_source(self, source: NewsSource):
        """Añade una fuente de noticias al nodo."""
        self.sources.append(source)
    
    async def process_queue(self):
        """Procesa la cola de artículos y los envía al servidor central."""
        await self.central_client.initialize()
        try:
            while True:
                article = await self.article_queue.get()
                try:
                    await self.central_client.send_article(article)
                    self.logger.debug(f"Artículo enviado: {article.title}")
                except Exception as e:
                    self.logger.error(f"Error enviando artículo al servidor central: {str(e)}")
                    # Podríamos implementar un mecanismo de reintentos más sofisticado
                    # o guardar en disco para enviar más tarde
                finally:
                    self.article_queue.task_done()
        finally:
            await self.central_client.close()
    
    async def run(self):
        """Inicia el nodo de adquisición."""
        self.logger.info(f"Iniciando nodo de adquisición {self.node_id}")
        
        # Crear tarea para procesar la cola
        queue_processor = asyncio.create_task(self.process_queue())
        
        # Crear tareas para cada fuente
        source_tasks = []
        for source in self.sources:
            task = asyncio.create_task(source.process_source(self.article_queue))
            source_tasks.append(task)
        
        # Esperar a que todas las tareas terminen (en realidad nunca deberían terminar)
        try:
            await asyncio.gather(*source_tasks)
        except Exception as e:
            self.logger.critical(f"Error en las tareas de fuentes: {str(e)}")
        finally:
            # Si llegamos aquí, algo salió mal. Cancelamos la tarea de procesamiento de la cola
            queue_processor.cancel()
            try:
                await queue_processor
            except asyncio.CancelledError:
                pass
            
            # Cerramos todas las fuentes
            for source in self.sources:
                try:
                    await source.close()
                except:
                    pass

# Simulación del servidor central para pruebas
class MockCentralServer:
    def __init__(self, host="localhost", port=8080):
        self.host = host
        self.port = port
        self.app = None
        self.articles = []
        self.logger = logging.getLogger("mock_server")
    
    async def handle_article(self, request):
        """Manejador para recibir artículos."""
        try:
            article_data = await request.json()
            self.logger.info(f"Artículo recibido: {article_data.get('title', 'Sin título')}")
            self.articles.append(article_data)
            return aiohttp.web.json_response({"id": article_data.get("id"), "status": "received"})
        except Exception as e:
            self.logger.error(f"Error procesando artículo recibido: {str(e)}")
            return aiohttp.web.json_response({"error": str(e)}, status=500)
    
    async def start(self):
        """Inicia el servidor mock."""
        from aiohttp import web
        
        self.app = web.Application()
        self.app.router.add_post('/api/articles', self.handle_article)
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        self.logger.info(f"Servidor mock iniciado en http://{self.host}:{self.port}")
        
        # Keep the server running
        while True:
            await asyncio.sleep(3600)  # Just keep it alive

# Función principal para configurar y ejecutar el nodo
async def main():
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("main")
    
    # Iniciar un servidor mock para pruebas (comentar en producción)
    use_mock_server = True
    if use_mock_server:
        mock_server = MockCentralServer()
        mock_server_task = asyncio.create_task(mock_server.start())
        central_server_url = "http://localhost:8080"
        logger.info("Usando servidor mock para pruebas")
    else:
        central_server_url = "https://central-server.frankfurt.example.com"
    
    # Crear el nodo de adquisición
    node = AcquisitionNode(
        node_id="madrid-node-1",
        central_server_url=central_server_url
    )
    
    # Configurar fuentes para este nodo - Usamos algunas fuentes públicas para pruebas
    node.add_source(RssNewsSource(
        source_id="bbc-tech-rss",
        url="http://feeds.bbci.co.uk/news/technology/rss.xml",
        polling_interval=60  # 1 minuto para pruebas
    ))
    
    node.add_source(RssNewsSource(
        source_id="nasa-breaking-news",
        url="https://www.nasa.gov/rss/dyn/breaking_news.rss",
        polling_interval=90  # 1.5 minutos para pruebas
    ))
    
    # Añadir ejemplo de API JSON (esto es un ejemplo, necesitaría adaptarse a la API real)
    # En pruebas, esta probablemente fallará, lo que es útil para ver la tolerancia a fallos
    node.add_source(JsonApiNewsSource(
        source_id="newsapi-tech",
        url="https://newsapi.org/v2/top-headlines?category=technology&apiKey=YOUR_API_KEY",
        polling_interval=120  # 2 minutos para pruebas
    ))
    
    # Ejecutar el nodo
    logger.info("Iniciando el nodo de adquisición...")
    try:
        await node.run()
    except KeyboardInterrupt:
        logger.info("Nodo detenido manualmente")
    except Exception as e:
        logger.error(f"Error fatal: {str(e)}")
        logger.error(traceback.format_exc())

# Punto de entrada
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Nodo detenido manualmente")
    except Exception as e:
        print(f"Error fatal: {e}")
        traceback.print_exc()