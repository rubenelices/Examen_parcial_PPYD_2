### EXAMEN 2 PPYD Rubén Elices.
Este repositorio contien el script que corre el codigo del ejercicio del segundo examen parcial.

## Sistema Distribuido de Análisis de Noticias
Este proyecto implementa un nodo de adquisición que obtiene artículos de múltiples fuentes de noticias (RSS y APIs JSON), los estandariza y los envía a un servidor central.

Características
Descarga noticias de varias fuentes de forma concurrente usando asyncio.

Estandariza los datos en un modelo común.

Envía cada artículo al servidor central mediante una API HTTP.

Incluye un servidor simulado (MockCentralServer) para pruebas locales.

Requisitos
Python 3.8 o superior

Instalar dependencias.

Fuentes configuradas
BBC Tecnología (RSS)

NASA Breaking News (RSS)

NewsAPI (requiere API key)
