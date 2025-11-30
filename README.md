# Informe Técnico: Motor de Búsqueda Textual (Full-Text Search)

## 1. Introducción y Diseño del Sistema
Este componente implementa un motor de búsqueda textual eficiente basado en el modelo de **Espacio Vectorial**, diseñado para operar sobre grandes volúmenes de datos sin depender de la carga total en memoria RAM (Memoria Principal).

La arquitectura se diseñó siguiendo el principio de **separación de responsabilidades**, articulándose con el motor de base de datos desarrollado en la primera entrega (Proyecto 1) de la siguiente manera:
* **Gestor de Datos (Proyecto 1):** El `HeapFile` actúa como la fuente de verdad, suministrando los registros crudos en formato binario.
* **Motor de Indexación (Proyecto 2):** Implementa la lógica de Procesamiento de Lenguaje Natural (NLP) y construcción de índices invertidos en memoria secundaria.

## 2. Técnicas y Decisiones de Diseño

### 2.1. Algoritmo SPIMI (Single-Pass In-Memory Indexing)
Para cumplir con el requisito de escalabilidad, se descartó la construcción del índice en memoria. Se adoptó el algoritmo **SPIMI**, el cual permite procesar colecciones de texto arbitrariamente grandes dividiéndolas en bloques manejables.

**Flujo de Implementación:**
1.  **Lectura por Lotes:** El sistema lee `N` registros (ej. 1000) desde el archivo binario del motor.
2.  **Inversión en Memoria:** Se construye un diccionario `{término: {doc_id: tf}}` en RAM hasta llenar el bloque.
3.  **Escritura a Disco:** El bloque se ordena alfabéticamente por término y se vuelca a disco como un archivo `.jsonl` secuencial.
4.  **Fusión (Merge):** Se utiliza un algoritmo **K-Way Merge** con una cola de prioridad (`Min-Heap`) para fusionar todos los bloques temporales en un único índice final, respetando la restricción de memoria (B-Buffers).

#### Explicación Gráfica del Funcionamiento (Diagrama de Flujo)

```mermaid
graph TD
    A[Fuente de Datos: HeapFile Binario] -->|Lee Lote de 1000| B(Preprocesamiento NLP)
    B --> C{¿Memoria Llena?}
    C -- No --> B
    C -- Sí --> D[Ordenar Términos Alfabéticamente]
    D --> E[Escribir Bloque Temporal .jsonl a Disco]
    E --> F{¿Más Datos?}
    F -- Sí --> B
    F -- No --> G[Fase de Fusión: K-Way Merge]
    
    subgraph "Memoria Secundaria (Disco)"
    E
    G
    end
    
    G -->|Heap de Prioridad| H[Índice Invertido Final]
    H --> I[Cálculo de Pesos Offline: IDF y Normas]
### 3. Ejecución Eficiente de Consultas (Similitud de Coseno)

La eficiencia del sistema no radica solo en la fórmula matemática, sino en la **estrategia de acceso a datos**. A diferencia de un escaneo secuencial que tiene una complejidad de $O(N)$ (donde $N$ es el total de documentos), nuestra implementación utiliza una arquitectura de indexación que reduce drásticamente el espacio de búsqueda.

#### 3.1. Estructura de Datos: Analogía con Árboles de Búsqueda (AVL)
Para entender por qué nuestra consulta es rápida, podemos usar la analogía de un **Árbol AVL (o B-Tree)**:

* **En un Árbol:** No recorres todos los nodos para encontrar un dato. La estructura te permite "descartar" ramas enteras y saltar directamente al nodo deseado en tiempo logarítmico.
* **En nuestro Índice Invertido:** No leemos todos los documentos. Utilizamos una estructura auxiliar en memoria (el **Lexicon**) que actúa como los punteros del árbol.

**Arquitectura Híbrida (RAM + Disco):**



1.  **El Lexicon (RAM):** Es un Hash Map (`Diccionario`) que reside en memoria principal. Su función es mapear cada término $t$ a su **ubicación física exacta** (offset en bytes) en el disco.
    * *Complejidad de acceso:* $O(1)$.
2.  **El Índice Invertido (Disco):** Es un archivo secuencial masivo (`.jsonl`) que contiene las *Posting Lists*. Solo accedemos a él mediante "saltos" precisos.
3.  **Normas Pre-calculadas (RAM):** Un arreglo ligero que contiene la magnitud $|\vec{d}|$ de cada documento, necesario para la normalización del coseno.

#### 3.2. Algoritmo de Consulta (Query Processing)
Cuando el sistema recibe una consulta (ej. *"sostenibilidad y finanzas"*), no escanea el archivo. Ejecuta el siguiente algoritmo de **Recuperación Dispersa**:

1.  **Vectorización de la Consulta ($\vec{q}$):**
    Se preprocesa la consulta y se calculan los pesos TF-IDF de sus términos en memoria.

2.  **Acceso Directo (Seek & Fetch):**
    Para cada término relevante en la consulta:
    * **Lookup:** Se busca el término en el *Lexicon*. Si no existe, se ignora (poda de búsqueda).
    * **Seek:** Si existe, obtenemos el *byte offset* (ej. byte 84500). El puntero de archivo del sistema operativo "salta" instantáneamente a esa posición (`file.seek(84500)`).
    * **Fetch:** Se lee **una sola línea** del disco (la *posting list* de ese término).
    * *Impacto:* En lugar de leer GBs de datos, leemos solo unos pocos KBs.

3.  **Cálculo de Similitud (Accumulator):**
    Se utiliza un acumulador disperso para sumar los productos punto solo de los documentos que contienen los términos:
    $$\text{DotProduct}(d) += W_{t,q} \times W_{t,d}$$

4.  **Normalización y Ranking:**
    Finalmente, aplicamos la fórmula del Coseno dividiendo por las normas pre-calculadas (que ya están en RAM, evitando lecturas adicionales):
    $$\text{Sim}(q, d) = \frac{\vec{q} \cdot \vec{d}}{\|\vec{q}\| \cdot \|\vec{d}\|}$$



**Conclusión:** Esta arquitectura garantiza que el tiempo de respuesta dependa del número de documentos que contienen los términos de la consulta ($k$), y no del tamaño total de la colección ($N$), logrando una complejidad de búsqueda sub-lineal.
