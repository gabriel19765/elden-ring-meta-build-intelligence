# ⚔️ Guía del Sinluz: Ejecución de la Demo End-to-End
> **Proyecto Final de Big Data Aplicado 2026**
> *Esta guía está diseñada para que la abras en VS Code durante tu presentación oral y ejecutes los comandos paso a paso para deslumbrar al tribunal.*

---

## 🏗️ FASE 0: Preparación y Encendido del Reino
Antes de empezar tu exposición, limpia el sistema y arranca toda la infraestructura de contenedores. Para ahorrar memoria y demostrar tu dominio de la rúbrica, levantaremos también el **perfil de NiFi**.

Ejecuta en tu terminal:

```bash
# 1. Detener y limpiar volúmenes previos para empezar con una demo totalmente limpia
make reset

# 2. Levantar la infraestructura completa incluyendo el perfil de Apache NiFi
docker compose --profile nifi up -d
```

> **⏳ Nota de Gracia:** Espera unos 30-40 segundos para que Zookeeper, Kafka, Postgres y Apache NiFi terminen de arrancar y estar saludables (`healthy`). Puedes comprobar su estado abriendo **Portainer** o ejecutando:
> ```bash
> docker compose ps
> ```

---

## 📚 FASE 1: Procesamiento de Datos Batch (Apache Spark)
* **Narrativa para el Tribunal:** *"Comenzamos cargando los datos estáticos de 322 armas y 114 jefes de la API oficial. Usamos Apache Spark para limpiar, cruzar y calcular una matriz de efectividad física, mágica y de fuego que nos da 36,708 afinidades teóricas."*

Ejecuta el Job de Spark en segundo plano:
```bash
docker compose run --rm spark-batch
```

> **🔍 Qué mostrar en la demo:** 
> Abre tu terminal y haz un query a la base de datos de Postgres para demostrar al tribunal que Spark ha insertado las 36,708 filas correctamente:
> ```bash
> docker exec -it er_postgres psql -U elden -d elden_ring -c "SELECT COUNT(*) FROM weapon_boss_effectiveness;"
> ```

---

## 📦 FASE 2: Ingesta de Datos en Tiempo Real (Apache NiFi)
* **Narrativa para el Tribunal:** *"Para la ingesta continua de metadatos de las armas, hemos diseñado una tubería en Apache NiFi 2.x que extrae en tiempo real los detalles de la API de Elden Ring y los publica directamente a un topic de Apache Kafka."*

1. **Abre el navegador en:** 👉 **[https://localhost:8443/nifi](https://localhost:8443/nifi)**
2. **Omitir SSL:** Haz clic en **Avanzado...** y luego en **Aceptar el riesgo y continuar**.
3. **Inicia sesión:**
   * **Usuario:** `admin`
   * **Contraseña:** `admin123456789`
4. **Cómo Importar el Flujo JSON (En 2 segundos):**
   * Arrastra el icono de **Process Group** (el cuarto icono de la barra superior, representado por **dos cuadrados superpuestos**) al lienzo central.
   * En la ventana emergente, haz clic en el icono de **Upload / Browse** (el botón con un icono de caja y flecha hacia arriba, situado justo a la derecha de la casilla del Nombre).
   * Selecciona el archivo JSON de tu repositorio:
     📂 `/home/gabi/CLASE/BIG_DATA_APLICADO/proyecto final/nifi/flows/elden_ring_ingest.json`
   * Haz clic en **Add**.
   * ¡Y listo! Aparecerá al instante el grupo con todo tu flujo pre-configurado y válido de `InvokeHTTP` ➡️ `PublishKafka`.
   * Entra en él haciendo doble clic y enciende los procesadores haciendo clic derecho -> **Start**.

---

## ⚡ FASE 3: Simulación en Vivo y Detección de Meta-Shifts (Kafka + Flink)
* **Narrativa para el Tribunal:** *"Ahora simularemos a cientos de jugadores enfrentándose a jefes del juego en tiempo real. Los eventos de victoria y derrota se publican a Kafka. Apache Flink los consume, calcula win-rates en vivo en ventanas de 5 minutos y alerta de variaciones superiores al 15% (Meta-Shifts)."*

Ejecuta el script interactivo que inyecta automáticamente los escenarios reales de juego:
```bash
bash scripts/demo.sh
```

El script simulará tres grandes escenarios de metagame:
* **Escenario 1 (Moonveil vs Margit):** 80 combates en tiempo real (alta tasa de victorias).
* **Escenario 2 (Rivers of Blood vs Malenia):** 75 combates intensos en el Árbol Hierático.
* **Escenario 3 (Dark Moon Greatsword vs Rennala):** Muestra un win-rate masivo del 91% (build de mago perfecto).

---

## 📊 FASE 4: Visualización del Éxito (Streamlit, Grafana y Discord)
Para coronar la presentación, abre las tres ventanas de salida interactiva ante el tribunal:

### 1. El Asistente de Combate Interactiva (Streamlit)
* **URL:** 👉 **[http://localhost:8501](http://localhost:8501)**
* **Acción:** Navega por las pestañas. En **🔍 Recomendador de Builds**, selecciona un jefe como *Malenia* o *Margit* y enseña al tribunal cómo el sistema compara al instante la **Recomendación Teórica (Spark)** con el **Rendimiento Real en Vivo (Flink)** que se acaba de simular. El gráfico de Plotly Express cargará de forma robusta e interactiva.

### 2. El Dashboard de Operaciones (Grafana)
* **URL:** 👉 **[http://localhost:3000](http://localhost:3000)**
* **Credenciales:** `admin` / `admin` (puedes omitir el cambio de contraseña).
* **Acción:** Muestra los gráficos en tiempo real que monitorizan las muertes, victorias, popularidad de armas y alertas generadas directamente desde Postgres.

### 3. Las Alertas del Canal de Comunicación (Discord)
* **Acción:** Abre tu canal de Discord donde configuraste el Webhook. Enseña los embeds enriquecidos en verde (**🚨 META-SHIFT: EMERGING**) y rojo (**🚨 META-SHIFT: DECLINING**) que tu bot publicó de forma automática al recibir las alertas de Flink a través de Kafka.

---

## 🧹 FASE 5: Cierre de la Exposición
Una vez que el tribunal te dé su enhorabuena, apaga el clúster de forma limpia y ordenada:
```bash
docker compose --profile nifi down -v
```

---
*¡Gracia y Gloria, Gabriel! Tienes en tus manos una presentación de 10/10.* ⚔️🔥
