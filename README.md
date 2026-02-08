# Система агрегации данных в реальном времени

### Генерация событий
- **3 режима генерации:**
    - `regular`: 1–10 событий/сек
    - `peak`: периодические всплески по 100–1000 событий
    - `night`: 1 событие каждые 10 секунд
- **Типы событий:**
    - Обычные просмотры (85%): длительность 10–600 секунд
    - Броунсы (10%): длительность < 5 секунд
    - Ошибки (5%): некорректные данные для тестирования валидации consumer’а
- **Дубликаты:** 1% событий имеют одинаковый ключ сообщения

### Стратегии отправки
- **Синхронная:** для критичных данных с логикой повторных попыток
- **Асинхронная:** для высокой пропускной способности с callback’ами
- **Batch отправка:** накопление N сообщений или X секунд

### Стратегии партиционирования
- **По ключу (70%):** партиционирование по `page_id`
- **Round-robin (20%):** равномерное распределение
- **Случайная (10%):** случайное распределение

### Обработка ошибок
- Повторные попытки с экспоненциальной задержкой (до 5 попыток)
- Максимальная задержка: 30 секунд
- Неудачные сообщения логируются

### Метрики и мониторинг
- Метрики Prometheus на `/metrics`
- Логирование статистики в реальном времени
- Endpoint для проверки состояния (health check)

### Динамическое управление
HTTP API для конфигурации во время работы:
- Изменение скорости генерации
- Переключение режимов генерации
- Принудительный запуск всплесков событий

## Запуск

Запуск инфраструктуры (Kafka + ClickHouse) в Docker, а сервисы локально:

```bash
# Запуск инфраструктуры
docker-compose up -d zookeeper kafka clickhouse kafka-ui kafka-init

# Скачивание зависимостей
go mod tidy

# Запуск Продюсера локально
cd producer
go run cmd/main.go

# Запуск Консьюмера локально (в другом терминале)
cd consumer
go run cmd/main.go -init-schema # Только при первом запуске
go run cmd/main.go
```
---


## Сервисы

| Service | Port | Description |
|---------|------|-------------|
| Zookeeper | 2181 | Координация Kafka |
| Kafka | 9092 | Брокер сообщений |
| ClickHouse (Native) | 9000 | БД (нативный протокол) |
| ClickHouse (HTTP) | 8123 | БД (HTTP интерфейс) |
| Kafka UI | 8090 | Веб-интерфейс для Kafka |
| Producer | 8080 | API генератора событий |
| Consumer | 8081 | API агрегатора |

---

## API Endpoints

### Изменение скорости
```bash
# Установить 100 событий/сек
curl -X POST "http://localhost:8080/config/speed?eps=100"
```

### Изменение режима
```bash
# Переключиться в режим пиковой нагрузки
curl -X POST "http://localhost:8080/config/mode?mode=peak"

```

### Генерация всплеска
```bash
# Сгенерировать 1000 событий немедленно
curl -X POST "http://localhost:8080/generate/burst?count=1000"
```

### Статус
```bash
# Получить текущую статистику
curl http://localhost:8080/status
```
### Health Чеки

```bash
# Check all services
docker-compose ps

# Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# ClickHouse
docker exec clickhouse clickhouse-client --query "SELECT 1"

# Producer status
curl http://localhost:8080/status

# Consumer status
curl http://localhost:8081/status
```

---

## Мониторинг

### Kafka UI
```
http://localhost:8090
```

### Метрики Продюсера
```bash
curl http://localhost:8080/metrics
curl http://localhost:8080/status
```

### Метрики Консьюмера
```bash
curl http://localhost:8081/metrics
curl http://localhost:8081/status
curl http://localhost:8081/query/aggregates?hours=1
```

### ClickHouse
```bash
# CLI
docker exec -it clickhouse clickhouse-client

# Queries
SELECT count() FROM page_views_raw;
SELECT count() FROM processing_errors;
```
---

# Устранение неполадок

### Высокая задержка
```
Last Send Latency: 500ms
```
**Решение:**
- Увеличьте размер пакета
- Используйте асинхронную отправку
- Проверьте сетевую производительность и состояние Kafka

### Messages Dropped
```
Event channel full, dropping event
```
**Решение:** Увеличьте буфер канала или снизьте скорость генерации


