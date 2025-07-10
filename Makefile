# Определяем переменные
NATS_CONTAINER_NAME := nats-server
NATS_PORT := 4222
NATS_HTTP_PORT := 8222
NATS_NETWORK := nats
APP_BINARY := ./app
COMPOSE_FILE := docker-compose.yml

# Основная цель: запуск NATS через docker-compose и приложения
all: start-nats build-app start-app

# --- Services block ---

#Запуск сервисов через docker-compose
start-services:
	@echo "Запуск сервисов через docker-compose..."
	@docker-compose -f $(COMPOSE_FILE) up -d
	@sleep 2

#Остановка всех сервисов docker-compose
stop-services:
	@echo "Остановка сервисов через docker-compose..."
	@docker-compose -f $(COMPOSE_FILE) stop

clean-services:
	@echo "Удаление контейнеров..."
	@docker-compose -f $(COMPOSE_FILE) down

# --- End services block ---

# --- DB block ---

# Запуск DB через docker-compose
start-db:
	@echo "Запуск DB-сервера через docker-compose..."
	@docker-compose -f $(COMPOSE_FILE) up -d db

# Остановка NATS
stop-db:
	@echo "Остановка DB..."
	@docker-compose -f $(COMPOSE_FILE) stop db

# --- End DB block ---

# --- NATS block ---

# Запуск NATS через docker-compose
start-nats:
	@echo "Запуск NATS-сервера через docker-compose..."
	@docker-compose -f $(COMPOSE_FILE) up -d nats
	@echo "Ожидание инициализации NATS-сервера..."

# Остановка NATS
stop-nats:
	@echo "Остановка NATS..."
	@docker-compose -f $(COMPOSE_FILE) stop
	@echo "NATS-сервер остановлен."

# Очистка: остановка и удаление контейнеров, томов и сети
clean-nats: stop-nats
	@echo "Удаление томов и сети..."
	@docker-compose -f $(COMPOSE_FILE) rm -f
	@docker volume rm nats-data || true
	@docker network rm $(NATS_NETWORK) || true
	@echo "Очистка завершена."

# Просмотр логов NATS
logs-nats:
	@echo "Просмотр логов NATS..."
	@docker-compose -f $(COMPOSE_FILE) logs nats

# --- End NATS block ---

# --- Cloudbeaver block---

start-cb:
	@echo "Запуск Cloudbeaver..."
	@docker-compose -f $(COMPOSE_FILE) up -d cloudbeaver

stop-cb:
	@echo "Остановка Cloudbeaver..."
	@docker-compose -f $(COMPOSE_FILE) stop cloudbeaver

# --- End cloudbeaver block ---

# --- App block ---

# Сборка Go-приложения
build-app:
	@echo "Сборка приложения..."
	@go build -o $(APP_BINARY) ./cmd/events

# Запуск приложения
start-app:
	@echo "Запуск приложения..."
	@$(APP_BINARY)

# --- End app block ---

# Проверка статуса сервисов
status:
	@echo "Проверка статуса сервисов..."
	@docker-compose -f $(COMPOSE_FILE) ps

#Запуск приложения
run:
	@echo "Запуск приложения..."
	@go run cmd/events/main.go


.PHONY: all run start-services stop-services start-nats stop-nats build-app start-app clean-nats logs-nats status logs-nats \
start-db stop-db start-cb stop-cb clean-services