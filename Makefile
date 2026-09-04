# Точки входа для локальной разработки. Все команды рассчитаны на запуск
# из корня репозитория.
#
# На Windows GNU make выполняет рецепты через cmd.exe, если рядом нет sh.exe,
# поэтому в рецептах нет unix-утилит, одинарных кавычек и конвейеров.

COMPOSE      := docker compose -f deploy/docker-compose.yml
POSTGRES_DSN ?= postgres://marketdata:marketdata@localhost:5432/marketdata?sslmode=disable

# Тесты гоняются с -race: агрегатор обновляет состояние из двух горутин
# (обработка батча и тикер дедлайнов), без детектора гонок такие ошибки
# не воспроизводятся. -count=1 отключает кэш результатов.
# Детектор требует cgo и установленного C-компилятора; там, где его нет,
# запускать как: make test GOTEST_FLAGS="-count=1"
GOTEST_FLAGS ?= -race -count=1

# golang-migrate запускается в контейнере, чтобы не требовать установки CLI.
# Внутри docker-сети база доступна по имени сервиса, а не по localhost.
MIGRATE_IMAGE := migrate/migrate:v4.18.1
MIGRATE       := docker run --rm --network mdp_default \
                   -v "$(CURDIR)/migrations:/migrations" $(MIGRATE_IMAGE) \
                   -path=/migrations \
                   -database "postgres://marketdata:marketdata@postgres:5432/marketdata?sslmode=disable"

.DEFAULT_GOAL := help

.PHONY: help up down logs migrate-up migrate-down ingestor aggregator \
        test lint fmt psql consume-trades consume-candles

# Список целей печатается голыми echo, а не вытаскивается из комментариев
# через grep/awk: в cmd.exe этих утилит нет.
help:
	@echo Available targets:
	@echo   up               Поднять Kafka, PostgreSQL и Kafka UI, создать топики
	@echo   down             Остановить инфраструктуру, сохранив тома с данными
	@echo   logs             Хвост логов инфраструктуры
	@echo   migrate-up       Применить миграции схемы
	@echo   migrate-down     Откатить последнюю миграцию
	@echo   ingestor         Запустить сервис ingestor
	@echo   aggregator       Запустить сервис aggregator
	@echo   test             Прогнать тесты
	@echo   lint             Статический анализ
	@echo   fmt              Форматирование и go vet
	@echo   psql             Интерактивная сессия psql
	@echo   consume-trades   Консольный консьюмер md.trades
	@echo   consume-candles  Консольный консьюмер md.candles

up:
	$(COMPOSE) up -d
	$(COMPOSE) ps

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

migrate-up:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

ingestor:
	go run ./cmd/ingestor

aggregator:
	go run ./cmd/aggregator

test:
	go test ./... $(GOTEST_FLAGS)

lint:
	golangci-lint run

fmt:
	gofmt -l -w .
	go vet ./...

psql:
	docker exec -it mdp-postgres psql -U marketdata -d marketdata

# Двойные кавычки вокруг разделителя обязательны: в cmd.exe одинарные
# кавычки не защищают символ | и он был бы разобран как конвейер.
consume-trades:
	docker exec -it mdp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic md.trades --property print.key=true --property key.separator=" | "

consume-candles:
	docker exec -it mdp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic md.candles --from-beginning --property print.key=true --property key.separator=" | "
