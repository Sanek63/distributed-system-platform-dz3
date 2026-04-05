.PHONY: build up down restart logs ps health command get clean

KEY ?= demo
VALUE ?= value
NODES ?= http://localhost:8001,http://localhost:8002,http://localhost:8003

build:
docker compose build

up:
docker compose up -d

down:
docker compose down

restart: down up

logs:
docker compose logs -f --tail=100

ps:
docker compose ps

health:
python3 client/client.py --nodes "$(NODES)" health

command:
python3 client/client.py --nodes "$(NODES)" command --key "$(KEY)" --value "$(VALUE)"

get:
python3 client/client.py --nodes "$(NODES)" get --key "$(KEY)"

clean:
docker compose down -v
