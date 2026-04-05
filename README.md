# distributed-system-platform-dz3

Лабораторная работа по Raft на FastAPI + Docker Compose.

## Состав
- `app/` — Raft-узел (Follower/Candidate/Leader)
- `client/client.py` — клиент для отправки команд
- `docker-compose.yml` — кластер из 3 узлов
- `Makefile` — команды управления

## Быстрый старт
```bash
make build
make up
make health
```

## Отправка команды
```bash
make command KEY=my_key VALUE=my_value
```

## Чтение состояния
```bash
make get KEY=my_key
```

## Остановка
```bash
make down
```

## API узла
- `POST /command` — принять клиентскую команду (только лидер)
- `GET /state/{key}` — получить значение ключа
- `GET /health` — состояние узла
- `POST /raft/request-vote` — RPC RequestVote
- `POST /raft/append-entries` — RPC AppendEntries
