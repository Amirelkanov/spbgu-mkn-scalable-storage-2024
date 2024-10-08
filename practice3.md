# Практика 3 — 4.1 Репликация

## ТЗ

- Реализовать репликасет `geojson` базы данных с протоколом репликации и векторными часами

## Архитектура

![Architecture](img/practice3-arch.png)

## ТЗ

Горутина `Engine` при запуске должна прочитать чекпоинт файл с данными затем должна прочитать файл с журналом транзакций.

После горутина `Engine` должна подключится ко всем адресатам из списка `replicas`.
Горутина `Engine` должна принимать новые транзакции из исходящих подключений `replicas`.

Горутина `Engine` должна принять *входящие* `websocket /replication` подключения и отправлять туда новые транзакции.

Горутина должна управлять переменной `vclock` `map[string]uint64`, которая содержит в себе `LSN` всех применённых транзакций со всех узлов. И с помощью переменной `vclock` и `LSN` у каждой транзакции фильтровать уже применённые транзакции.

Создавать новые транзакции может только `Storage` у которого `leader = true`.

В `main` создать несколько сервисов. В каждом из них перечислить адреса соседних сервисов. Таким образом сделать топологию `fullmesh`.

## Добавить HTTP API

- `websocket` `/replication`

- `GET /select`
  - Реализовать счетчик текущих запросов
    - При достижении порогового значения (например 3)
    - Редиректить на реплику
    - Реализовать защиту от бесконечных редиректов
