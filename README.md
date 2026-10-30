# schoolotzyv-analytics

Изолированное ядро очереди сбора школьных корпусов государственных кластеров Москвы.
**Статус: проверенный локальный staging-код, не развёрнутый сборщик.** Production transport намеренно отсутствует. Публичные карточки и мастер Excel не меняются.

Python 3.10+, только стандартная библиотека:

```sh
python3 -m unittest discover -s tests -v
python3 -m moscow_queue demo --directory /tmp/moscow-dry-run
python3 -m moscow_queue demo --directory /tmp/moscow-fixture --simulate-write
python3 -m moscow_queue status --db /tmp/moscow-fixture/queue.sqlite
```

`--simulate-write` пишет только в отдельную синтетическую SQLite fixture. Без этого флага отправка отсутствует. Для сравнения режимов нужны разные каталоги: выполненный dry-run не преобразуется в live-запись повторным запуском. Fixture IDs/URL вымышленные и не являются московским inventory.

* [Обследование инфраструктуры и блокеры](docs/INFRASTRUCTURE.md)
* [Модель исполнения, ограничения, приёмка](docs/OPERATIONS.md)
* [MCP bridge для координатора](docs/BRIDGE.md)
* [Правила импорта и сохранённые ограничения](docs/HANDOVER.md)

Код: `store.py` — транзакции/аренды/история; `engine.py` — исследования, план и отдельное восстановление; `adapters.py` — локальная fixture БД и staging XLSX; `handover.py` — замороженный импорт сверенного inventory. Тесты в `tests/test_queue.py`.
