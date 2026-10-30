# Исполнение и границы гарантий

SQLite — вариант для одного постоянного хоста с локальным диском и нескольких процессов. Не использовать файл на NFS и не копировать один файл без WAL/online backup. Для нескольких хостов нужен отдельный транзакционный backend; в этой реализации его нет. Проверки выполнены на синтетических данных.

## Очереди

`research`, `write`, `qa`, `export` независимы. `BEGIN IMMEDIATE` делает выбор и claim атомарными. Партия исследователя — до 20 task IDs; shard = исходный ID % 3, строго 0/1/2. `drain` после партии сам выбирает следующую; нет курсора в prompt. Последняя партия может быть меньше 20. Три worker процесса используют один файл БД и разные shard. Тест проверяет 125 заданий и автоматический проход трёх партий в каждом shard.

Статусы задач: ready → leased → done/held/conflict/failed. Истёкшая обычная аренда допускает новый claim. Каждая аренда имеет owner, случайный token, expires. Любое завершение/heartbeat/операционный переход проверяет owner+token+срок внутри транзакции. Долгие реальные callback должны продлевать аренду через heartbeat; при истечении результат старого владельца отклоняется. Fixture CLI не является supervisor или постоянно работающим scheduler.

Write claim выдаёт ровно одну задачу и запрещает второй активный write lease. Если существует sending/unknown, любые новые write claims запрещены. Только `claim_recovery` выдаёт readback-only lease; `write_one` отвергает такую аренду. Held неопределённая операция удерживает write gate до отдельного доказательного разрешения, но не блокирует research/QA/export.

Операция: prepared → sending → unknown → applied либо held/conflict. Намерение отправки фиксируется до вызова с историческим SENT_UNKNOWN. Результат вызова и readback записываются отдельно. Падение после сохранения intent, но до фактического send трактуется консервативно как неопределённость. Никакого автоматического повтора отправки. Readback null или совпадение значения без operation receipt НЕ доказывает NOT_SENT/APPLIED. Исторический SENT_UNKNOWN остаётся в append-only journal даже после успешного readback; reconciliation добавляет новую запись, не переписывает старую и не снимает hold.

Восстановление applied/dry_run после падения до finish не повторяет отправку. Создание downstream задач и finish выполняются в одной транзакции; UNIQUE(queue,dedupe) не допускает дублей. Другая payload под тем же ключом — Conflict.

Fixture backend атомарно проверяет весь expected snapshot, пишет snapshot и receipt под operation key. Только поэтому race между предварительным чтением и write безопасен в тестах. **Школьный MCP не объявляет этот контракт; fixture-гарантии нельзя приписывать ему.** Нет гарантии exactly-once для произвольного удалённого сервиса. Production adapter заблокирован кодом, а не флагом CLI.

## Данные и QA

Исходная полная карточка должна сохраняться в snapshot (при импорте MCP — raw_card плюс канонические writable поля по definition_id). Исследователь получает копию существующих данных. Работа разрешена с частично заполненным корпусом. План дополняет только пустые writable поля с evidence; заполненные значения не заменяются, предложения нормализации уходят в reviews. Evidence/basis хранится в proposal, отдельно от публичных values. `ВЫВОД по…` запрещён валидатором и при планировании, и перед отправкой. IDs, URL, связи, top-level educational_status и прочие поля snapshot не меняются.

Нельзя делать вывод о потере факта по одному сокращённому тексту: QA обязана учитывать всю карточку, включая top-level educational_status и другие registry fields. Тест сохраняет факт начальной школы 1–4 при наличии текста о продлёнке. Здесь реализована консервативная сохранность данных, **не полный семантический QA-агент**. QA остаётся ready для независимой проверки. Количество заполненных полей не является полной приёмкой. В bridge.py есть canonicalize_mcp для проверенной формы GET; scope parent берётся только из сверенного inventory. Полного inventory пока нет.

Каждый отказ источника сохраняется как action/target/reason/evidence; такой источник пропускается, другие разрешённые источники продолжаются. Новые CAPTCHA/security denial также становятся holds. Held tasks не попадают в claim. Нет автоматической release API. Точные inherited scope/составные операции не реконструируются по догадке.

Метрики раздельные: прочитанные задачи, применённые операции fixture, readback, QA, public, staging Excel, master Excel. Подсчёт идёт по уникальным задачам/операциям, не по попыткам. public считает только bridge observed_match, excel_merged всегда 0. В выполненных пилотах public=0: реальных записей нет. Research done означает завершение исследования этой задачи, не факт публикации/QA/Excel.

## Excel

После fixture applied создаётся export-задача. Её ошибка не блокирует независимые очереди. XLSX пишется во временный файл, fsync, atomic rename в новый staging-файл с content hash. Повтор может безопасно воссоздать тот же локальный артефакт. Все cells inline strings: формулы из источников не исполняются. Это не merge с мастер-книгой. EXCEL_MERGED допустим только после отдельного разрешённого replacement и readback мастера; такой путь здесь отсутствует.

## Проверка

`python3 -m unittest discover -s tests -v`: 35 тестов. Включены crash before-send, after-intent, after-send, after-applied, stale/forged lease, duplicate task, concurrent claim, singleton writer, conflicting edit до отправки и между read/send, held/CAPTCHA source, failed XLSX, unknown+null, readonly recovery lease, annotation rejection, сохранность фактов всей карточки, replay metrics, import freeze, append-only audit. Процессы перезапускаются логически через закрытие/повторное открытие двух SQLite БД; это не испытание power loss/дискового hardware.

Дополнительно реализован интерфейс ручного MCP handoff: см. BRIDGE.md. Его observed_match слабее fixture applied с CAS; эти гарантии намеренно не смешаны.
