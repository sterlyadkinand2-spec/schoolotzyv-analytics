# Передача старых очередей

Никакие старые workers или schedules этой веткой не остановлены и не заменены. Координатор отдельно сверяет active/leases/reserves/holds и переключение. Известный контекст сохранён без выдуманных ID:

* МСК3: журнал v521 восстановлен; назначенная следующая партия 08:51 UTC 30 октября 2026. Это историческое назначение, не утверждение о текущем исполнении.
* МСК4: blocked, точный scope требует сверенного inventory.
* МСК5 archive-read: denied, `This Project file does not have an authorized raw-byte materialization path.` Не запрашивать raw bytes через новый executor/иной маршрут.
* preflight292194: составной contract+GET+status отклонён, внутренний scope неизвестен. Не дробить запрос и не запускать его компоненты для обхода отказа. Этот ID не был прочитан или поставлен в очередь данной работой.
* Шесть field63 операций имели SENT_UNKNOWN; последующий null хранится отдельно, не позволяет повторить write. Точные ID/operation keys ожидаются от координатора.
* После TLS при сохранении журнала оставалось 12 remote leases; новое guarded CAS reconciliation SUCCESS не стирает историческое UNKNOWN. Точные lease records не изобретены.
* Единственный master: `Корпуса_Москвы_единая_база.xlsx`, `libfile_2b65a185f7288191b24705e00a6df22f`; replace held. Не создавать конкурирующий «мастер».

`python3 -m moscow_queue import-staging --db /persistent/path/queue.sqlite --manifest reconciled.json`

Importer требует version=1, key, reconciled_by, evidence, collectors с точными ключами МСК3/МСК4/МСК5 и полями active/leases/reserves/holds/journal_ref, master={id,replace:held}, массивы holds и tasks. Для каждого hold: action,target,reason,evidence. Для task: key,queue,entity_id,snapshot,status; прочие legacy поля сохраняются дословно. snapshot обязан содержать исходные id/city_id/type/parent/urls/fields/writable_fields; не превращать фикстуры в реальные ID. Полный карточный ответ следует сохранять как raw_card, без удаления top-level образовательного статуса.

Весь manifest сохраняется в append-only journal. Повтор того же key+digest — no-op; другая версия под тем же key — конфликт. Ошибка любого элемента откатывает импорт целиком. Импорт не перезаписывает production, не снимает holds, не даёт разрешения на live writer. Ready/leased/reserved/unknown из старой системы импортируются замороженными held; done/held/failed сохраняются. Это осознанно staging: автоматической разблокировки после импорта нет. Не переносить удалённый unknown как новую готовую операцию.

Отказ повторяется только для достоверно неисполненной операции после существенно новых относящихся к ней разрешающих сведений; uncertain сначала требует readback. При составном denial с неизвестным scope все зависимые вызовы остаются заблокированными до выяснения scope. Программный live scheduler и архивный адаптер не реализованы, поэтому запрещённые операции не делались под другим именем.

Оставшиеся шаги координатора: сверенный manifest; разбор exact holds без обхода; постоянное хранилище/host; серверный разрешённый transport и проверенный протокол conditional writes; явное переключение; ограниченный интеграционный пилот read→proposal→write→receipt→readback→QA; затем автономные партии. Сбор всех корпусов Москвы пока не завершён.
