# Проверка 30 октября 2026 UTC

Среда schoolotzyv-analytics запустилась. Репозиторий `sterlyadkinand2-spec/schoolotzyv-analytics`, исходный main `feb39c40882efea116bae12388003a30d3f7d30e`. До изменений: только README. AGENTS.md и код старых сборщиков отсутствовали. Поиск ограничен выбранным workspace; закрытые хранилища credentials не читались.

| Возможность | Подтверждено | Ограничение |
|---|---|---|
| Локальное состояние | Python, SQLite 3.53.1, WAL + synchronous FULL, файловые транзакции, тесты перезапуска | Сохранность диска после уничтожения этой среды не подтверждена. Это не постоянный сервер |
| Общая БД портала | Чтение через подключённый школьный MCP v1.6 в чате | Не найден отдельный серверный SQL/API транспорт или настроенная connection string |
| Запись карточек | В каталоге tools есть update_institution, адреса, связи, реестровые поля | Запись не вызывалась. В опубликованной схеме update_institution нет expected revision/CAS/idempotency key. Гарантия отсутствия lost update не подтверждена |
| Планировщик | systemctl/docker binaries присутствуют | PID 1 = tail; crontab отсутствует. Бинарник не подтверждает сервис, daemon или разрешённый host. Ничего не установлено/запущено |
| GitHub | Origin доступен через существующий git transport; main прочитан через ls-remote | Не является БД очереди или подтверждённым scheduler; CI/deployment не настроены |
| ChatGPT MCP | Реальное read-only обращение успешно | tools из functions.exec не импортируются как Python API. Не найден документированный разрешённый серверный bridge. Токены не извлекались |
| Excel | Локальный staging XLSX формируется стандартной библиотекой | Master libfile_2b65a185f7288191b24705e00a6df22f replacement held, не обращались |

Успешные read-only запросы:

1. `list_directory_records(type=city,q=Москва,per_page=10)` вернул city_id=530 и 522 объекта типа cluster.
2. `list_institutions(city_id=530,object_types=[cluster],per_page=1,page=1)` вернул total=522, ID 246618, child_ids 293254–293257.

Это проверка доступности чтения, **не готовый inventory государственных школьных корпусов и не полнота Москвы**. Перечисленные реальные ID не добавлены в очередь и не резервировались. `school_building_kindergarten`/дошкольные корпуса вне текущего адаптера.

Нет выполненных production writes, импорта реальных очередей, изменения старых schedules, deployment или merge. Не создавались credentials, не покупались сервисы.

Для интеграционного пилота требуются: постоянный host/disk с backup; разрешённый серверный transport; проверенный атомарный CAS+receipt endpoint либо доказанный альтернативный протокол, исключающий внешних writers; точный сверенный manifest и явное переключение координатором. Эти зависимости не заменены обещанием, что эфемерный контейнер будет работать постоянно.

3. Дополнительный GET 293254 успешно вернул schema карточки. Фактический type оказался private_school_building, city=null, хотя parent cluster. Он не импортирован: это подтверждает необходимость проверять тип каждого корпуса и не считать список child_ids государственным inventory. Canonical mapper отвергает такой тип.

Добавлен offline MCP handoff интерфейс для родителя (docs/BRIDGE.md): fresh GET/minimal patch/GET, без claims CAS. Он не создаёт серверный доступ и не был использован для live write.
