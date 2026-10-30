# MCP bridge для родительского координатора

`MCPBridge` ничего не отправляет в сеть. Он формирует outbox envelope и принимает результат разрешённого connector. Это интерфейс для ограниченного координируемого пилота, **не серверный transport**. Production fixture writer по-прежнему отвергает реальные адаптеры.

```python
from moscow_queue.store import Store
from moscow_queue.bridge import MCPBridge, canonicalize_mcp

store = Store('/persistent/path/queue.sqlite')
bridge = MCPBridge(store)
lease = store.claim('write', 'coordinator')[0]
get_request = bridge.request_before(lease)
# Родитель отдельно выполняет get_institution через свой разрешённый connector.
# raw — полный фактический ответ; verified_parent — точный сверенный inventory.
# current = canonicalize_mcp(raw, verified_parent)
# Для дополнительного raw audit:
# with store.tx():
#     store.check(lease)
#     store.log(lease['id'], 'raw_mcp_before', raw)
# preview = bridge.arm(lease, current)  # default dry_run=True, send_allowed=False
```

`canonicalize_mcp` сохраняет все registry values, metadata определений и весь остальной ответ в card_context, включая educational_status, URLs, адреса, связи. Поля в canonical `fields` используют строковый definition_id. На запись допускаются только writable custom_field группы building_info и field63; geo/type/дошкольные поля не попадают в allowlist. Частный тип корпуса либо неоднозначная связь отклоняются. Отсутствующий город корпуса может наследоваться только от явно проверенного московского parent; противоречащий город отклоняется. Исходный raw ответ дополнительно сохраняется в journal координатором.

Для реальной выдачи write envelope нужен `arm(..., dry_run=False, handover_evidence={coordinator, legacy_writers_stopped, inventory_reconciled, authorization_ref})`. Этот метод в данной работе **не вызывался на реальных карточках**. Все четыре поля должны содержать проверенные сведения, не заглушки. Составные held scopes передаются как `hold_dependencies=[(action,target), ...]`; никакой компонент такого запроса нельзя отдельно выполнять. Перед GET родитель обязан проверить exact holds/зависимости, потому что мост не управляет внешним инструментом.

Алгоритм:

1. Один leased writer получает свежий GET. Сравнение со всем before; несовпадение → conflict без write.
2. Валидируется минимальный gap-only patch. Evidence остаётся вне публичного value.
3. `arm` сначала атомарно сохраняет SENT_UNKNOWN + exact request, только потом возвращает envelope. Родитель передаёт connector **только request.arguments**, а не локальный operation_key: MCP такого аргумента не объявляет.
4. Повторный `arm` запрещён, даже если координатор не уверен, был ли отправлен запрос.
5. `record_result(lease, actual_result, acknowledged=...)` сохраняет сырой ответ. acknowledged=True только для однозначно успешного ответа. Затем выдаётся GET readback request.
6. `record_readback` сохраняет current отдельно. Успешный ответ + точное совпадение полной canonical карточки → `observed_match`, отдельная QA-задача. Это наблюдение желаемого состояния, **не CAS и не гарантия отсутствия lost update между GET и write**. Значение, изменённое внешним writer в этом окне, могло быть затёрто — разрешение этого риска остаётся у координатора пилота.
7. Иначе состояние unknown + task held, никакого resend. Null не означает NOT_SENT. После истечения аренды доступна только readback recovery lease. При исключении/отказе использовать record_denial; intent не стирается. Постфактум reconciliation не открывает gate автоматически.

Если координатор упал после observed_match до task finish, новый writer lease может вызвать complete_observed, который атомарно завершит задачу и создаст QA без send; не запускать fixture write_one на MCP-задачах. Следует использовать один тип обработчика на ledger. Совместное выполнение fixture и реального bridge на одной БД заблокировано bind_transport на уровне ledger; для пилота отдельный файл.

Offline проверка bridge полностью исполняется через `python3 -m unittest discover -s tests -v`; tests/test_bridge.py моделирует последовательность GET→arm→result→GET, exact patch, null, denial и compound hold. Сетевых операций в тестах нет.

Минимальный недостающий доступ для постоянного сервера: согласованный постоянно включённый host с persistent volume/backup и правом запускать supervisor/timer; документированный авторизованный transport к тому же школьному API (существующие server credentials, предоставленные штатно, или разрешённый bridge runtime); сверенный inventory/holds и handover. Если требуется защита от внешних конкурентных правок — дополнительно серверный conditional update (revision/CAS) и долговечный operation receipt. Наличие connector в чате не даёт такого доступа серверу.
