# ТЗ — Apps Script Web App: будівник фінансових таблиць

> Версія 1.0 | Фінансова система малого бізнесу | @matsukoleksandr

---

## Контекст

Це не просто скрипт який записує дані в клітинки.  
Це **агент-будівник** який отримує структурований опис таблиці, сам вирішує як її організувати, перевіряє себе після побудови і вміє вносити точкові правки без перебудови з нуля.

Живе як **Standalone Apps Script проєкт** на Google Drive власника курсу.  
Розгорнутий як **Web App** — приймає POST-запити від Telegram-агента.  
Виконується від імені власника курсу — має доступ до його Drive.

Використовується для побудови: Cashflow, P&L, Баланс, Дашборд.  
Один скрипт — всі типи таблиць.

---

## 1. Точка входу

```javascript
function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents);
    
    switch (payload.action) {
      case 'ping':         return respond({ status: 'ok', message: 'pong' });
      case 'build_table':  return buildTable(payload);
      case 'update_table': return updateTable(payload);
      default:             return respond({ status: 'error', message: 'unknown action: ' + payload.action });
    }
  } catch (err) {
    return respond({ status: 'error', message: err.message, stack: err.stack });
  }
}

function respond(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
```

---

## 2. buildTable(payload) — основна функція побудови

### 2.1 Загальна логіка

```
1. Валідація payload
2. Знайти або створити кореневу папку курсу
3. Знайти або створити підпапку компанії
4. Створити пусті підпапки P&L, Баланс, Дашборд (якщо не існують)
5. Перевірити чи файл з такою назвою вже існує
6. Створити Spreadsheet
7. Побудувати аркуші залежно від report_type і архітектури
8. Налаштувати форматування, формули, захист, дропдауни
9. Якщо потрібна Google Form — створити і прив'язати
10. Налаштувати права доступу
11. Самоперевірка побудованого файлу
12. Повернути результат
```

### 2.2 Валідація payload

Перед будь-якою роботою — перевірити що є обов'язкові поля:

```javascript
function validatePayload(payload) {
  const required = ['report_type', 'business_name', 'articles'];
  const missing = required.filter(f => !payload[f]);
  if (missing.length > 0) {
    throw new Error('Відсутні обов\'язкові поля: ' + missing.join(', '));
  }
  
  const validTypes = ['cashflow', 'pl', 'balance', 'dashboard'];
  if (!validTypes.includes(payload.report_type)) {
    throw new Error('Невідомий тип таблиці: ' + payload.report_type);
  }
}
```

### 2.3 Структура папок на Drive

```
ROOT_FOLDER_NAME = "Фінансова система — Курс"

Фінансова система — Курс/
└── {business_name}/
    ├── Cashflow/
    │   └── Cashflow_{business_name}_{year}.xlsx  ← створюється тут
    ├── P&L/          ← пуста, створюється одразу
    ├── Баланс/       ← пуста, створюється одразу
    └── Дашборд/      ← пуста, створюється одразу
```

Логіка пошуку/створення папок:

```javascript
function getOrCreateFolder(parentFolder, name) {
  const existing = parentFolder.getFoldersByName(name);
  if (existing.hasNext()) return existing.next();
  return parentFolder.createFolder(name);
}
```

Якщо файл з такою назвою вже існує — додати `_v2`, `_v3` і т.д.:

```javascript
function resolveFileName(folder, baseName) {
  let name = baseName;
  let version = 2;
  while (folder.getFilesByName(name + '.xlsx').hasNext() ||
         folder.getFilesByName(name).hasNext()) {
    name = baseName + '_v' + version;
    version++;
  }
  return name;
}
```

---

## 3. Аркуші — що будується для кожного типу

### 3.1 Cashflow

| Аркуш | Умова | Опис |
|-------|-------|------|
| 📊 Cashflow | завжди | Зведений звіт. Формується SUMIF. Захищений. |
| ⬇️ Надходження | arch A або B для inflows | Введення надходжень: дата / контрагент / стаття / сума / коментар |
| ⬆️ Витрати | arch A або B для outflows | Введення витрат: аналогічно |
| ⬆️ Витрати — {Ім'я} | є особа з input_mode: 'sheet' | Спрощений аркуш для підзвітної особи |
| 📅 Платіжний календар | options.payment_calendar | Прогноз залишку по датах |
| 📝 Лог | є хтось з input_mode: 'form' | Сирі дані з Google Form. Не редагується. |
| 📋 Довідники | завжди | Статті, відповідальні, контрагенти. Named ranges. |
| ⚙️ Налаштування | завжди | Параметри файлу |
| 🔗 References | завжди | Посилання на інші файли для IMPORTRANGE |

### 3.2 P&L

| Аркуш | Умова | Опис |
|-------|-------|------|
| 📊 P&L | завжди | Зведений звіт. Захищений. |
| 💰 Доходи | є статті доходів | Введення доходів |
| 💸 Прямі витрати | є статті COGS | Введення прямих витрат |
| 💸 Операційні витрати | є статті OPEX | Введення операційних витрат |
| 📋 Довідники | завжди | Статті P&L |
| ⚙️ Налаштування | завжди | Параметри |
| 🔗 References | завжди | IMPORTRANGE посилання |

### 3.3 Баланс і Дашборд

Будуються за аналогічним принципом — структура передається в payload.  
Дашборд тягне дані з інших файлів через References → IMPORTRANGE.

---

## 4. Аркуш «Довідники» — детально

Це серце таблиці. Всі дропдауни беруться звідси.

```
Стовпець A: Статті надходжень    → named range "articles_inflows"
Стовпець C: Статті витрат        → named range "articles_outflows"
Стовпець E: Відповідальні        → named range "responsible_list"
Стовпець G: Контрагенти          → named range "counterparties"  (якщо options.counterparty_tracking)
```

Заповнюється з `payload.articles` і `payload.responsible`.

Named ranges — щоб при додаванні рядків дропдауни автоматично оновлювались:

```javascript
function createNamedRange(sheet, rangeName, startRow, col, numRows) {
  const range = sheet.getRange(startRow, col, numRows, 1);
  const ss = sheet.getParent();
  // видалити якщо вже існує
  const existing = ss.getNamedRanges().find(nr => nr.getName() === rangeName);
  if (existing) existing.remove();
  ss.setNamedRange(rangeName, range);
}
```

---

## 5. Аркуші введення — структура і формули

### 5.1 Аркуш «Надходження» / «Витрати»

```
Рядок 1 (заголовки, закріплені):
A: Дата | B: Контрагент | C: Стаття | D: Сума | E: Коментар

Рядок 2+: дані

Налаштування:
- Стовпець C: Data Validation → список з named range
- Стовпець A: формат дати DD.MM.YYYY
- Стовпець D: числовий формат # ##0.00
- Рядок 1: freeze
- Захист заголовків від редагування
```

### 5.2 Аркуш «Витрати — {Ім'я}» (для підзвітних)

Спрощена версія — тільки найнеобхідніше:

```
A: Дата | B: Що куплено | C: Стаття | D: Сума

Внизу:
- Підсумок: =SUM(D2:D)
- Залишок авансу: = {сума авансу} - SUM(D2:D)  // сума авансу вноситься вручну в Налаштування
```

### 5.3 Аркуш «Платіжний календар»

```
Рядок 1: Дата | Надходження план | Виплати план | Залишок
Рядок 2+: по кожній даті місяця

Залишок = попередній залишок + надходження - виплати
Умовне форматування: якщо залишок < Налаштування!B5 → фон #FEF2F2
```

---

## 6. Зведений аркуш «Cashflow» — формули

```javascript
// По кожній статті надходжень:
// =SUMIF(Надходження!C:C, A{row}, Надходження!D:D)

// По кожній статті витрат (може бути кілька джерел):
// =SUMIF(Витрати!C:C, A{row}, Витрати!D:D)
//  + SUMIF('Витрати — Дмитро'!C:C, A{row}, 'Витрати — Дмитро'!D:D)
//  + SUMIF(Лог!C:C, A{row}, Лог!D:D)  // якщо є Form

// Підсумки:
// Загальні надходження: =SUM(...)
// Загальні виплати: =SUM(...)
// Чистий Cashflow: = надходження - виплати
// Залишок: = Налаштування!B4 + чистий cashflow

// Умовне форматування рядка Залишок:
// < 0 → фон #FEF2F2 (червоний)
// >= 0 → фон #E6F4EC (зелений)
```

Захист аркушу — дозволити редагування тільки власнику:

```javascript
function protectSheet(sheet, description) {
  const protection = sheet.protect();
  protection.setDescription(description);
  protection.setWarningOnly(false); // жорсткий захист
  // зняти захист для поточного користувача (власника)
  const me = Session.getEffectiveUser();
  protection.addEditor(me);
  protection.removeEditors(protection.getEditors().filter(e => e.getEmail() !== me.getEmail()));
}
```

---

## 7. Google Form (архітектура C)

Якщо є хтось з `input_mode: 'form'`:

```javascript
function createForm(businessName, articles, responsibleName) {
  const form = FormApp.create('Витрати — ' + responsibleName + ' | ' + businessName);
  form.setDescription('Форма для фіксації витрат');
  
  // Поля форми
  form.addDateItem().setTitle('Дата').setRequired(true);
  form.addTextItem().setTitle('Контрагент / Опис').setRequired(true);
  
  const articleItem = form.addListItem().setTitle('Стаття витрат').setRequired(true);
  articleItem.setChoiceValues(articles); // статті з payload
  
  form.addTextItem().setTitle('Сума (грн)').setRequired(true);
  form.addTextItem().setTitle('Коментар');
  
  // Прив'язати відповіді до аркушу Лог
  form.setDestination(FormApp.DestinationType.SPREADSHEET, spreadsheetId);
  // Перейменувати аркуш відповідей
  // (Google створює аркуш "Відповіді форми 1" — перейменувати на "Лог")
  
  return form.getPublishedUrl();
}
```

---

## 8. Аркуш «References»

Єдине місце де зберігаються посилання на інші файли.  
При копіюванні папки учасник змінює тільки цей аркуш.

```
A1: cashflow_url   B1: [url файлу Cashflow]
A2: pl_url         B2: [url файлу P&L]
A3: balance_url    B3: [url файлу Балансу]
```

Дашборд використовує:
```
=IMPORTRANGE(References!B1, "Cashflow!A1:B100")
=IMPORTRANGE(References!B2, "P&L!A1:B100")
```

При першому відкритті IMPORTRANGE вимагає підтвердження доступу — попередити про це в повідомленні боту.

---

## 9. Аркуш «Налаштування»

```
A1: Назва компанії      B1: {business_name}
A2: Звітний рік         B2: {current_year}
A3: Валюта              B3: ₴
A4: Залишок на початок  B4: 0
A5: Поріг підсвітки     B5: 0
A6: Авансу Дмитро       B6: 0   ← якщо є підзвітна особа
```

---

## 10. Права доступу

```javascript
function setPermissions(folder, file, userEmail, shareMode) {
  if (userEmail) {
    // Папка — тільки перегляд
    folder.addViewer(userEmail);
    // Файл — редагування
    file.addEditor(userEmail);
  }
  
  if (shareMode === 'anyone_with_link') {
    folder.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
  }
}
```

---

## 11. Самоперевірка після побудови

Після створення файлу — агент перевіряє себе перед тим як повернути результат:

```javascript
function validateBuiltFile(spreadsheet, payload) {
  const errors = [];
  const sheetNames = spreadsheet.getSheets().map(s => s.getName());
  
  // Перевірити обов'язкові аркуші
  const required = getRequiredSheets(payload); // залежить від report_type
  required.forEach(name => {
    if (!sheetNames.includes(name)) {
      errors.push('Відсутній аркуш: ' + name);
    }
  });
  
  // Перевірити named ranges
  const namedRanges = spreadsheet.getNamedRanges().map(nr => nr.getName());
  ['articles_inflows', 'articles_outflows'].forEach(name => {
    if (!namedRanges.includes(name)) {
      errors.push('Відсутній named range: ' + name);
    }
  });
  
  // Перевірити що Cashflow має формули (не порожній)
  const cashflowSheet = spreadsheet.getSheetByName('Cashflow') ||
                        spreadsheet.getSheetByName('📊 Cashflow');
  if (cashflowSheet) {
    const formulas = cashflowSheet.getDataRange().getFormulas().flat().filter(f => f !== '');
    if (formulas.length === 0) {
      errors.push('Аркуш Cashflow не містить формул');
    }
  }
  
  return { valid: errors.length === 0, errors };
}
```

---

## 12. updateTable(payload) — точкові правки

Отримує `spreadsheet_id` і масив змін. Не перебудовує таблицю — вносить мінімальний набір змін.

```javascript
function updateTable(payload) {
  const ss = SpreadsheetApp.openById(payload.spreadsheet_id);
  const results = [];
  
  for (const change of payload.changes) {
    try {
      switch (change.type) {
        case 'add_article':    results.push(addArticle(ss, change)); break;
        case 'remove_article': results.push(removeArticle(ss, change)); break;
        case 'rename_article': results.push(renameArticle(ss, change)); break;
        case 'change_access':  results.push(changeAccess(ss, change)); break;
        case 'add_sheet':      results.push(addSheet(ss, change)); break;
        case 'remove_sheet':   results.push(removeSheet(ss, change)); break;
        default:               results.push({ type: change.type, status: 'unknown type' });
      }
    } catch (err) {
      results.push({ type: change.type, status: 'error', message: err.message });
    }
  }
  
  return respond({ status: 'ok', changes_applied: results });
}
```

### Приклад — addArticle:

```javascript
function addArticle(ss, change) {
  // 1. Додати в Довідники
  const refSheet = ss.getSheetByName('Довідники') || ss.getSheetByName('📋 Довідники');
  const col = change.section === 'inflows' ? 1 : 3;
  const lastRow = refSheet.getLastRow();
  refSheet.getRange(lastRow + 1, col).setValue(change.article);
  
  // 2. Розширити named range
  const rangeName = change.section === 'inflows' ? 'articles_inflows' : 'articles_outflows';
  const namedRange = ss.getNamedRanges().find(nr => nr.getName() === rangeName);
  if (namedRange) {
    const oldRange = namedRange.getRange();
    const newRange = refSheet.getRange(oldRange.getRow(), col, lastRow + 1 - oldRange.getRow() + 1, 1);
    namedRange.setRange(newRange);
  }
  
  // 3. Додати рядок у зведений аркуш з формулою SUMIF
  const mainSheet = ss.getSheets()[0]; // перший аркуш — зведений
  // знайти секцію і додати рядок з формулою
  
  return { type: 'add_article', status: 'ok', article: change.article };
}
```

---

## 13. Відповідь після побудови

```json
{
  "status": "ok",
  "folder_url": "https://drive.google.com/drive/folders/...",
  "files": [
    {
      "name": "Cashflow_Агенція Ткаченко_2026",
      "url": "https://docs.google.com/spreadsheets/d/...",
      "spreadsheet_id": "1BxiMVs0XRA5..."
    }
  ],
  "forms": [
    {
      "name": "Витрати — Ірина",
      "url": "https://forms.google.com/..."
    }
  ],
  "sheets_built": ["Cashflow", "Надходження", "Витрати", "Витрати — Дмитро", "Лог", "Платіжний календар", "Довідники", "Налаштування", "References"],
  "validation": {
    "valid": true,
    "errors": []
  }
}
```

При помилці:
```json
{
  "status": "error",
  "message": "Зрозуміле пояснення що пішло не так",
  "details": "технічна деталь для дебагу"
}
```

---

## 14. Тестова функція

Запускається вручну в редакторі Apps Script для перевірки без HTTP-виклику:

```javascript
function testBuild() {
  const testPayload = {
    action: 'build_table',
    report_type: 'cashflow',
    business_name: 'Тест Компанія',
    language: 'uk',
    user_email: 'your@gmail.com', // замінити на реальний
    architecture: { inflows: 'A', outflows: 'B' },
    articles: {
      inflows: ['Оплата від клієнтів', 'Передоплати'],
      outflows: ['Зарплати', 'Підрядники', 'Оренда']
    },
    responsible: {
      'Оплата від клієнтів': { name: 'Марина', access: true, input_mode: 'direct' },
      'Зарплати':            { name: 'Наталія', access: true, input_mode: 'direct' },
      'Підрядники':          { name: 'Дмитро', access: false, input_mode: 'sheet', payment: 'accountable' }
    },
    options: {
      payment_calendar: true,
      multi_account: false,
      counterparty_tracking: false
    }
  };
  
  const result = buildTable(testPayload);
  Logger.log(result.getContent());
}
```

---

## 15. Важливі обмеження Apps Script

- **Таймаут**: 6 хвилин на виконання. Для великих таблиць (50+ статей) — оптимізувати batching операцій
- **Quota**: обмеження на кількість звернень до Google API за день — не критично для курсу
- **IMPORTRANGE**: вимагає ручного підтвердження при першому відкритті файлу — попередити користувача
- **Google Form → Spreadsheet**: прив'язка форми до таблиці може змінити порядок аркушів — враховувати при перейменуванні
- **Захист аркушів**: `setWarningOnly(false)` блокує редагування для всіх крім editors — перевірити що власник курсу в editors

---

## 16. Що не робити (антипатерни)

- **Не використовувати** `getValues()` / `setValues()` в циклі — краще один batch-виклик
- **Не хардкодити** назви аркушів як рядки в формулах — використовувати named ranges
- **Не перебудовувати** всю таблицю при правках — тільки мінімальні зміни
- **Не ігнорувати** помилки — кожна операція в try/catch з зрозумілим повідомленням

---

*Фінансова система малого бізнесу | Олександр Мацук | @matsukoleksandr | 2026*
