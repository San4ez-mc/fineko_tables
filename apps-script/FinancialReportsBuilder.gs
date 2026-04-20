var ROOT_FOLDER_NAME = 'Фінансова система — Курс';
var DEFAULT_SHARE_MODE = 'anyone_with_link';
var SETTINGS_SHEET_NAME = '⚙️ Налаштування';
var DEBUG_LOG_FILE_NAME = 'DEBUG_APP_SCRIPT_LOGS';
var DEBUG_LOG_SHEET_NAME = 'Logs';
var DEBUG_LOG_PROPERTY_KEY = 'DEBUG_LOG_SPREADSHEET_ID';
var CURRENT_TRACE_ID = '';
var CURRENT_ACTION = '';
var INPUT_THEME = {
  HEADER_BG: '#1A56DB',
  HEADER_TEXT: '#FFFFFF',
  ROW_ODD: '#FFFFFF',
  ROW_EVEN: '#EBF2FF'
};
var FORMULA_THEME = {
  HEADER_BG: '#B91C1C',
  HEADER_TEXT: '#FFFFFF',
  ROW_ODD: '#FFFFFF',
  ROW_EVEN: '#FEE2E2'
};
var THEME = {
  HEADER_BG: '#1A56DB',
  HEADER_TEXT: '#FFFFFF',
  LOCKED_BG: '#F0F9F9',
  LOCKED_TEXT: '#0D4A4D',
  INPUT_BG: '#FFFFFF',
  ALT_ROW_BG: '#F5F5F5',
  TOTAL_BG: '#E6F4EC',
  TOTAL_TEXT: '#0E7C3A',
  WARN_BG: '#FEF2F2',
  WARN_TEXT: '#B91C1C'
};

function trimSheet(sheet, keepRows, keepCols) {
  var maxRows = sheet.getMaxRows();
  var maxCols = sheet.getMaxColumns();
  if (maxRows > keepRows) sheet.deleteRows(keepRows + 1, maxRows - keepRows);
  if (maxCols > keepCols) sheet.deleteColumns(keepCols + 1, maxCols - keepCols);
}

function autoResizeAllColumns(sheet) {
  var lastCol = Math.max(sheet.getLastColumn(), 1);
  sheet.autoResizeColumns(1, lastCol);

  var minByCol = { 1: 200, 2: 160, 3: 180, 4: 130, 5: 220 };
  Object.keys(minByCol).forEach(function(col) {
    var c = Number(col);
    if (c <= lastCol && sheet.getColumnWidth(c) < minByCol[col]) {
      sheet.setColumnWidth(c, minByCol[col]);
    }
  });
}

function applySheetBanding_(sheet, theme, keepRows, keepCols) {
  var cols = Math.max(keepCols || sheet.getLastColumn(), 1);
  var rows = Math.max(keepRows || sheet.getLastRow(), 2);

  var header = sheet.getRange(1, 1, 1, cols);
  header.setBackground(theme.HEADER_BG).setFontColor(theme.HEADER_TEXT).setFontWeight('bold');
  sheet.setFrozenRows(1);
  sheet.setRowHeight(1, 32);

  var bodyRows = Math.max(rows - 1, 1);
  var body = sheet.getRange(2, 1, bodyRows, cols);
  body.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY);

  for (var r = 2; r <= rows; r++) {
    var bg = r % 2 === 0 ? theme.ROW_EVEN : theme.ROW_ODD;
    sheet.getRange(r, 1, 1, cols).setBackground(bg);
  }
}

function addTestData(sheet, articles, isInflow) {
  var list = Array.isArray(articles) ? articles.filter(Boolean) : [];
  if (!list.length) return;

  var testArticle = list[0];
  var now = new Date();
  var d1 = new Date(now.getFullYear(), now.getMonth(), 1);
  var d2 = new Date(now.getFullYear(), now.getMonth(), 15);

  var rows = [
    [d1, 'Тестовий ' + (isInflow ? 'клієнт А' : 'постачальник А'), testArticle, isInflow ? 10000 : 3000, 'Тестовий запис — видали перед використанням'],
    [d2, 'Тестовий ' + (isInflow ? 'клієнт Б' : 'постачальник Б'), testArticle, isInflow ? 5000 : 2000, 'Тестовий запис — видали перед використанням']
  ];

  sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
  sheet.getRange(2, 1, rows.length, rows[0].length)
    .setBackground('#FFF9C4')
    .setNote('Тестовий рядок — видали перед реальним використанням');
}

function buildInstructionSheet(ss, payload) {
  var sheet = ss.getSheetByName('📖 Інструкція');
  if (!sheet) sheet = ss.insertSheet('📖 Інструкція', 0);

  sheet.clear();
  sheet.getRange('A1').setValue((payload.business_name || 'Бізнес') + ' — Інструкція')
    .setFontSize(16)
    .setFontWeight('bold')
    .setBackground('#1A56DB')
    .setFontColor('#FFFFFF');

  var lines = [
    'ЯК ЧИТАТИ КОЛЬОРИ',
    '🔵 Синій заголовок — колонки введення даних',
    '🔴 Червоний заголовок — аркуші формул (не редагувати вручну)',
    '🟡 Жовті рядки — тестові дані, видали перед стартом',
    '',
    'ЯК ВНОСИТИ ДАНІ',
    '1. Заповнюй аркуші Надходження і Витрати',
    '2. Аркуш Cashflow перераховується автоматично',
    '3. Перед першим використанням видали жовті рядки 2-3',
    '',
    'ЯКЩО ЩОСЬ ЗЛАМАЛОСЬ',
    '#REF! — перевір назви аркушів і формули',
    '#NAME? — перевір назву функції або named range',
    'Нуль у Cashflow — перевір збіг назв статей у ввідних аркушах і довідниках'
  ];
  sheet.getRange(3, 1, lines.length, 1).setValues(lines.map(function(v) { return [v]; }));

  sheet.setRowHeight(1, 44);
  sheet.setColumnWidth(1, 520);
  sheet.protect().setWarningOnly(true).setDescription('Інструкція — краще не редагувати');
  trimSheet(sheet, 45, 3);
}

function checkFormulaErrors(sheet) {
  var range = sheet.getDataRange();
  var values = range.getDisplayValues();
  var formulas = range.getFormulas();
  var errors = [];
  for (var r = 0; r < values.length; r++) {
    for (var c = 0; c < values[r].length; c++) {
      if (!formulas[r][c]) continue;
      var cell = values[r][c];
      if (typeof cell === 'string' && (/^#REF!|^#ERROR!|^#NAME\?|^#VALUE!|^#DIV\/0!/).test(cell)) {
        errors.push({ sheet: sheet.getName(), cell: columnToLetter_(c + 1) + String(r + 1), value: cell });
      }
    }
  }
  return errors;
}

function columnToLetter_(col) {
  var temp = '';
  var letter = '';
  while (col > 0) {
    temp = (col - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    col = (col - temp - 1) / 26;
  }
  return letter;
}

function safeStringify_(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    return JSON.stringify({ message: 'stringify_failed', error: String(err && err.message || err) });
  }
}

function getDebugLogSpreadsheet_() {
  var props = PropertiesService.getScriptProperties();
  var existingId = props.getProperty(DEBUG_LOG_PROPERTY_KEY);

  if (existingId) {
    try {
      return SpreadsheetApp.openById(existingId);
    } catch (err) {
      props.deleteProperty(DEBUG_LOG_PROPERTY_KEY);
    }
  }

  var rootFolder = getOrCreateRootFolder_();
  var file = SpreadsheetApp.create(DEBUG_LOG_FILE_NAME);
  var spreadsheet = SpreadsheetApp.openById(file.getId());
  var driveFile = DriveApp.getFileById(file.getId());
  rootFolder.addFile(driveFile);
  DriveApp.getRootFolder().removeFile(driveFile);

  var sheet = spreadsheet.getSheets()[0];
  sheet.setName(DEBUG_LOG_SHEET_NAME);
  sheet.clear();
  sheet.getRange(1, 1, 1, 8).setValues([['timestamp', 'trace_id', 'level', 'action', 'step', 'message', 'details_json', 'user_key']]);
  sheet.setFrozenRows(1);

  props.setProperty(DEBUG_LOG_PROPERTY_KEY, spreadsheet.getId());
  return spreadsheet;
}

function appendDebugLog_(level, step, message, details) {
  try {
    var spreadsheet = getDebugLogSpreadsheet_();
    var sheet = spreadsheet.getSheetByName(DEBUG_LOG_SHEET_NAME) || spreadsheet.getSheets()[0];
    var userKey = '';
    if (details && typeof details === 'object') {
      userKey = String(details.telegram_id || details.spreadsheet_id || details.business_name || '');
    }

    sheet.appendRow([
      new Date(),
      CURRENT_TRACE_ID || '',
      String(level || 'INFO').toUpperCase(),
      CURRENT_ACTION || '',
      step || '',
      message || '',
      safeStringify_(details || {}),
      userKey
    ]);
  } catch (err) {
    Logger.log(JSON.stringify({
      trace_id: CURRENT_TRACE_ID || '',
      event: 'debug_log_write_failed',
      level: level || 'INFO',
      step: step || '',
      message: String(err && err.message || err)
    }));
  }
}

function logInfo_(step, message, details) {
  Logger.log(JSON.stringify({ trace_id: CURRENT_TRACE_ID || '', level: 'INFO', step: step, message: message, details: details || {} }));
  appendDebugLog_('INFO', step, message, details);
}

function logError_(step, message, details) {
  Logger.log(JSON.stringify({ trace_id: CURRENT_TRACE_ID || '', level: 'ERROR', step: step, message: message, details: details || {} }));
  appendDebugLog_('ERROR', step, message, details);
}

function doPost(e) {
  var traceId = 'as_' + new Date().getTime() + '_' + Math.floor(Math.random() * 1000000);
  try {
    var payload = JSON.parse((e && e.postData && e.postData.contents) || '{}');
    CURRENT_TRACE_ID = traceId;
    CURRENT_ACTION = String(payload.action || '');
    logInfo_('doPost.start', 'Apps Script request started', {
      action: payload.action || '',
      report_type: payload.report_type || '',
      telegram_id: payload.telegram_id || '',
      spreadsheet_id: payload.spreadsheet_id || '',
      changes_count: Array.isArray(payload.changes) ? payload.changes.length : 0
    });
    var output;

    switch (payload.action) {
      case 'ping':
        output = respond({ status: 'ok', message: 'pong', trace_id: traceId });
        break;
      case 'build_table':
        output = buildTable(payload);
        break;
      case 'update_table':
        output = updateTable(payload);
        break;
      case 'list_tables':
        output = listTables(payload);
        break;
      case 'validate_table':
        output = validateTableAction(payload);
        break;
      default:
        output = respond({ status: 'error', message: 'unknown action: ' + payload.action, trace_id: traceId });
        break;
    }

    try {
      var content = output && output.getContent ? output.getContent() : '';
      var parsed = content ? JSON.parse(content) : {};
      var logSpreadsheet = getDebugLogSpreadsheet_();
      parsed.trace_id = parsed.trace_id || traceId;
      parsed.log_sheet_url = parsed.log_sheet_url || logSpreadsheet.getUrl();

      logInfo_('doPost.end', 'Apps Script request finished', {
        action: payload.action || '',
        status: parsed.status || '',
        valid: parsed.valid,
        errors_count: (parsed.errors || []).length,
        warnings_count: (parsed.warnings || []).length,
        log_sheet_url: parsed.log_sheet_url
      });

      return respond(parsed);
    } catch (logErr) {
      logError_('doPost.end.parse_failed', 'Failed to parse action response for final logging', {
        error: String(logErr && logErr.message || logErr)
      });
    }

    return output;
  } catch (err) {
    CURRENT_TRACE_ID = traceId;
    logError_('doPost.error', 'Unhandled Apps Script error', {
      error: String(err && err.message || err),
      stack: String(err && err.stack || '')
    });
    return respond({ status: 'error', message: err.message, details: err.stack, trace_id: traceId, log_sheet_url: getDebugLogSpreadsheet_().getUrl() });
  }
}

function respond(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function buildTable(payload) {
  try {
    logInfo_('build_table.validate_payload', 'Validating build payload', {
      business_name: payload.business_name || '',
      report_type: payload.report_type || '',
      inflows: Array.isArray(payload.articles && payload.articles.inflows) ? payload.articles.inflows.length : 0,
      outflows: Array.isArray(payload.articles && payload.articles.outflows) ? payload.articles.outflows.length : 0
    });
    validatePayload(payload);

    var year = new Date().getFullYear();
    var businessName = sanitizeName(payload.business_name || 'Business');
    var reportType = String(payload.report_type || '').toLowerCase();

    var rootFolder = getOrCreateRootFolder_();
    var clientFolderName = buildClientFolderName_(payload);
    var clientFolder = getOrCreateFolder(rootFolder, clientFolderName);
    logInfo_('build_table.prepare_drive', 'Prepared client folder', {
      client_folder: clientFolderName,
      root_folder: ROOT_FOLDER_NAME
    });

    var baseFileName = titleByType_(reportType) + '_' + businessName + '_' + year;
    var resolvedFileName = resolveFileName(clientFolder, baseFileName);

    var ss = SpreadsheetApp.create(resolvedFileName);
    var file = DriveApp.getFileById(ss.getId());
    clientFolder.addFile(file);
    DriveApp.getRootFolder().removeFile(file);
    logInfo_('build_table.file_created', 'Spreadsheet file created', {
      spreadsheet_id: ss.getId(),
      file_name: resolvedFileName,
      spreadsheet_url: ss.getUrl()
    });

    var context = {
      payload: payload,
      spreadsheet: ss,
      reportType: reportType,
      clientFolder: clientFolder,
      file: file,
      forms: [],
      sheetsBuilt: []
    };

    if (reportType === 'cashflow') {
      logInfo_('build_table.build_cashflow', 'Building cashflow workbook', {});
      buildCashflow_(context);
    } else if (reportType === 'pl') {
      logInfo_('build_table.build_pl', 'Building P&L workbook', {});
      buildPl_(context);
    } else if (reportType === 'balance') {
      logInfo_('build_table.build_balance', 'Building balance workbook', {});
      buildBalance_(context);
    } else {
      logInfo_('build_table.build_dashboard', 'Building dashboard workbook', {});
      buildDashboard_(context);
    }

    buildInstructionSheet(ss, payload);
    logInfo_('build_table.instruction_sheet', 'Instruction sheet built', {});

    if (payload.options && payload.options.formatting) {
      applyWorkbookTheme_(ss);
      logInfo_('build_table.apply_theme', 'Workbook formatting applied', {});
    }

    setPermissions(clientFolder, file, payload.user_email, payload.share_mode || DEFAULT_SHARE_MODE);
    logInfo_('build_table.permissions', 'Permissions configured', {
      share_mode: payload.share_mode || DEFAULT_SHARE_MODE,
      user_email: payload.user_email || ''
    });

    var validation = validateBuiltFile(ss, payload);
    logInfo_('build_table.validation', 'Post-build validation finished', {
      valid: validation.valid,
      errors_count: Array.isArray(validation.errors) ? validation.errors.length : 0
    });
    var files = [{
      name: resolvedFileName,
      url: ss.getUrl(),
      spreadsheet_id: ss.getId()
    }];

    return respond({
      status: 'ok',
      folder_url: clientFolder.getUrl(),
      client_folder: clientFolderName,
      files: files,
      forms: context.forms,
      sheets_built: context.sheetsBuilt,
      validation: validation,
      trace_id: CURRENT_TRACE_ID || '',
      log_sheet_url: getDebugLogSpreadsheet_().getUrl()
    });
  } catch (err) {
    logError_('build_table.error', 'Build table failed', {
      error: err.message,
      stack: String(err && err.stack || '')
    });
    return respond({
      status: 'error',
      message: 'Не вдалося побудувати таблицю',
      details: err.message,
      trace_id: CURRENT_TRACE_ID || '',
      log_sheet_url: getDebugLogSpreadsheet_().getUrl()
    });
  }
}

function listTables(payload) {
  logInfo_('list_tables.start', 'Listing user tables', {
    telegram_id: payload && payload.telegram_id || '',
    telegram_username: payload && payload.telegram_username || ''
  });
  var rootFolder = getOrCreateRootFolder_();
  var clientFolderName = buildClientFolderName_(payload || {});
  var clientFolder = findFolderByName_(rootFolder, clientFolderName);

  if (!clientFolder) {
    return respond({
      status: 'ok',
      folder_exists: false,
      client_folder: clientFolderName,
      folder_url: null,
      tables: [],
      trace_id: CURRENT_TRACE_ID || '',
      log_sheet_url: getDebugLogSpreadsheet_().getUrl()
    });
  }

  var files = clientFolder.getFiles();
  var tables = [];

  while (files.hasNext()) {
    var file = files.next();
    if (file.getMimeType() !== MimeType.GOOGLE_SHEETS) {
      continue;
    }

    tables.push({
      name: file.getName(),
      url: file.getUrl(),
      spreadsheet_id: file.getId(),
      updated_at: file.getLastUpdated()
    });
  }

  tables.sort(function(a, b) {
    return new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime();
  });

  return respond({
    status: 'ok',
    folder_exists: true,
    client_folder: clientFolderName,
    folder_url: clientFolder.getUrl(),
    tables: tables,
    trace_id: CURRENT_TRACE_ID || '',
    log_sheet_url: getDebugLogSpreadsheet_().getUrl()
  });
}

function updateTable(payload) {
  if (!payload.spreadsheet_id) {
    return respond({ status: 'error', message: 'spreadsheet_id is required' });
  }

  var changes = Array.isArray(payload.changes) ? payload.changes : [];
  logInfo_('update_table.start', 'Updating spreadsheet', {
    spreadsheet_id: payload.spreadsheet_id,
    changes_count: changes.length
  });
  var ss = SpreadsheetApp.openById(payload.spreadsheet_id);
  var results = [];

  for (var i = 0; i < changes.length; i++) {
    var change = changes[i];
    try {
      switch (change.type) {
        case 'add_article':
          logInfo_('update_table.change', 'Applying add_article', change);
          results.push(addArticle_(ss, change));
          break;
        case 'remove_article':
          logInfo_('update_table.change', 'Applying remove_article', change);
          results.push(removeArticle_(ss, change));
          break;
        case 'rename_article':
          logInfo_('update_table.change', 'Applying rename_article', change);
          results.push(renameArticle_(ss, change));
          break;
        case 'add_sheet':
          logInfo_('update_table.change', 'Applying add_sheet', change);
          results.push(addSheet_(ss, change));
          break;
        case 'remove_sheet':
          logInfo_('update_table.change', 'Applying remove_sheet', change);
          results.push(removeSheet_(ss, change));
          break;
        case 'repair_formulas':
          logInfo_('update_table.change', 'Applying repair_formulas', change);
          results.push(repairFormulas_(ss));
          break;
        default:
          logInfo_('update_table.change', 'Unknown update change type', change);
          results.push({ type: change.type, status: 'unknown type' });
      }
    } catch (err) {
      logError_('update_table.change_error', 'Failed to apply change', {
        change: change,
        error: err.message
      });
      results.push({ type: change.type, status: 'error', message: err.message });
    }
  }

  return respond({ status: 'ok', changes_applied: results, trace_id: CURRENT_TRACE_ID || '', log_sheet_url: getDebugLogSpreadsheet_().getUrl() });
}

function validateTableAction(payload) {
  if (!payload.spreadsheet_id) {
    return respond({ status: 'error', message: 'spreadsheet_id is required' });
  }

  var ss = SpreadsheetApp.openById(payload.spreadsheet_id);
  logInfo_('validate_table.start', 'Running validation', {
    spreadsheet_id: payload.spreadsheet_id
  });
  var errors = [];
  var warnings = [];
  var names = ss.getSheets().map(function(s) { return s.getName(); });

  if (names.length === 0) {
    errors.push('Файл не містить аркушів');
  }

  var requiredByType = ['articles_inflows', 'articles_outflows'];
  var namedRanges = ss.getNamedRanges();
  requiredByType.forEach(function(name) {
    var nr = namedRanges.find(function(r) { return r.getName() === name; });
    if (!nr) {
      warnings.push('Відсутній named range: ' + name);
      return;
    }
    if (nr.getRange().isBlank()) {
      warnings.push('Named range порожній: ' + name);
    }
  });

  var mainSheet = ss.getSheetByName('📊 Cashflow') || ss.getSheetByName('📊 P&L') || ss.getSheets()[0];
  if (!mainSheet) {
    errors.push('Не знайдено зведений аркуш');
  } else {
    var dataRange = mainSheet.getDataRange();
    var formulas = dataRange.getFormulas().reduce(function(acc, row) { return acc.concat(row); }, []).filter(function(v) { return v !== ''; });
    if (formulas.length === 0) {
      errors.push('Зведений аркуш не містить формул');
    }

    var values2d = dataRange.getDisplayValues();
    var formulas2d = dataRange.getFormulas();
    var broken = [];
    for (var rr = 0; rr < values2d.length; rr++) {
      for (var cc = 0; cc < values2d[rr].length; cc++) {
        if (!formulas2d[rr][cc]) continue;
        var cellValue = values2d[rr][cc];
        if (typeof cellValue === 'string' && (/^#REF!|^#ERROR!|^#NAME\?/).test(cellValue)) {
          broken.push(cellValue);
        }
      }
    }
    if (broken.length > 0) {
      errors.push('Зламані формули: ' + broken.slice(0, 5).join(', '));
    }
  }

  ss.getSheets().forEach(function(sheet) {
    checkFormulaErrors(sheet).forEach(function(e) {
      errors.push(e.sheet + '!' + e.cell + ': ' + e.value);
    });
  });

  var inputSheet = ss.getSheetByName('⬇️ Надходження') || ss.getSheetByName('⬆️ Витрати');
  if (inputSheet && inputSheet.getLastRow() <= 1) {
    warnings.push('Аркуш введення порожній — тестові дані не додались');
  }

  if (ss.getSheets().length > 0 && ss.getSheets()[0].getName().indexOf('Інструкція') === -1) {
    warnings.push('Аркуш «Інструкція» не стоїть першим');
  }

  var protections = ss.getProtections(SpreadsheetApp.ProtectionType.SHEET);
  if (!protections || protections.length === 0) {
    warnings.push('Зведений аркуш не захищений від редагування');
  }

  return respond({
    status: 'ok',
    valid: errors.length === 0,
    errors: errors,
    warnings: warnings,
    sheets_found: names,
    trace_id: CURRENT_TRACE_ID || '',
    log_sheet_url: getDebugLogSpreadsheet_().getUrl()
  });
}

function validatePayload(payload) {
  var required = ['report_type', 'business_name', 'articles'];
  var missing = required.filter(function(key) { return !payload[key]; });

  if (missing.length > 0) {
    throw new Error('Відсутні обов\'язкові поля: ' + missing.join(', '));
  }

  var validTypes = ['cashflow', 'pl', 'balance', 'dashboard'];
  if (validTypes.indexOf(String(payload.report_type).toLowerCase()) === -1) {
    throw new Error('Невідомий тип таблиці: ' + payload.report_type);
  }
}

function buildCashflow_(ctx) {
  var ss = ctx.spreadsheet;
  var payload = ctx.payload;
  var articles = payload.articles || {};
  var responsible = payload.responsible || {};
  var options = payload.options || {};

  renameDefaultSheet_(ss, '📊 Cashflow');
  ctx.sheetsBuilt.push('📊 Cashflow');

  var inflowsMode = (payload.architecture && payload.architecture.inflows) || 'A';
  var outflowsMode = (payload.architecture && payload.architecture.outflows) || 'A';

  if (inflowsMode === 'A' || inflowsMode === 'B' || inflowsMode === 'C') {
    var inflowsSheet = ensureSheet_(ss, '⬇️ Надходження');
    setupInputSheet_(inflowsSheet, 'articles_inflows');
    addTestData(inflowsSheet, articles.inflows || [], true);
    ctx.sheetsBuilt.push('⬇️ Надходження');
  }

  if (outflowsMode === 'A' || outflowsMode === 'B' || outflowsMode === 'C') {
    var outflowsSheet = ensureSheet_(ss, '⬆️ Витрати');
    setupInputSheet_(outflowsSheet, 'articles_outflows');
    addTestData(outflowsSheet, articles.outflows || [], false);
    ctx.sheetsBuilt.push('⬆️ Витрати');
  }

  var extraExpenseSheets = [];
  var formRequired = false;
  var seenPeople = {};

  Object.keys(responsible).forEach(function(article) {
    var item = responsible[article] || {};
    if (item.input_mode === 'sheet' && item.name && !seenPeople[item.name]) {
      seenPeople[item.name] = true;
      var title = '⬆️ Витрати — ' + item.name;
      extraExpenseSheets.push(title);
      var personalSheet = ensureSheet_(ss, title);
      setupPersonalExpenseSheet_(personalSheet);
      ctx.sheetsBuilt.push(title);
    }

    if (item.input_mode === 'form') {
      formRequired = true;
    }
  });

  if (options.payment_calendar) {
    var calendar = ensureSheet_(ss, '📅 Платіжний календар');
    setupPaymentCalendar_(calendar);
    ctx.sheetsBuilt.push('📅 Платіжний календар');
  }

  if (formRequired) {
    var logSheet = ensureSheet_(ss, '📝 Лог');
    setupLogSheet_(logSheet);
    ctx.sheetsBuilt.push('📝 Лог');
  }

  var refs = ensureSheet_(ss, '🔗 References');
  setupReferencesSheet_(refs, ss.getUrl());
  ctx.sheetsBuilt.push('🔗 References');

  var settings = ensureSheet_(ss, '⚙️ Налаштування');
  setupSettingsSheet_(settings, payload.business_name);
  ctx.sheetsBuilt.push('⚙️ Налаштування');

  var directories = ensureSheet_(ss, '📋 Довідники');
  setupDirectories_(ss, directories, articles, responsible, options);
  ctx.sheetsBuilt.push('📋 Довідники');

  var cashflow = ss.getSheetByName('📊 Cashflow');
  setupCashflowSummary_(cashflow, articles, extraExpenseSheets, formRequired);
  protectSheet_(cashflow, 'Зведений аркуш Cashflow');
  cashflow.setFrozenColumns(1);

  // Input sheets: 200 rows, formula/service sheets: compact with запас.
  ['⬇️ Надходження', '⬆️ Витрати', '📝 Лог'].forEach(function(name) {
    var sh = ss.getSheetByName(name);
    if (sh) {
      autoResizeAllColumns(sh);
      trimSheet(sh, 200, Math.max(sh.getLastColumn() + 1, 6));
    }
  });

  ['📊 Cashflow', '📋 Довідники', '⚙️ Налаштування', '🔗 References'].forEach(function(name) {
    var sh = ss.getSheetByName(name);
    if (sh) {
      autoResizeAllColumns(sh);
      trimSheet(sh, Math.max(sh.getLastRow() + 5, 20), Math.max(sh.getLastColumn() + 1, 6));
    }
  });

  if (formRequired) {
    var createdForms = createFormsForResponsible_(payload, ss.getId());
    for (var i = 0; i < createdForms.length; i++) {
      ctx.forms.push(createdForms[i]);
    }
  }
}

function buildPl_(ctx) {
  var ss = ctx.spreadsheet;
  renameDefaultSheet_(ss, '📊 P&L');
  ensureSheet_(ss, '💰 Доходи');
  ensureSheet_(ss, '💸 Прямі витрати');
  ensureSheet_(ss, '💸 Операційні витрати');
  ensureSheet_(ss, '📋 Довідники');
  ensureSheet_(ss, '⚙️ Налаштування');
  ensureSheet_(ss, '🔗 References');

  ctx.sheetsBuilt = ['📊 P&L', '💰 Доходи', '💸 Прямі витрати', '💸 Операційні витрати', '📋 Довідники', '⚙️ Налаштування', '🔗 References'];
  var pl = ss.getSheetByName('📊 P&L');
  if (pl) pl.setFrozenColumns(1);
}

function buildBalance_(ctx) {
  var ss = ctx.spreadsheet;
  renameDefaultSheet_(ss, '📊 Баланс');
  ensureSheet_(ss, '📋 Довідники');
  ensureSheet_(ss, '⚙️ Налаштування');
  ensureSheet_(ss, '🔗 References');
  ctx.sheetsBuilt = ['📊 Баланс', '📋 Довідники', '⚙️ Налаштування', '🔗 References'];
  var balance = ss.getSheetByName('📊 Баланс');
  if (balance) balance.setFrozenColumns(1);
}

function buildDashboard_(ctx) {
  var ss = ctx.spreadsheet;
  renameDefaultSheet_(ss, '📊 Dashboard');
  var refs = ensureSheet_(ss, '🔗 References');
  setupReferencesSheet_(refs, '');
  var dashboard = ss.getSheetByName('📊 Dashboard');
  dashboard.getRange('A1').setValue('Dashboard').setFontWeight('bold').setFontSize(16);
  dashboard.getRange('A3').setValue('Cashflow import:');
  dashboard.getRange('B3').setFormula('=IFERROR(IMPORTRANGE(\'🔗 References\'!B1, "Cashflow!A1:D100"), "Надай доступ IMPORTRANGE")');

  ctx.sheetsBuilt = ['📊 Dashboard', '🔗 References'];
}

function setupInputSheet_(sheet, namedRangeName) {
  sheet.clear();
  sheet.getRange(1, 1, 1, 5).setValues([['Дата', 'Контрагент', 'Стаття', 'Сума', 'Коментар']]);
  sheet.setFrozenRows(1);
  sheet.getRange('A:A').setNumberFormat('dd.mm.yyyy');
  sheet.getRange('D:D').setNumberFormat('# ##0.00');

  var validation = SpreadsheetApp.newDataValidation()
    .requireFormulaSatisfied('=COUNTIF(' + namedRangeName + ',INDIRECT("RC",FALSE))>=0')
    .setAllowInvalid(true)
    .build();
  sheet.getRange(2, 3, 1000, 1).setDataValidation(validation);

  protectHeader_(sheet);
}

function setupPersonalExpenseSheet_(sheet) {
  sheet.clear();
  sheet.getRange(1, 1, 1, 4).setValues([['Дата', 'Що куплено', 'Стаття', 'Сума']]);
  sheet.setFrozenRows(1);
  sheet.getRange('A:A').setNumberFormat('dd.mm.yyyy');
  sheet.getRange('D:D').setNumberFormat('# ##0.00');
  sheet.getRange('F1').setValue('Підсумок');
  sheet.getRange('G1').setFormula('=SUM(D2:D)');
  sheet.getRange('F2').setValue('Залишок авансу');
  sheet.getRange('G2').setFormula('=\'' + SETTINGS_SHEET_NAME + '\'!B6-SUM(D2:D)');
  protectHeader_(sheet);
}

function setupPaymentCalendar_(sheet) {
  sheet.clear();
  sheet.getRange(1, 1, 1, 4).setValues([['Дата', 'Надходження план', 'Виплати план', 'Залишок']]);
  sheet.setFrozenRows(1);
  sheet.getRange('A:A').setNumberFormat('dd.mm.yyyy');

  for (var row = 2; row <= 40; row++) {
    if (row === 2) {
      sheet.getRange(row, 4).setFormula('=\'' + SETTINGS_SHEET_NAME + '\'!B4+B2-C2');
    } else {
      sheet.getRange(row, 4).setFormula('=D' + (row - 1) + '+B' + row + '-C' + row);
    }
  }

  var rule = SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied('=$D2<\'' + SETTINGS_SHEET_NAME + '\'!B5')
    .setBackground('#FEF2F2')
    .setRanges([sheet.getRange('A2:D1000')])
    .build();
  sheet.setConditionalFormatRules([rule]);
}

function setupLogSheet_(sheet) {
  sheet.clear();
  sheet.getRange(1, 1, 1, 5).setValues([['Дата', 'Контрагент / Опис', 'Стаття', 'Сума', 'Коментар']]);
  sheet.setFrozenRows(1);
}

function setupReferencesSheet_(sheet, cashflowUrl) {
  sheet.clear();
  sheet.getRange('A1').setValue('cashflow_url');
  sheet.getRange('B1').setValue(cashflowUrl || '');
  sheet.getRange('A2').setValue('pl_url');
  sheet.getRange('B2').setValue('');
  sheet.getRange('A3').setValue('balance_url');
  sheet.getRange('B3').setValue('');
}

function setupSettingsSheet_(sheet, businessName) {
  sheet.clear();
  sheet.getRange('A1:B8').setValues([
    ['Назва компанії', businessName || ''],
    ['Звітний рік', new Date().getFullYear()],
    ['Валюта', '₴'],
    ['Залишок на початок', 0],
    ['Поріг підсвітки', 0],
    ['Авансу Дмитро', 0],
    ['Оновлено', new Date()],
    ['Версія', '1.0']
  ]);
  sheet.getRange('B7').setNumberFormat('dd.mm.yyyy hh:mm');
}

function setupDirectories_(ss, sheet, articles, responsible, options) {
  sheet.clear();
  sheet.getRange('A1').setValue('Статті надходжень');
  sheet.getRange('C1').setValue('Статті витрат');
  sheet.getRange('E1').setValue('Відповідальні');
  sheet.getRange('G1').setValue('Контрагенти');

  var inflows = Array.isArray(articles.inflows) ? articles.inflows : [];
  var outflows = Array.isArray(articles.outflows) ? articles.outflows : [];
  var responsibleNames = uniqueValues_(Object.keys(responsible).map(function(article) {
    return (responsible[article] && responsible[article].name) || '';
  }).filter(Boolean));

  if (inflows.length) {
    sheet.getRange(2, 1, inflows.length, 1).setValues(inflows.map(function(v) { return [v]; }));
  }
  if (outflows.length) {
    sheet.getRange(2, 3, outflows.length, 1).setValues(outflows.map(function(v) { return [v]; }));
  }
  if (responsibleNames.length) {
    sheet.getRange(2, 5, responsibleNames.length, 1).setValues(responsibleNames.map(function(v) { return [v]; }));
  }

  if (options && options.counterparty_tracking) {
    sheet.getRange(2, 7).setValue('ТОВ Приклад');
  }

  createNamedRange(ss, sheet, 'articles_inflows', 2, 1, Math.max(inflows.length, 1));
  createNamedRange(ss, sheet, 'articles_outflows', 2, 3, Math.max(outflows.length, 1));
  createNamedRange(ss, sheet, 'responsible_list', 2, 5, Math.max(responsibleNames.length, 1));

  if (options && options.counterparty_tracking) {
    createNamedRange(ss, sheet, 'counterparties', 2, 7, 1);
  }
}

function setupCashflowSummary_(sheet, articles, extraExpenseSheets, includeLog) {
  sheet.clear();
  sheet.getRange(1, 1, 1, 4).setValues([['Стаття', 'Надходження', 'Витрати', 'Чистий Cashflow']]);

  var inflows = Array.isArray(articles.inflows) ? articles.inflows : [];
  var outflows = Array.isArray(articles.outflows) ? articles.outflows : [];
  var rows = [];

  for (var i = 0; i < inflows.length; i++) {
    rows.push([inflows[i], '', '', '']);
  }
  for (var j = 0; j < outflows.length; j++) {
    rows.push([outflows[j], '', '', '']);
  }

  if (rows.length) {
    sheet.getRange(2, 1, rows.length, 4).setValues(rows);
  }

  for (var r = 2; r < 2 + rows.length; r++) {
    var inflowFormula = '=SUMIF(\'⬇️ Надходження\'!C:C,A' + r + ',\'⬇️ Надходження\'!D:D)';

    var outflowFormula = '=SUMIF(\'⬆️ Витрати\'!C:C,A' + r + ',\'⬆️ Витрати\'!D:D)';
    for (var s = 0; s < extraExpenseSheets.length; s++) {
      outflowFormula += '+SUMIF(\'' + extraExpenseSheets[s] + '\'!C:C,A' + r + ',\'' + extraExpenseSheets[s] + '\'!D:D)';
    }
    if (includeLog) {
      outflowFormula += '+SUMIF(\'📝 Лог\'!C:C,A' + r + ',\'📝 Лог\'!D:D)';
    }

    sheet.getRange(r, 2).setFormula(inflowFormula);
    sheet.getRange(r, 3).setFormula(outflowFormula);
    sheet.getRange(r, 4).setFormula('=B' + r + '-C' + r);
  }

  var totalRow = 2 + rows.length + 1;
  sheet.getRange(totalRow, 1, 4, 2).setValues([
    ['Загальні надходження', '=SUM(B2:B' + (totalRow - 2) + ')'],
    ['Загальні виплати', '=SUM(C2:C' + (totalRow - 2) + ')'],
    ['Чистий Cashflow', '=B' + totalRow + '-B' + (totalRow + 1)],
    ['Залишок', '=\'' + SETTINGS_SHEET_NAME + '\'!B4+B' + (totalRow + 2)]
  ]);

  var rules = sheet.getConditionalFormatRules();
  rules.push(SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied('=$B' + (totalRow + 3) + '<0')
    .setBackground('#FEF2F2')
    .setRanges([sheet.getRange(totalRow + 3, 1, 1, 2)])
    .build());
  rules.push(SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied('=$B' + (totalRow + 3) + '>=0')
    .setBackground('#E6F4EC')
    .setRanges([sheet.getRange(totalRow + 3, 1, 1, 2)])
    .build());
  sheet.setConditionalFormatRules(rules);
  sheet.setFrozenRows(1);
}

function createFormsForResponsible_(payload, spreadsheetId) {
  var responsible = payload.responsible || {};
  var outflows = (payload.articles && payload.articles.outflows) || [];
  var forms = [];
  var created = {};

  Object.keys(responsible).forEach(function(article) {
    var item = responsible[article] || {};
    if (item.input_mode !== 'form' || !item.name || created[item.name]) {
      return;
    }

    created[item.name] = true;
    var formTitle = 'Витрати — ' + item.name + ' | ' + payload.business_name;
    var form = FormApp.create(formTitle);
    form.setDescription('Форма для фіксації витрат');

    form.addDateItem().setTitle('Дата').setRequired(true);
    form.addTextItem().setTitle('Контрагент / Опис').setRequired(true);
    form.addListItem().setTitle('Стаття витрат').setChoiceValues(outflows).setRequired(true);
    form.addTextItem().setTitle('Сума (грн)').setRequired(true);
    form.addTextItem().setTitle('Коментар');

    form.setDestination(FormApp.DestinationType.SPREADSHEET, spreadsheetId);

    forms.push({
      name: 'Витрати — ' + item.name,
      url: form.getPublishedUrl()
    });
  });

  var ss = SpreadsheetApp.openById(spreadsheetId);
  var autoResponseSheet = ss.getSheets().find(function(s) {
    return /^Відповіді форми|^Form Responses/i.test(s.getName());
  });
  if (autoResponseSheet) {
    autoResponseSheet.setName('📝 Лог');
  }

  return forms;
}

function validateBuiltFile(spreadsheet, payload) {
  var errors = [];
  var reportType = String(payload.report_type || '').toLowerCase();
  var names = spreadsheet.getSheets().map(function(s) { return s.getName(); });

  var required = getRequiredSheets_(payload, reportType);
  required.forEach(function(name) {
    if (names.indexOf(name) === -1) {
      errors.push('Відсутній аркуш: ' + name);
    }
  });

  var namedRanges = spreadsheet.getNamedRanges().map(function(nr) { return nr.getName(); });
  if (reportType === 'cashflow') {
    ['articles_inflows', 'articles_outflows'].forEach(function(name) {
      if (namedRanges.indexOf(name) === -1) {
        errors.push('Відсутній named range: ' + name);
      }
    });
  }

  if (reportType === 'cashflow') {
    var mainSheet = spreadsheet.getSheetByName('📊 Cashflow') || spreadsheet.getSheets()[0];
    var formulas = mainSheet.getDataRange().getFormulas().reduce(function(acc, row) {
      return acc.concat(row);
    }, []).filter(function(f) { return f !== ''; });
    if (formulas.length === 0) {
      errors.push('Зведений аркуш не містить формул');
    }
  }

  return { valid: errors.length === 0, errors: errors };
}

function setPermissions(folder, file, userEmail, shareMode) {
  if (userEmail) {
    folder.addViewer(userEmail);
    file.addEditor(userEmail);
  }

  if (shareMode === 'anyone_with_link') {
    folder.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.VIEW);
  }
}

function createNamedRange(ss, sheet, rangeName, startRow, col, numRows) {
  var range = sheet.getRange(startRow, col, numRows, 1);
  var existing = ss.getNamedRanges().find(function(nr) { return nr.getName() === rangeName; });
  if (existing) {
    existing.remove();
  }
  ss.setNamedRange(rangeName, range);
}

function protectSheet_(sheet, description) {
  var protection = sheet.protect();
  protection.setDescription(description || 'Protected');
  protection.setWarningOnly(false);
  var me = Session.getEffectiveUser();
  protection.addEditor(me);
  var editors = protection.getEditors();
  for (var i = 0; i < editors.length; i++) {
    if (editors[i].getEmail() !== me.getEmail()) {
      protection.removeEditor(editors[i]);
    }
  }
}

function protectHeader_(sheet) {
  var protection = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), 5)).protect();
  protection.setDescription('Protect headers');
  protection.setWarningOnly(false);
}

function applyWorkbookTheme_(ss) {
  var sheets = ss.getSheets();
  var inputNames = {
    '⬇️ Надходження': true,
    '⬆️ Витрати': true,
    '📝 Лог': true
  };
  var formulaNames = {
    '📊 Cashflow': true,
    '📊 P&L': true,
    '📊 Баланс': true,
    '📊 Dashboard': true
  };

  for (var i = 0; i < sheets.length; i++) {
    var sheet = sheets[i];
    var maxCols = Math.max(sheet.getLastColumn(), 5);
    var isInput = !!inputNames[sheet.getName()];
    var isFormula = !!formulaNames[sheet.getName()];
    var workingRows = isInput ? 200 : Math.max(sheet.getLastRow() + 3, 20);

    applySheetBanding_(sheet, isFormula ? FORMULA_THEME : INPUT_THEME, workingRows, maxCols);

    var lastCol = sheet.getLastColumn();
    var lastRow = sheet.getLastRow();
    if (lastCol > 0 && lastRow > 2) {
      var values = sheet.getRange(1, 1, lastRow, lastCol).getDisplayValues();
      for (var r = 1; r <= lastRow; r++) {
        var firstCell = String(values[r - 1][0] || '').toLowerCase();
        if (firstCell.indexOf('загальні') >= 0 || firstCell.indexOf('чистий') >= 0 || firstCell.indexOf('залишок') >= 0) {
          var totalRow = sheet.getRange(r, 1, 1, lastCol);
          totalRow.setBackground(THEME.TOTAL_BG);
          totalRow.setFontColor(THEME.TOTAL_TEXT);
          totalRow.setFontWeight('bold');
          totalRow.setBorder(true, null, null, null, null, null, '#0E7C3A', SpreadsheetApp.BorderStyle.SOLID_MEDIUM);
        }
      }
    }
  }
}

function addArticle_(ss, change) {
  var sheet = ss.getSheetByName('📋 Довідники') || ss.getSheetByName('Довідники');
  if (!sheet) {
    throw new Error('Не знайдено аркуш Довідники');
  }

  var col = change.section === 'inflows' ? 1 : 3;
  var lastRow = Math.max(sheet.getLastRow(), 1);
  sheet.getRange(lastRow + 1, col).setValue(change.article);

  var rangeName = change.section === 'inflows' ? 'articles_inflows' : 'articles_outflows';
  createNamedRange(ss, sheet, rangeName, 2, col, Math.max(lastRow, 1));

  return { type: 'add_article', status: 'ok', article: change.article };
}

function repairFormulas_(ss) {
  var sheets = ss.getSheets();
  var fixed = 0;

  for (var i = 0; i < sheets.length; i++) {
    var sheet = sheets[i];
    var dataRange = sheet.getDataRange();
    if (!dataRange) continue;

    var formulas = dataRange.getFormulas();
    var changed = false;

    for (var r = 0; r < formulas.length; r++) {
      for (var c = 0; c < formulas[r].length; c++) {
        var formula = formulas[r][c];
        if (!formula) continue;

        var next = formula.replace(/(^|[^A-Za-z0-9_'])Settings!/g, "$1'" + SETTINGS_SHEET_NAME + "'!");
        if (next !== formula) {
          formulas[r][c] = next;
          changed = true;
          fixed++;
        }
      }
    }

    if (changed) {
      dataRange.setFormulas(formulas);
    }
  }

  return { type: 'repair_formulas', status: 'ok', fixed_formulas: fixed };
}

function removeArticle_(ss, change) {
  var sheet = ss.getSheetByName('📋 Довідники') || ss.getSheetByName('Довідники');
  var col = change.section === 'inflows' ? 1 : 3;
  var values = sheet.getRange(2, col, Math.max(sheet.getLastRow() - 1, 1), 1).getValues();

  for (var i = values.length - 1; i >= 0; i--) {
    if (String(values[i][0]) === String(change.article)) {
      sheet.deleteRow(i + 2);
      break;
    }
  }

  return { type: 'remove_article', status: 'ok', article: change.article };
}

function renameArticle_(ss, change) {
  var sheet = ss.getSheetByName('📋 Довідники') || ss.getSheetByName('Довідники');
  var col = change.section === 'inflows' ? 1 : 3;
  var values = sheet.getRange(2, col, Math.max(sheet.getLastRow() - 1, 1), 1).getValues();

  for (var i = 0; i < values.length; i++) {
    if (String(values[i][0]) === String(change.from)) {
      sheet.getRange(i + 2, col).setValue(change.to);
      return { type: 'rename_article', status: 'ok', from: change.from, to: change.to };
    }
  }

  return { type: 'rename_article', status: 'skipped', message: 'article not found' };
}

function addSheet_(ss, change) {
  if (!change.name) {
    throw new Error('Sheet name is required');
  }
  if (ss.getSheetByName(change.name)) {
    return { type: 'add_sheet', status: 'skipped', message: 'already exists' };
  }
  ss.insertSheet(change.name);
  return { type: 'add_sheet', status: 'ok', name: change.name };
}

function removeSheet_(ss, change) {
  var sheet = ss.getSheetByName(change.name || '');
  if (!sheet) {
    return { type: 'remove_sheet', status: 'skipped', message: 'not found' };
  }
  ss.deleteSheet(sheet);
  return { type: 'remove_sheet', status: 'ok', name: change.name };
}

function getRequiredSheets_(payload, reportType) {
  if (reportType === 'cashflow') {
    var list = ['📊 Cashflow', '⬇️ Надходження', '⬆️ Витрати', '📋 Довідники', '⚙️ Налаштування', '🔗 References'];
    if (payload.options && payload.options.payment_calendar) {
      list.push('📅 Платіжний календар');
    }
    var responsible = payload.responsible || {};
    Object.keys(responsible).forEach(function(article) {
      var item = responsible[article] || {};
      if (item.input_mode === 'form') {
        list.push('📝 Лог');
      }
      if (item.input_mode === 'sheet') {
        list.push('⬆️ Витрати — ' + item.name);
      }
    });
    return uniqueValues_(list);
  }

  if (reportType === 'pl') {
    return ['📊 P&L', '💰 Доходи', '💸 Прямі витрати', '💸 Операційні витрати', '📋 Довідники', '⚙️ Налаштування', '🔗 References'];
  }

  if (reportType === 'balance') {
    return ['📊 Баланс', '📋 Довідники', '⚙️ Налаштування', '🔗 References'];
  }

  return ['📊 Dashboard', '🔗 References'];
}

function getOrCreateRootFolder_() {
  var folders = DriveApp.getFoldersByName(ROOT_FOLDER_NAME);
  if (folders.hasNext()) {
    return folders.next();
  }
  return DriveApp.createFolder(ROOT_FOLDER_NAME);
}

function getOrCreateFolder(parentFolder, name) {
  var existing = parentFolder.getFoldersByName(name);
  if (existing.hasNext()) {
    return existing.next();
  }
  return parentFolder.createFolder(name);
}

function findFolderByName_(parentFolder, name) {
  var existing = parentFolder.getFoldersByName(name);
  if (existing.hasNext()) {
    return existing.next();
  }
  return null;
}

function buildClientFolderName_(payload) {
  var username = sanitizeTelegramUsername_(payload.telegram_username || '');
  if (username) {
    return 'client_tg_' + username;
  }

  var tgId = String(payload.telegram_id || '').trim();
  if (tgId) {
    return 'client_tg_id_' + tgId;
  }

  return 'client_tg_unknown';
}

function sanitizeTelegramUsername_(value) {
  return String(value || '')
    .trim()
    .replace(/^@+/, '')
    .replace(/[^a-zA-Z0-9_]/g, '')
    .toLowerCase();
}

function resolveFileName(folder, baseName) {
  var name = baseName;
  var version = 2;
  while (folder.getFilesByName(name + '.xlsx').hasNext() || folder.getFilesByName(name).hasNext()) {
    name = baseName + '_v' + version;
    version++;
  }
  return name;
}

function ensureSheet_(ss, title) {
  var sheet = ss.getSheetByName(title);
  if (sheet) {
    return sheet;
  }
  return ss.insertSheet(title);
}

function renameDefaultSheet_(ss, title) {
  var firstSheet = ss.getSheets()[0];
  firstSheet.setName(title);
  return firstSheet;
}

function titleByType_(reportType) {
  if (reportType === 'cashflow') return 'Cashflow';
  if (reportType === 'pl') return 'P&L';
  if (reportType === 'balance') return 'Баланс';
  return 'Dashboard';
}

function reportFolderName_(reportType) {
  if (reportType === 'cashflow') return 'Cashflow';
  if (reportType === 'pl') return 'P&L';
  if (reportType === 'balance') return 'Баланс';
  return 'Дашборд';
}

function sanitizeName(value) {
  return String(value || 'Business')
    .trim()
    .replace(/[\\/:*?"<>|]/g, '_')
    .replace(/\s+/g, ' ')
    .substring(0, 90);
}

function uniqueValues_(arr) {
  var map = {};
  var out = [];
  arr.forEach(function(item) {
    var key = String(item);
    if (!map[key]) {
      map[key] = true;
      out.push(item);
    }
  });
  return out;
}

function testBuild() {
  var testPayload = {
    action: 'build_table',
    report_type: 'cashflow',
    business_name: 'Тест Компанія',
    language: 'uk',
    user_email: 'your@gmail.com',
    architecture: { inflows: 'A', outflows: 'B' },
    articles: {
      inflows: ['Оплата від клієнтів', 'Передоплати'],
      outflows: ['Зарплати', 'Підрядники', 'Оренда']
    },
    responsible: {
      'Оплата від клієнтів': { name: 'Марина', access: true, input_mode: 'direct' },
      'Зарплати': { name: 'Наталія', access: true, input_mode: 'direct' },
      'Підрядники': { name: 'Дмитро', access: false, input_mode: 'sheet', payment: 'accountable' }
    },
    options: {
      payment_calendar: true,
      multi_account: false,
      counterparty_tracking: true
    }
  };

  var fakeEvent = { postData: { contents: JSON.stringify(testPayload) } };
  var result = doPost(fakeEvent);
  Logger.log(result.getContent());
}
