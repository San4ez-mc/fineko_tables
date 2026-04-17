var ROOT_FOLDER_NAME = 'Фінансова система — Курс';
var DEFAULT_SHARE_MODE = 'anyone_with_link';
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

function doPost(e) {
  try {
    var payload = JSON.parse((e && e.postData && e.postData.contents) || '{}');

    switch (payload.action) {
      case 'ping':
        return respond({ status: 'ok', message: 'pong' });
      case 'build_table':
        return buildTable(payload);
      case 'update_table':
        return updateTable(payload);
      case 'list_tables':
        return listTables(payload);
      case 'validate_table':
        return validateTableAction(payload);
      default:
        return respond({ status: 'error', message: 'unknown action: ' + payload.action });
    }
  } catch (err) {
    return respond({ status: 'error', message: err.message, details: err.stack });
  }
}

function respond(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function buildTable(payload) {
  try {
    validatePayload(payload);

    var year = new Date().getFullYear();
    var businessName = sanitizeName(payload.business_name || 'Business');
    var reportType = String(payload.report_type || '').toLowerCase();

    var rootFolder = getOrCreateRootFolder_();
    var clientFolderName = buildClientFolderName_(payload);
    var clientFolder = getOrCreateFolder(rootFolder, clientFolderName);

    var baseFileName = titleByType_(reportType) + '_' + businessName + '_' + year;
    var resolvedFileName = resolveFileName(clientFolder, baseFileName);

    var ss = SpreadsheetApp.create(resolvedFileName);
    var file = DriveApp.getFileById(ss.getId());
    clientFolder.addFile(file);
    DriveApp.getRootFolder().removeFile(file);

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
      buildCashflow_(context);
    } else if (reportType === 'pl') {
      buildPl_(context);
    } else if (reportType === 'balance') {
      buildBalance_(context);
    } else {
      buildDashboard_(context);
    }

    if (payload.options && payload.options.formatting) {
      applyWorkbookTheme_(ss);
    }

    setPermissions(clientFolder, file, payload.user_email, payload.share_mode || DEFAULT_SHARE_MODE);

    var validation = validateBuiltFile(ss, payload);
    var files = [{
      name: resolvedFileName,
      url: ss.getUrl(),
      spreadsheet_id: ss.getId()
    }];

    return respond({
      status: validation.valid ? 'ok' : 'error',
      folder_url: clientFolder.getUrl(),
      client_folder: clientFolderName,
      files: files,
      forms: context.forms,
      sheets_built: context.sheetsBuilt,
      validation: validation
    });
  } catch (err) {
    return respond({
      status: 'error',
      message: 'Не вдалося побудувати таблицю',
      details: err.message
    });
  }
}

function listTables(payload) {
  var rootFolder = getOrCreateRootFolder_();
  var clientFolderName = buildClientFolderName_(payload || {});
  var clientFolder = findFolderByName_(rootFolder, clientFolderName);

  if (!clientFolder) {
    return respond({
      status: 'ok',
      folder_exists: false,
      client_folder: clientFolderName,
      folder_url: null,
      tables: []
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
    tables: tables
  });
}

function updateTable(payload) {
  if (!payload.spreadsheet_id) {
    return respond({ status: 'error', message: 'spreadsheet_id is required' });
  }

  var changes = Array.isArray(payload.changes) ? payload.changes : [];
  var ss = SpreadsheetApp.openById(payload.spreadsheet_id);
  var results = [];

  for (var i = 0; i < changes.length; i++) {
    var change = changes[i];
    try {
      switch (change.type) {
        case 'add_article':
          results.push(addArticle_(ss, change));
          break;
        case 'remove_article':
          results.push(removeArticle_(ss, change));
          break;
        case 'rename_article':
          results.push(renameArticle_(ss, change));
          break;
        case 'add_sheet':
          results.push(addSheet_(ss, change));
          break;
        case 'remove_sheet':
          results.push(removeSheet_(ss, change));
          break;
        default:
          results.push({ type: change.type, status: 'unknown type' });
      }
    } catch (err) {
      results.push({ type: change.type, status: 'error', message: err.message });
    }
  }

  return respond({ status: 'ok', changes_applied: results });
}

function validateTableAction(payload) {
  if (!payload.spreadsheet_id) {
    return respond({ status: 'error', message: 'spreadsheet_id is required' });
  }

  var ss = SpreadsheetApp.openById(payload.spreadsheet_id);
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

    var values = dataRange.getDisplayValues().reduce(function(acc, row) { return acc.concat(row); }, []);
    var broken = values.filter(function(v) {
      return typeof v === 'string' && (/^#REF!|^#ERROR!|^#NAME\?/).test(v);
    });
    if (broken.length > 0) {
      errors.push('Зламані формули: ' + broken.slice(0, 5).join(', '));
    }
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
    sheets_found: names
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
    ctx.sheetsBuilt.push('⬇️ Надходження');
  }

  if (outflowsMode === 'A' || outflowsMode === 'B' || outflowsMode === 'C') {
    var outflowsSheet = ensureSheet_(ss, '⬆️ Витрати');
    setupInputSheet_(outflowsSheet, 'articles_outflows');
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
}

function buildBalance_(ctx) {
  var ss = ctx.spreadsheet;
  renameDefaultSheet_(ss, '📊 Баланс');
  ensureSheet_(ss, '📋 Довідники');
  ensureSheet_(ss, '⚙️ Налаштування');
  ensureSheet_(ss, '🔗 References');
  ctx.sheetsBuilt = ['📊 Баланс', '📋 Довідники', '⚙️ Налаштування', '🔗 References'];
}

function buildDashboard_(ctx) {
  var ss = ctx.spreadsheet;
  renameDefaultSheet_(ss, '📊 Dashboard');
  var refs = ensureSheet_(ss, '🔗 References');
  setupReferencesSheet_(refs, '');
  var dashboard = ss.getSheetByName('📊 Dashboard');
  dashboard.getRange('A1').setValue('Dashboard').setFontWeight('bold').setFontSize(16);
  dashboard.getRange('A3').setValue('Cashflow import:');
  dashboard.getRange('B3').setFormula('=IFERROR(IMPORTRANGE(References!B1, "Cashflow!A1:D100"), "Надай доступ IMPORTRANGE")');

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
  sheet.getRange('G2').setFormula('=Settings!B6-SUM(D2:D)');
  protectHeader_(sheet);
}

function setupPaymentCalendar_(sheet) {
  sheet.clear();
  sheet.getRange(1, 1, 1, 4).setValues([['Дата', 'Надходження план', 'Виплати план', 'Залишок']]);
  sheet.setFrozenRows(1);
  sheet.getRange('A:A').setNumberFormat('dd.mm.yyyy');

  for (var row = 2; row <= 40; row++) {
    if (row === 2) {
      sheet.getRange(row, 4).setFormula('=Settings!B4+B2-C2');
    } else {
      sheet.getRange(row, 4).setFormula('=D' + (row - 1) + '+B' + row + '-C' + row);
    }
  }

  var rule = SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied('=$D2<Settings!B5')
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
    ['Залишок', '=Settings!B4+B' + (totalRow + 2)]
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

  var mainSheet = spreadsheet.getSheetByName('📊 Cashflow') || spreadsheet.getSheetByName('📊 P&L') || spreadsheet.getSheets()[0];
  var formulas = mainSheet.getDataRange().getFormulas().reduce(function(acc, row) {
    return acc.concat(row);
  }, []).filter(function(f) { return f !== ''; });
  if (formulas.length === 0) {
    errors.push('Зведений аркуш не містить формул');
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

  for (var i = 0; i < sheets.length; i++) {
    var sheet = sheets[i];
    var maxCols = Math.max(sheet.getLastColumn(), 5);
    var maxRows = Math.max(sheet.getLastRow(), 2);

    var header = sheet.getRange(1, 1, 1, maxCols);
    header.setBackground(THEME.HEADER_BG);
    header.setFontColor(THEME.HEADER_TEXT);
    header.setFontWeight('bold');
    sheet.setFrozenRows(1);
    sheet.setRowHeight(1, 32);

    if (maxRows > 1) {
      var inputRange = sheet.getRange(2, 1, maxRows - 1, maxCols);
      inputRange.setBackground(THEME.INPUT_BG);

      for (var row = 2; row <= maxRows; row++) {
        if (row % 2 === 0) {
          sheet.getRange(row, 1, 1, maxCols).setBackground(THEME.ALT_ROW_BG);
        }
      }
    }

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
