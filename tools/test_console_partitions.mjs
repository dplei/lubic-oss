import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import vm from 'node:vm';

const html = readFileSync(new URL('../crates/controller/assets/console.html', import.meta.url), 'utf8');
const types = [{ id: 1, name: '游戏', children: [{ id: 900, name: '新分区' }] }];

function consolePage(api) {
  const nodes = new Map();
  const $ = selector => {
    if (!nodes.has(selector)) nodes.set(selector, { hidden: true, setAttribute() {}, querySelectorAll: () => [] });
    return nodes.get(selector);
  };
  const context = vm.createContext({
    $, api: async (...args) => {
      const data = await api(...args);
      return Array.isArray(data) ? { type_mode: 0, typelist: data, human_type_list: [] } : data;
    }, structuredClone, accounts: [{ id: 1, name: '账号' }], templates: [], rooms: [],
    esc: s => String(s), accName: () => '账号', splitTags: () => [], renderTags() {},
    setCopyright() {}, renderTitle() {}, tTitle: {},
    mountSelect: (el, opts, value, pick) => Object.assign(el, { opts, value, pick }),
    fail: (el, e) => { el.hidden = false; el.textContent = e.message; },
  });
  vm.runInContext(
    html.slice(html.indexOf('// —— 模板 ——'), html.indexOf('let tpl = null')) +
    '\nlet tpl = null, tags = [];\n' +
    html.slice(html.indexOf('// 分区要问 B 站'), html.indexOf("$('#t-save').onclick")), context);
  return { $, context, run: code => vm.runInContext(code, context) };
}

test('新模板从接口选择默认分区，加载期间不能保存', async () => {
  let resolve;
  const page = consolePage(() => new Promise(r => { resolve = r; }));
  page.run('openTemplate("new")');
  assert.equal(page.run('tpl.tid'), null);
  assert.equal(page.$('#t-save').disabled, true);
  resolve(types);
  await page.run('loadAccountLists()');
  assert.equal(page.run('tpl.tid'), 900);
  assert.equal(page.$('#t-save').disabled, false);
});

test('已有模板保留接口中不存在的分区', async () => {
  const page = consolePage(async () => types);
  page.context.templates = [{ id: 2, account_id: 1, tid: 171 }];
  page.run('openTemplate("2")');
  await page.run('loadAccountLists()');
  assert.equal(page.run('tpl.tid'), 171);
  assert.equal(page.$('[data-select="tid"]').opts[0].v, 171);
  assert.equal(page.$('#t-save').disabled, false);
});

test('空列表或接口失败时，新模板不能保存，已有模板仍能保存', async () => {
  for (const response of [[], null]) {
    const page = consolePage(async () => {
      if (response === null) throw new Error('接口失败');
      return response;
    });
    page.run('openTemplate("new")');
    await page.run('loadAccountLists()');
    assert.equal(page.$('#t-save').disabled, true);
    assert.equal(page.$('#t-err').hidden, false);
    page.context.templates = [{ id: 2, account_id: 1, tid: 171 }];
    page.run('openTemplate("2")');
    await page.run('loadAccountLists()');
    assert.equal(page.run('tpl.tid'), 171);
    assert.equal(page.$('#t-save').disabled, false);
  }
});

test('模板列表使用各自账号的分区名，同账号请求复用', async () => {
  const calls = [];
  const page = consolePage(async (_, path) => {
    calls.push(path);
    return [{ id: 1, name: '父分区', children: [{ id: 900, name: path.includes('/1/') ? '甲' : '乙' }] }];
  });
  page.context.templates = [
    { id: 1, account_id: 1, tid: 900 }, { id: 2, account_id: 2, tid: 900 },
    { id: 3, account_id: 1, tid: 900 },
  ];
  await page.run('renderTemplates()');
  assert.equal(page.$('[data-template-tid="1"]').textContent, '900（甲）');
  assert.equal(page.$('[data-template-tid="2"]').textContent, '900（乙）');
  assert.equal(page.$('[data-template-tid="1"]').title, '父分区 / 甲');
  assert.equal(calls.length, 2);
});

test('切换账号后，迟到的响应不能覆盖当前编辑', async () => {
  const pending = new Map();
  const page = consolePage((_, path) => new Promise(resolve => pending.set(path, resolve)));
  page.run('openTemplate("new")');
  const first = page.run('loadAccountLists()');
  page.run('tpl.account_id = 2');
  const second = page.run('loadAccountLists()');
  pending.get('/api/accounts/2/types')([{ id: 902, name: '第二账号' }]);
  await second;
  pending.get('/api/accounts/1/types')(types);
  await first;
  assert.equal(page.run('tpl.tid'), 902);
  assert.equal(page.$('[data-select="tid"]').value, 902);
});

test('同账号切换模板时，旧响应不覆盖新模板；缓存不混入旧分区', async () => {
  let resolve;
  const page = consolePage(() => new Promise(r => { resolve = r; }));
  page.context.templates = [{ id: 2, account_id: 1, tid: 171 }];
  page.run('openTemplate("2")');
  const old = page.run('loadAccountLists()');
  page.run('openTemplate("new")');
  const current = page.run('loadAccountLists()');
  resolve(types);
  await Promise.all([old, current]);
  assert.equal(page.run('tpl.tid'), 900);
  assert.equal(page.$('[data-select="tid"]').opts.some(o => o.v === 171), false);
});

test('没有账号时不请求接口；失败后再次打开可以重试', async () => {
  let calls = 0;
  const page = consolePage(async () => {
    if (++calls === 1) throw new Error('接口失败');
    return types;
  });
  page.context.accounts = [];
  page.run('openTemplate("new")');
  await page.run('loadAccountLists()');
  assert.equal(calls, 0);
  assert.equal(page.$('#t-save').disabled, true);
  page.context.accounts = [{ id: 1 }];
  page.run('openTemplate("new")');
  await page.run('loadAccountLists()');
  assert.equal(page.$('#t-save').disabled, true);
  page.run('openTemplate("new")');
  await page.run('loadAccountLists()');
  assert.equal(page.run('tpl.tid'), 900);
  assert.equal(page.$('#t-save').disabled, false);
});

const modern = { type_mode: 2, typelist: types, human_type_list: [{ id: 1008, name: '游戏' }, { id: 1011, name: '人工智能' }] };

test('新版账号新模板选择 human_type2，不把新 ID 写进旧 tid', async () => {
  const page = consolePage(async () => modern);
  page.run('openTemplate("new")');
  await page.run('loadAccountLists()');
  assert.equal(page.run('tpl.human_type2'), 1008);
  assert.equal(page.run('tpl.tid'), 0);
  page.$('[data-select="tid"]').pick(1011);
  assert.equal(page.run('tpl.human_type2'), 1011);
  assert.equal(page.run('tpl.tid'), 0);
});

test('新版账号打开旧模板保留 tid，只有明确选择才设置新版分区', async () => {
  const page = consolePage(async () => modern);
  page.context.templates = [{ id: 2, account_id: 1, tid: 900, human_type2: null }];
  page.run('openTemplate("2")');
  await page.run('loadAccountLists()');
  const select = page.$('[data-select="tid"]');
  assert.equal(select.value, null);
  assert.match(select.opts[0].label, /旧分区，保留/);
  assert.equal(page.run('tpl.human_type2'), null);
  select.pick(1008);
  assert.equal(page.run('tpl.human_type2'), 1008);
  assert.equal(page.run('tpl.tid'), 900);
});

test('模板列表优先显示新版分区名称，旧模板仍按旧 tid 查找', async () => {
  const page = consolePage(async () => modern);
  page.context.templates = [{ id: 1, account_id: 1, tid: 900, human_type2: 1008 }, { id: 2, account_id: 1, tid: 900 }];
  await page.run('renderTemplates()');
  assert.equal(page.$('[data-template-tid="1"]').textContent, '1008（游戏）');
  assert.equal(page.$('[data-template-tid="2"]').textContent, '900（新分区）');
});

test('新版列表为空或请求失败，不能静默用旧列表创建新模板', async () => {
  for (const data of [{ ...modern, human_type_list: [] }, null]) {
    const page = consolePage(async () => data);
    page.run('openTemplate("new")');
    await page.run('loadAccountLists()');
    assert.equal(page.$('#t-save').disabled, true);
    assert.equal(page.$('#t-err').hidden, false);
  }
});
