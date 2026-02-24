#!/usr/bin/env node
// Simple Node client that calls Supabase RPCs and writes CSV
// Usage: node supa_client.js get_Pune_contacts_200 --export out.csv --max all

const fs = require('fs');
const https = require('https');

function parseArgs(argv) {
  const res = { cmd: null, export: null, max: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (!res.cmd && !a.startsWith('--')) {
      res.cmd = a;
      continue;
    }
    if (a === '--export' && i + 1 < argv.length) {
      res.export = argv[++i];
      continue;
    }
    if (a === '--max' && i + 1 < argv.length) {
      res.max = argv[++i];
      continue;
    }
  }
  return res;
}

function rpcNameFor(cmd) {
  const n = cmd.toLowerCase();
  if (n.startsWith('get_pune_contacts')) return 'get_pune_contacts';
  if (n.startsWith('get_mh_contacts')) return 'get_mh_contacts';
  if (n.startsWith('get_contacts')) return 'get_contacts';
  return null;
}

function parseLimit(cmd, maxArg) {
  const m = cmd.match(/_(\d+)$/);
  let limit = 100;
  if (m) limit = parseInt(m[1], 10) || 100;
  if (typeof maxArg !== 'undefined' && maxArg !== null) {
    if (String(maxArg).toLowerCase() === 'all') return 0;
    const v = parseInt(maxArg, 10);
    if (!isNaN(v)) return v;
    return limit;
  }
  return limit;
}

function postJson(url, key, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const u = new URL(url);
    const options = {
      hostname: u.hostname,
      path: u.pathname + (u.search || '') ,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        'apikey': key,
        'Authorization': `Bearer ${key}`,
      }
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        if (res.statusCode >= 400) return reject(new Error(`Status ${res.statusCode}: ${body}`));
        try {
          const parsed = JSON.parse(body || '[]');
          resolve(parsed);
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function toCsv(rows) {
  if (!rows || rows.length === 0) return '';
  const keys = [];
  const seen = new Set();
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (!seen.has(k)) { seen.add(k); keys.push(k); }
    }
  }
  const escape = (v) => {
    if (v === null || typeof v === 'undefined') return '';
    if (typeof v === 'object') return JSON.stringify(v);
    const s = String(v);
    if (s.includes(',') || s.includes('\n') || s.includes('"')) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };
  const lines = [keys.join(',')];
  for (const r of rows) {
    const row = keys.map(k => escape(r[k]));
    lines.push(row.join(','));
  }
  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.cmd) {
    console.error('Usage: node supa_client.js <command> [--export file] [--max all|N]');
    process.exit(1);
  }
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    console.error('Set SUPABASE_URL and SUPABASE_ANON_KEY in env');
    process.exit(1);
  }
  const rpc = rpcNameFor(args.cmd);
  if (!rpc) {
    console.error('Unknown command; use get_Pune_contacts*, get_MH_contacts*, or get_contacts*');
    process.exit(1);
  }
  const limit = parseLimit(args.cmd, args.max);
  const body = {};
  if (typeof limit !== 'undefined' && limit !== null) body['p_limit'] = limit;
  const base = supabaseUrl.replace(/\/+$/,'');
  const root = base.toLowerCase().endsWith('/rest/v1') ? base : (base + '/rest/v1');
  const url = root + '/rpc/' + rpc;
  try {
    const rows = await postJson(url, supabaseKey, body);
    if (!rows || rows.length === 0) {
      console.log('No rows returned');
      return;
    }
    if (args.export) {
      const csv = toCsv(rows);
      fs.writeFileSync(args.export, csv, 'utf8');
      console.log(`Wrote ${rows.length} rows to ${args.export}`);
    } else {
      console.log(JSON.stringify(rows.slice(0,10), null, 2));
      if (rows.length > 10) console.log(`... ${rows.length} rows total`);
    }
  } catch (err) {
    console.error('Error:', err.message || err);
    process.exit(1);
  }
}

main();
