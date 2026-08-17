/**
 * sync-feed.mjs  (v2 — dynamic + memory)
 * -------------------------------------------------------------------------
 * Aggregates Baljeetsingh Sucharia's external activity (LinkedIn, Medium,
 * Stack Overflow) into `streams.json`, consumed by blog.baljeetsingh.net.
 *
 * The blog's own posts are native Blogger data and are NOT fetched here.
 *
 * OUTPUT SCHEMA of streams.json is a public contract with the site and must
 * not change. Per-type item shapes are frozen (see the build sites below).
 *
 * DESIGN: fully dynamic discovery, no hardcoded list of "verified" URLs.
 *   - Stack Overflow  -> public API (authoritative)
 *   - Medium          -> RSS (rss2json, with direct-XML fallback)
 *   - LinkedIn        -> live web-search discovery across multiple engines
 *                        (+ Google CSE when configured), then strict author
 *                        verification.
 *
 * MEMORY: because search engines are noisy (a run may find 30 posts or 3),
 * newly discovered items are MERGED into the previously committed
 * streams.json. Once an item is discovered it persists — the versioned JSON
 * is the memory, so a thin search run never shrinks the feed. Nothing is
 * hand-maintained.
 *
 * Other robustness: fetch timeouts + retries, bounded concurrency, quota
 * guarding for Google CSE, deterministic fallback images (no churn), and a
 * content-diff gate so the file is only rewritten when items actually change.
 *
 * Run:  node scripts/sync-feed.mjs
 * Test: node scripts/sync-feed.mjs --selftest   (offline, no network)
 * -------------------------------------------------------------------------
 */

import fs from 'fs';

// ------------------------------- config ----------------------------------
const STACK_OVERFLOW_USER_ID = '1415364';
const MEDIUM_USERNAME = 'baljeetsingh';
const OUTPUT_PATH = 'streams.json';

const GOOGLE_API_KEY = (process.env.GOOGLE_SEARCH_API_KEY || '').trim();
const GOOGLE_CX = (process.env.GOOGLE_SEARCH_CX || '').trim();

const FETCH_TIMEOUT_MS = 15000;
const FETCH_RETRIES = 2;
const SEARCH_CONCURRENCY = 6;

const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

// Search phrases used to DISCOVER posts (not the posts themselves).
const DISCOVERY_TERMS = [
  '"baljeetsingh sucharia"',
  '"baljeetsingh" "sucharia"',
  '"baljeetsingh"',
  '"baljeetsingh" leadership',
  '"baljeetsingh" architecture',
  '"baljeetsingh" caching',
  '"baljeetsingh" productmanagement',
  '"baljeetsingh" quotes',
  '"baljeetsingh" hiring',
  '"baljeetsingh" dpg'
];

const UNIQUE_PHOTO_POOL = [
  'https://images.unsplash.com/photo-1558494949-ef010cbdcc31?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1544197150-b99a580bb7a8?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1551808525-51a94da548ce?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1531482615713-2afd69097998?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1518770660439-4636190af475?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1563986768609-322da13575f3?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1544383835-bda2bc66a55d?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1551288049-bebda4e38f71?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1504868584819-f8e8b4b6d7e3?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1460925895917-afdab827c52f?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1526374965328-7f61d4dc18c5?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1508739773434-c26b3d09e071?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1555066931-4365d14bab8c?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1542838132-92c53300491e?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1517694712202-14dd9538aa97?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1607799279861-4dd421887fb3?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1515879218367-8466d910aaa4?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1519389950473-47ba0277781c?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1498050108023-c5249f4df085?auto=format&fit=crop&w=1200&q=80',
  'https://images.unsplash.com/photo-1587620962725-abab7fe55159?auto=format&fit=crop&w=1200&q=80'
];

// --------------------------- small utilities -----------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(...a);

/** Deterministic 32-bit FNV-1a hash. */
function stableHash(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
  }
  return h >>> 0;
}
const pickFallbackImage = (key) => UNIQUE_PHOTO_POOL[stableHash(String(key)) % UNIQUE_PHOTO_POOL.length];
const isFallbackImage = (u) => !!u && u.includes('images.unsplash.com');

async function safeFetch(url, opts = {}, { timeout = FETCH_TIMEOUT_MS, retries = FETCH_RETRIES } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeout);
    try {
      const res = await fetch(url, { ...opts, signal: ctrl.signal });
      clearTimeout(timer);
      if ((res.status >= 500 || res.status === 429) && attempt < retries) { await sleep(500 * (attempt + 1)); continue; }
      return res;
    } catch (err) {
      clearTimeout(timer);
      lastErr = err;
      if (attempt < retries) { await sleep(500 * (attempt + 1)); continue; }
    }
  }
  throw lastErr || new Error('fetch failed');
}

/** Bounded-concurrency map. Never rejects; per-item errors -> null. */
async function mapPool(items, limit, fn) {
  const out = new Array(items.length);
  let idx = 0;
  const worker = async () => {
    while (idx < items.length) {
      const i = idx++;
      try { out[i] = await fn(items[i], i); } catch { out[i] = null; }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) || 0 }, worker));
  return out;
}

function decodeHtml(html) {
  if (!html) return '';
  return html
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => { try { return String.fromCodePoint(parseInt(hex, 16)); } catch { return ''; } })
    .replace(/&#([0-9]+);/g, (_, dec) => { try { return String.fromCodePoint(parseInt(dec, 10)); } catch { return ''; } })
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&nbsp;/g, ' ').replace(/ /g, ' ')
    .replace(/&#27;/g, "'").replace(/&#x2F;/g, '/').replace(/&#x60;/g, '`').replace(/&#x3D;/g, '=');
}
function extractPureText(html) {
  if (!html) return '';
  return decodeHtml(html.replace(/<[^>]*>?/gm, ' ')).replace(/\s+/g, ' ').trim();
}
function calculateReadTime(text) {
  const words = (text || '').trim().split(/\s+/).filter(Boolean).length;
  return `${Math.max(1, Math.ceil(words / 200))} min read`;
}
function safeDecode(str) {
  try { return decodeURIComponent(str); } catch { try { return unescape(str); } catch { return str; } }
}

// ------------------------------ Stack Overflow ----------------------------
async function fetchStackOverflow() {
  log('\n⚡ [Stack Overflow] fetching answers…');
  const items = [];
  try {
    const url = `https://api.stackexchange.com/2.3/users/${STACK_OVERFLOW_USER_ID}/answers`
      + `?order=desc&sort=creation&site=stackoverflow&pagesize=100&filter=default`;
    const res = await safeFetch(url);
    log(`   answers status ${res.status}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (data.backoff) { log(`   backoff ${data.backoff}s`); await sleep((data.backoff + 1) * 1000); }
    if (data.items && data.items.length) {
      const qids = [...new Set(data.items.map((a) => a.question_id))].join(';');
      const qMap = {};
      if (qids) {
        const qRes = await safeFetch(`https://api.stackexchange.com/2.3/questions/${qids}?site=stackoverflow&filter=default`);
        if (qRes.ok) (((await qRes.json()).items) || []).forEach((q) => { qMap[q.question_id] = decodeHtml(q.title); });
      }
      for (const item of data.items) {
        const title = qMap[item.question_id] || 'Stack Overflow Engineering Solution';
        const d = new Date(item.creation_date * 1000);
        items.push({
          type: 'stackoverflow',
          id: `so-${item.answer_id}`,
          title,
          url: `https://stackoverflow.com/a/${item.answer_id}`,
          score: item.score,
          isAccepted: item.is_accepted,
          snippet: `Engineering solution for "${title}" on Stack Overflow (Score: ${item.score}${item.is_accepted ? ', Accepted' : ''}).`,
          date: d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }),
          publishedAt: d.toISOString(),
          readTime: '2 min read',
          labels: ['Stack Overflow', 'Engineering', 'Architecture']
        });
      }
    }
  } catch (err) { console.error('   ❌ Stack Overflow error:', err.message); }
  log(`   ✅ ${items.length} answers`);
  return items;
}

// -------------------------------- Medium ----------------------------------
function mediumItemFromRss(item) {
  const title = decodeHtml(item.title || '');
  const content = extractPureText(item.content || item.description || '');
  const d = new Date(item.pubDate);
  return {
    type: 'medium',
    title,
    url: item.link,
    snippet: content.length > 30 ? content.slice(0, 160) + '...' : 'Published story by Baljeetsingh Sucharia on Medium.',
    cover: item.thumbnail || null,
    image: item.thumbnail || null,
    date: isNaN(d) ? '' : d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }),
    publishedAt: isNaN(d) ? null : d.toISOString(),
    readTime: calculateReadTime(content),
    labels: item.categories && item.categories.length ? ['Medium', ...item.categories.slice(0, 3)] : ['Medium', 'Article']
  };
}
function parseMediumXml(xml) {
  const out = [];
  for (const b of xml.split(/<item>/i).slice(1)) {
    const body = b.split(/<\/item>/i)[0];
    const pick = (tag) => {
      const m = body.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i'));
      return m ? m[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1').trim() : '';
    };
    const categories = [...body.matchAll(/<category[^>]*>([\s\S]*?)<\/category>/gi)]
      .map((m) => m[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1').trim()).filter(Boolean);
    const content = pick('content:encoded') || pick('description');
    out.push({
      title: pick('title'), link: pick('link'), pubDate: pick('pubDate'),
      content, description: pick('description'), categories,
      thumbnail: (content.match(/<img[^>]+src=["']([^"']+)["']/i) || [])[1] || null
    });
  }
  return out;
}
async function fetchMedium() {
  log('\n⚡ [Medium] fetching articles…');
  const items = [];
  const feedUrl = `https://medium.com/feed/@${MEDIUM_USERNAME}`;
  try {
    const res = await safeFetch(`https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(feedUrl)}`);
    log(`   rss2json status ${res.status}`);
    if (res.ok) {
      const data = await res.json();
      if (data.status === 'ok' && data.items) data.items.forEach((it) => items.push(mediumItemFromRss(it)));
    }
  } catch (err) { console.warn('   ⚠️ rss2json failed:', err.message); }
  if (items.length === 0) {
    try {
      log('   ↩️ direct Medium RSS XML fallback…');
      const res = await safeFetch(feedUrl, { headers: { 'User-Agent': BROWSER_UA, 'Accept': 'application/rss+xml,text/xml,*/*' } });
      if (res.ok) parseMediumXml(await res.text()).forEach((it) => { if (it.link) items.push(mediumItemFromRss(it)); });
    } catch (err) { console.warn('   ⚠️ direct Medium RSS failed:', err.message); }
  }
  log(`   ✅ ${items.length} stories`);
  return items;
}

// ------------------------------- LinkedIn ---------------------------------
const LINKEDIN_EXCLUDE = [
  'baljeetsinghhira', 'baljeetssangha', 'baljeetsinghofficial', 'baljeetsinghcpa',
  'tejinder-kaur', 'baljeet-singh', '/posts/baljeet-singh', 'harsh-baljeetsingh-yadav',
  'the-flowry-show', 'pulse/snapper', 'pulse/arabs', 'pulse/liefde', 'pulse/guide-facebook'
];
function isAuthenticLinkedIn(link) {
  const l = link.toLowerCase();
  const isLi = l.includes('linkedin.com/posts/') || l.includes('linkedin.com/feed/update/') || l.includes('linkedin.com/pulse/');
  if (!isLi) return false;
  if (LINKEDIN_EXCLUDE.some((x) => l.includes(x))) return false;
  return l.includes('/posts/baljeetsingh_') || (l.includes('/pulse/') && (l.includes('baljeetsingh') || l.includes('sucharia')));
}

/** Engine-agnostic: pull LinkedIn post/pulse URLs out of any search-result HTML. */
function extractLinkedInUrls(html) {
  const found = new Set();
  const text = html + '\n' + safeDecode(html);
  const re = /https?:\/\/[a-z0-9.-]*linkedin\.com\/(?:posts|pulse|feed\/update)\/[^\s"'<>\\)]+/gi;
  for (const m of text.matchAll(re)) {
    let u = m[0].split(/[?#]/)[0].replace(/[.,)]+$/, '');
    u = u.replace(/https?:\/\/[a-z0-9-]+\.linkedin\.com/i, 'https://www.linkedin.com');
    found.add(u);
  }
  return [...found];
}

function cleanTitle(raw, url) {
  let t = decodeHtml(raw || '');
  t = t.replace(/\s*\|\s*Baljeetsingh\s+Sucharia.*$/i, '')
    .replace(/\s*\|\s*LinkedIn.*$/i, '')
    .replace(/\s*\|\s*Facebook.*$/i, '')
    .replace(/^Baljeetsingh\s*-\s*/i, '')
    .replace(/^[0-9]+\s*[ऀ-ൿ઀-૿a-zA-Z\s]+\|\s*/i, '')
    .replace(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/gu, '')
    .trim();
  if (t.includes('\n')) {
    const lines = t.split(/\n+/).map((l) => l.trim()).filter((l) => l.length > 5 && !l.startsWith('#'));
    if (lines.length > 0) t = lines[0];
  }
  if (t.startsWith('#')) {
    const tags = t.split(/\s+/).filter((w) => w.startsWith('#')).map((w) => w.replace(/^#/, ''));
    if (tags.length > 0) t = tags.map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(', ');
  }
  t = t.replace(/\s*#[a-zA-Z0-9_-]+/g, '').trim();
  if (t.length > 100 && t.includes('.')) {
    const first = t.split('.')[0].trim();
    if (first.length > 15) t = first;
  }
  if (!t || t.length < 3) {
    let slug = (url.split('/posts/')[1] || url.split('/pulse/')[1] || '');
    slug = slug.split('-activity-')[0].replace(/^baljeetsingh_/, '').replace(/^baljeetsinghsucharia_/, '').replace(/-/g, ' ');
    t = slug.replace(/\b\w/g, (l) => l.toUpperCase()).trim();
  }
  if (!t || t.length < 3) return 'Engineering & Leadership Insights';
  return t;
}
function deriveLabels(title, url, snippet) {
  const lower = (title + ' ' + url + ' ' + snippet).toLowerCase();
  const labels = ['LinkedIn'];
  if (/\b(ai|intelligence|futureofwork|llm)\b/.test(lower)) labels.push('AI');
  if (/\b(architecture|caching|cloud|systems|opensource|dpg|dpdpa)\b/.test(lower)) labels.push('Architecture');
  if (/\b(leadership|coach|leaders|mindset|career|gratitude|womensday)\b/.test(lower)) labels.push('Leadership');
  if (/\b(strategy|product|kpi|growth|business|innovation)\b/.test(lower)) labels.push('Strategy');
  if (labels.length === 1) labels.push('Engineering', 'Insights');
  return labels;
}
/** Date from the activity Snowflake ID, or og:published time, else null (undateable -> dropped). */
function resolveLinkedInDate(url, html) {
  const am = url.match(/activity-([0-9]{15,22})/);
  if (am) {
    try {
      const ts = Number(BigInt(am[1]) >> 22n);
      if (ts > 1262304000000 && ts < Date.now() + 86400000) {
        const d = new Date(ts);
        return { isoDate: d.toISOString(), dateStr: d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }) };
      }
    } catch { /* ignore */ }
  }
  if (html) {
    const m = html.match(/<meta[^>]+property=["']article:published_time["'][^>]+content=["']([^"']+)["']/i)
      || html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']article:published_time["']/i);
    if (m) { const d = new Date(m[1]); if (!isNaN(d)) return { isoDate: d.toISOString(), dateStr: d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }) }; }
  }
  return null;
}

async function queryGoogleCustomSearch(query, quota) {
  if (!GOOGLE_API_KEY || !GOOGLE_CX || quota.exhausted) return [];
  const results = [];
  try {
    for (let start = 1; start <= 11; start += 10) {
      const url = `https://www.googleapis.com/customsearch/v1?key=${GOOGLE_API_KEY}&cx=${GOOGLE_CX}&q=${encodeURIComponent(query)}&start=${start}&num=10`;
      const res = await safeFetch(url);
      if (res.status === 403 || res.status === 429) { quota.exhausted = true; log('   ⚠️ Google CSE quota hit — stopping.'); break; }
      if (!res.ok) break;
      const data = await res.json();
      if (data.items && data.items.length) results.push(...data.items); else break;
      if (!data.queries?.nextPage) break;
    }
  } catch (err) { console.warn('   ⚠️ Google CSE warning:', err.message); }
  return results;
}

async function fetchLinkedInArticles() {
  log('\n🌐 [LinkedIn] dynamic discovery…');
  const candidateLinks = new Set();
  const googleMetadataMap = {};
  const quota = { exhausted: false };

  // 1) Google Custom Search (structured; also yields titles/images) — quota guarded.
  if (GOOGLE_API_KEY && GOOGLE_CX) {
    for (const term of DISCOVERY_TERMS) {
      if (quota.exhausted) break;
      for (const scope of ['site:linkedin.com/posts', 'site:linkedin.com/pulse']) {
        if (quota.exhausted) break;
        const items = await queryGoogleCustomSearch(`${term} ${scope}`, quota);
        items.forEach((it) => {
          if (it.link && (it.link.includes('linkedin.com/posts/') || it.link.includes('linkedin.com/pulse/'))) {
            candidateLinks.add(it.link.split(/[?#]/)[0]);
            googleMetadataMap[it.link.split(/[?#]/)[0]] = { title: it.title, snippet: it.snippet, image: it.image };
          }
        });
      }
    }
  }

  // 2) Multi-engine open web search (best effort, engine-agnostic URL extraction).
  const engines = [
    (q, p) => `https://search.yahoo.com/search?p=${encodeURIComponent(q)}&b=${p}`,
    (q, p) => `https://www.bing.com/search?q=${encodeURIComponent(q)}&first=${p}`,
    (q) => `https://html.duckduckgo.com/html/?q=${encodeURIComponent(q)}`,
    (q) => `https://lite.duckduckgo.com/lite/?q=${encodeURIComponent(q)}`
  ];
  const jobs = [];
  for (const term of DISCOVERY_TERMS) {
    for (const scope of ['site:linkedin.com/posts', 'site:linkedin.com/pulse']) {
      const q = `${scope} ${term}`;
      for (const engine of engines) {
        for (const p of [1, 11, 21]) {
          jobs.push(engine(q, p));
          if (engine.length === 1) break; // DDG endpoints: single page only
        }
      }
    }
  }
  const scraped = await mapPool(jobs, SEARCH_CONCURRENCY, async (u) => {
    const res = await safeFetch(u, { headers: { 'User-Agent': BROWSER_UA, 'Accept': 'text/html,*/*' } }, { retries: 0 });
    return extractLinkedInUrls(await res.text());
  });
  scraped.forEach((links) => (links || []).forEach((l) => candidateLinks.add(l)));

  log(`   candidate pool: ${candidateLinks.size} urls`);
  const verified = [...candidateLinks].filter(isAuthenticLinkedIn);
  log(`   authentic after filter: ${verified.length}`);

  // 3) Enrich (title / snippet / image / date). Date can come from the URL's
  //    activity id alone, so posts survive even when LinkedIn blocks the bot.
  const enriched = await mapPool(verified, SEARCH_CONCURRENCY, async (url) => {
    const gMeta = googleMetadataMap[url] || {};
    let postPhoto = gMeta.image || null;
    let rawTitle = gMeta.title || '';
    let rawSnippet = gMeta.snippet || '';
    let html = '';
    try {
      const r = await safeFetch(url, {
        headers: { 'User-Agent': 'LinkedInBot/1.0 (compatible; Mozilla/5.0; Apache-HttpClient +http://www.linkedin.com)', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' },
        redirect: 'follow'
      }, { retries: 1 });
      const finalUrl = r.url || url;
      if (r.status !== 404 && !finalUrl.includes('article_not_found') && !finalUrl.includes('trk=article_not_found')) {
        html = await r.text();
        const ogImg = html.match(/<meta\s+(?:property|name)=["']og:image["']\s+content=["']([^"']+)["']/i)
          || html.match(/<meta\s+content=["']([^"']+)["']\s+(?:property|name)=["']og:image["']/i)
          || html.match(/<meta\s+(?:property|name)=["']twitter:image["']\s+content=["']([^"']+)["']/i);
        if (ogImg) {
          const cand = ogImg[1].replace(/&amp;/g, '&');
          if (!cand.includes('static.licdn.com/aero-v1/sc/h/') && !cand.includes('static.xx.fbcdn.net/rsrc.php')) postPhoto = cand;
        }
        const ogT = html.match(/<meta\s+(?:property|name)=["']og:title["']\s+content=["']([^"']+)["']/i) || html.match(/<meta\s+content=["']([^"']+)["']\s+(?:property|name)=["']og:title["']/i);
        const ogD = html.match(/<meta\s+(?:property|name)=["']og:description["']\s+content=["']([^"']+)["']/i) || html.match(/<meta\s+content=["']([^"']+)["']\s+(?:property|name)=["']og:description["']/i);
        if (ogT && ogT[1]) rawTitle = ogT[1];
        if (ogD && ogD[1]) rawSnippet = ogD[1];
      }
    } catch { /* fall back to search metadata + activity-id date */ }

    const dateInfo = resolveLinkedInDate(url, html);
    if (!dateInfo) return null; // undateable -> drop (prevents "now" sort poisoning)

    const title = cleanTitle(rawTitle, url);
    const snippet = rawSnippet
      ? rawSnippet.replace(/&amp;/g, '&').replace(/<[^>]*>?/gm, '').slice(0, 180) + '...'
      : `LinkedIn dispatch by Baljeetsingh Sucharia on ${title.toLowerCase()}, product strategy, and engineering architecture.`;

    return { type: 'linkedin', title, url, cover: postPhoto, image: postPhoto, snippet, date: dateInfo.dateStr, publishedAt: dateInfo.isoDate, labels: deriveLabels(title, url, snippet) };
  });

  const items = enriched.filter(Boolean);
  log(`   ✅ ${items.length} authentic linkedin items discovered this run`);
  return items;
}

// --------------------- memory merge, assemble & write ---------------------
/** Stable identity across runs. */
function identityKey(it) {
  if (it.id) return 'id:' + it.id;
  const m = (it.url || '').match(/activity-([0-9]{15,22})/);
  if (m) return 'act:' + m[1];
  return 'url:' + (it.url || '').split(/[?#]/)[0].replace(/\/$/, '').toLowerCase();
}
/** Non-image signature — for change detection & churn suppression. */
function signature(it) {
  return JSON.stringify({ type: it.type, title: it.title, url: it.url, snippet: it.snippet, date: it.date, publishedAt: it.publishedAt, labels: it.labels, score: it.score, isAccepted: it.isAccepted, readTime: it.readTime });
}

/**
 * Merge freshly discovered items over remembered ones. Discovered metadata
 * wins, but a real (non-fallback) remembered image is preserved to avoid
 * churn from volatile LinkedIn image tokens. Nothing is ever dropped.
 */
function mergeWithMemory(prior, discovered) {
  const byKey = new Map();
  for (const it of prior) byKey.set(identityKey(it), { ...it });
  for (const d of discovered) {
    const k = identityKey(d);
    const ex = byKey.get(k);
    if (!ex) { byKey.set(k, { ...d }); continue; }
    const merged = { ...ex };
    for (const f of ['title', 'snippet', 'date', 'publishedAt', 'labels', 'score', 'isAccepted', 'readTime']) {
      if (d[f] !== undefined && d[f] !== null && d[f] !== '') merged[f] = d[f];
    }
    // Image: keep a real remembered image; only upgrade from fallback/none.
    if (d.image && d.image !== ex.image) {
      if (!ex.image || isFallbackImage(ex.image)) { merged.cover = d.image; merged.image = d.image; }
    }
    byKey.set(k, merged);
  }
  // De-dupe LinkedIn by title (same post surfaced via slightly different URLs).
  const seen = new Set();
  const out = [];
  for (const it of byKey.values()) {
    if (it.type === 'linkedin') {
      const t = (it.title || '').toLowerCase();
      if (seen.has(t)) continue;
      seen.add(t);
    }
    out.push(it);
  }
  return out;
}

function assemble(items) {
  items.sort((a, b) => {
    const tA = a.publishedAt ? Date.parse(a.publishedAt) : 0;
    const tB = b.publishedAt ? Date.parse(b.publishedAt) : 0;
    if (tB !== tA) return tB - tA;
    return (a.url || '').localeCompare(b.url || '');
  });
  for (const it of items) {
    if (!it.cover) it.cover = pickFallbackImage(it.id || it.url || it.title);
    if (!it.image) it.image = it.cover;
  }
  return items;
}
function loadExisting() {
  try { const d = JSON.parse(fs.readFileSync(OUTPUT_PATH, 'utf8')); return Array.isArray(d.items) ? d : null; } catch { return null; }
}
function itemsChanged(existing, newItems) {
  if (!existing) return true;
  return JSON.stringify(existing.items) !== JSON.stringify(newItems);
}

async function runSync() {
  log('🚀 Feed sync v2 (dynamic + memory)');
  log(`   Google CSE: ${GOOGLE_API_KEY && GOOGLE_CX ? 'configured' : 'not configured'}`);

  const [soItems, medItems, liItems] = await Promise.all([fetchStackOverflow(), fetchMedium(), fetchLinkedInArticles()]);
  const discovered = [...liItems, ...medItems, ...soItems];
  log(`\n📊 discovered this run — linkedin=${liItems.length} medium=${medItems.length} stackoverflow=${soItems.length}`);

  // Safety: a run that discovers nothing AND has no memory must not write empty.
  const existing = loadExisting();
  if (discovered.length === 0 && !existing) {
    console.error('❌ Nothing discovered and no existing feed. Aborting.');
    process.exit(1);
  }

  const prior = existing ? existing.items : [];
  const merged = mergeWithMemory(prior, discovered);
  const finalItems = assemble(merged);
  log(`   feed size after memory merge: ${finalItems.length} items`);

  if (!itemsChanged(existing, finalItems)) {
    log('\n✅ No content changes — leaving streams.json untouched (no commit).');
    return;
  }

  const payload = {
    updatedAt: new Date().toISOString(),
    author: 'Baljeetsingh Sucharia',
    totalItems: finalItems.length,
    userProfiles: {
      linkedin: 'https://www.linkedin.com/in/baljeetsingh/',
      stackoverflow: `https://stackoverflow.com/users/${STACK_OVERFLOW_USER_ID}/baljeetsingh-sucharia?tab=answers`,
      medium: `https://medium.com/@${MEDIUM_USERNAME}`,
      blog: 'https://blog.baljeetsingh.net'
    },
    items: finalItems
  };
  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(payload, null, 2));
  log(`\n🎉 Wrote ${OUTPUT_PATH} (${fs.statSync(OUTPUT_PATH).size} bytes, ${finalItems.length} items).`);
}

// -------------------------------- selftest --------------------------------
function selftest() {
  let pass = 0, fail = 0;
  const check = (n, c) => { if (c) pass++; else { fail++; console.error('FAIL:', n); } };

  check('readtime', calculateReadTime('word '.repeat(400)) === '2 min read');
  check('readtime min', calculateReadTime('') === '1 min read');
  check('decodeHtml', decodeHtml('a &amp; b') === 'a & b');
  check('pureText', extractPureText('<p>Hi <b>there</b></p>') === 'Hi there');
  check('hash stable', stableHash('abc') === stableHash('abc'));
  check('img deterministic', pickFallbackImage('x') === pickFallbackImage('x'));
  check('isFallback', isFallbackImage(UNIQUE_PHOTO_POOL[0]) && !isFallbackImage('https://media.licdn.com/x'));

  check('authentic ok', isAuthenticLinkedIn('https://www.linkedin.com/posts/baljeetsingh_x-activity-7016818115023921152-1exC'));
  check('authentic reject imposter', !isAuthenticLinkedIn('https://www.linkedin.com/posts/baljeet-singh_x-activity-1'));
  check('authentic reject non-li', !isAuthenticLinkedIn('https://example.com/x'));

  const urls = extractLinkedInUrls('junk <a href="https://in.linkedin.com/posts/baljeetsingh_ai-activity-7016818115023921152-1exC?trk=x">z</a> more');
  check('extract urls normalizes host+strips query', urls.includes('https://www.linkedin.com/posts/baljeetsingh_ai-activity-7016818115023921152-1exC'));

  const d = resolveLinkedInDate('https://x/activity-7016818115023921152-1exC', '');
  check('date from activity id', !!d && d.isoDate === '2023-01-05T17:30:04.798Z');
  check('date undateable -> null', resolveLinkedInDate('https://linkedin.com/pulse/foo', '') === null);
  const dp = resolveLinkedInDate('https://linkedin.com/pulse/foo', '<meta property="article:published_time" content="2021-05-04T00:00:00Z">');
  check('date from og published', !!dp && dp.isoDate.startsWith('2021'));

  // Schema shape (frozen contract)
  const med = mediumItemFromRss({ title: 'A', link: 'http://m', pubDate: '2020-01-01', content: '<p>hello world foo</p>', categories: ['x'] });
  check('medium schema', ['type', 'title', 'url', 'snippet', 'cover', 'image', 'date', 'publishedAt', 'readTime', 'labels'].every((k) => k in med) && med.type === 'medium');
  const li = { type: 'linkedin', title: 'T', url: 'https://www.linkedin.com/posts/baljeetsingh_x-activity-7016818115023921152-1exC', cover: null, image: null, snippet: 's', date: 'd', publishedAt: '2022-01-01T00:00:00.000Z', labels: ['LinkedIn'] };
  check('linkedin schema (no readTime)', ['type', 'title', 'url', 'cover', 'image', 'snippet', 'date', 'publishedAt', 'labels'].every((k) => k in li) && !('readTime' in li));

  // Memory merge: nothing dropped, discovered wins, real image preserved
  const prior = [{ ...li, title: 'Old Title', image: 'https://media.licdn.com/real.jpg', cover: 'https://media.licdn.com/real.jpg', snippet: 'old' }];
  const disc = [{ ...li, title: 'New Title', image: null, cover: null, snippet: 'new' }];
  const merged = mergeWithMemory(prior, disc);
  check('merge keeps single identity', merged.length === 1);
  check('merge discovered metadata wins', merged[0].title === 'New Title' && merged[0].snippet === 'new');
  check('merge preserves real image', merged[0].image === 'https://media.licdn.com/real.jpg');

  const priorOnly = [{ ...li, url: 'https://www.linkedin.com/posts/baljeetsingh_y-activity-6740862813146779649-kYX3' }];
  check('merge never drops remembered', mergeWithMemory(priorOnly, []).length === 1);

  // Upgrade from fallback image to a discovered real one
  const priorFb = [{ ...li, image: UNIQUE_PHOTO_POOL[0], cover: UNIQUE_PHOTO_POOL[0] }];
  const discReal = [{ ...li, image: 'https://media.licdn.com/new.jpg', cover: 'https://media.licdn.com/new.jpg' }];
  check('merge upgrades fallback image', mergeWithMemory(priorFb, discReal)[0].image === 'https://media.licdn.com/new.jpg');

  // assemble: sort + fallback image
  const asm = assemble([{ ...li, url: 'a', publishedAt: '2020-01-01T00:00:00.000Z', cover: null, image: null }, { ...li, url: 'b', publishedAt: '2022-01-01T00:00:00.000Z', cover: null, image: null }]);
  check('sort desc', asm[0].url === 'b');
  check('fallback image assigned', isFallbackImage(asm[0].cover));

  // change detection
  check('change same=false', itemsChanged({ items: [med] }, [med]) === false);
  check('change diff=true', itemsChanged({ items: [med] }, [{ ...med, title: 'B' }]) === true);
  check('change first-run=true', itemsChanged(null, [med]) === true);

  console.log(`\nSELFTEST: ${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

// --------------------------------- main -----------------------------------
if (process.argv.includes('--selftest')) selftest();
else runSync().catch((err) => { console.error('Fatal:', err); process.exit(1); });
