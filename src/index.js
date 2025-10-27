// --- Configuration: Your Permanent Vercel Backends ---
const REGULAR_BACKEND_URL = 'https://multi-result-beu-regular.vercel.app/api/regular/result';
const LE_BACKEND_URL = 'https://multi-result-beu-le.vercel.app/api/le/result';

// --- Cache Time (4 days in seconds) ---
const CACHE_TTL = 4 * 24 * 60 * 60;

// --- Configuration: Fetch Ranges ---
const CONCURRENCY_LIMIT = 1; // Kept at 1 for 100% accuracy
const BATCH_STEP = 5;
const SMALL_REG_RANGE = [1, 60];
const SMALL_LE_RANGE = [901, 930];
const BIG_REG_RANGE = [1, 200];
const BIG_LE_RANGE = [901, 950];

// --- CORS Headers ---
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

/**
 * --- ES Module Entry Point ---
 * 1. 'fetch': Handles the user's request
 * 2. 'scheduled': Handles the cron job
 */
export default {
  /**
   * --- 1. THE FETCH HANDLER (for user requests) ---
   */
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: CORS_HEADERS });
    }
    if (request.method !== 'GET') {
      return new Response(JSON.stringify({ error: 'Method Not Allowed' }), {
        status: 405, headers: CORS_HEADERS
      });
    }

    try {
      // Pass 'env' and 'ctx' so our handler can use KV and Caching
      return await handleGetRequest(request, env, ctx);
    } catch (error) {
      console.error(`Critical Error: ${error.message}`);
      return new Response(JSON.stringify({ error: 'An unexpected server error occurred.' }), {
        status: 500, headers: CORS_HEADERS
      });
    }
  },

  /**
   * --- 2. THE SCHEDULED HANDLER (for the "cron job") ---
   * This runs automatically based on your wrangler.toml
   */
  async scheduled(event, env, ctx) {
    console.log("Cron job running: Retrying failed batches...");

    // Get the "to-do list" of failed batches from KV
    // We only retry 50 batches per hour to avoid rate limits
    const { keys } = await env.RETRY_QUEUE.list({ limit: 50 }); 

    for (const key of keys) {
      const targetUrl = key.name; // The key is the full URL that failed
      console.log(`Retrying: ${targetUrl}`);

      // Re-build the parameters from the URL
      const url = new URL(targetUrl);
      const params = url.searchParams;
      const regNo = params.get('reg_no');
      const queryParams = {
        year: params.get('year'),
        semester: params.get('semester'),
        exam_held: params.get('exam_held')
      };

      let baseUrl;
      if (targetUrl.startsWith(REGULAR_BACKEND_URL)) {
          baseUrl = REGULAR_BACKEND_URL;
      } else {
          baseUrl = LE_BACKEND_URL;
      }

      // 1. Re-fetch the failed batch from Vercel
      const freshBatch = await fetchVercelBatch(baseUrl, regNo, queryParams);

      // 2. Check the new result (is it still a temporary error?)
      const isBadBatch = freshBatch.some(r => r.status.includes('Error'));

      if (!isBadBatch) {
        // --- SUCCESS! ---
        console.log(`Retry SUCCESS for ${targetUrl}. Caching and de-queuing.`);

        const responseToCache = new Response(JSON.stringify(freshBatch), {
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': `public, max-age=${CACHE_TTL}`
          }
        });

        // 3a. Put the good data in the main cache
        const cacheKey = new Request(targetUrl);
        ctx.waitUntil(caches.default.put(cacheKey, responseToCache.clone()));

        // 3b. Remove it from the "to-do list"
        ctx.waitUntil(env.RETRY_QUEUE.delete(key.name));

      } else {
        // --- FAILED AGAIN ---
        console.log(`Retry FAILED for ${targetUrl}. Will try again next hour.`);
        // Just leave it in the KV queue.
      }
    }
    console.log("Cron job finished.");
  }
};

/**
 * --- Main Request Handler Logic ---
 * Handles the incoming request from the user
 */
async function handleGetRequest(request, env, ctx) {
  const url = new URL(request.url);
  const params = url.searchParams;

  const regNo = params.get('reg_no');
  const year = params.get('year');
  const semester = params.get('semester');
  const examHeld = params.get('exam_held');

  if (!regNo || !/^\d{11}$/.test(regNo)) {
    return new Response(JSON.stringify({ error: 'Invalid "reg_no"' }), { status: 400, headers: CORS_HEADERS });
  }
  if (!year || !semester || !examHeld) {
    return new Response(JSON.stringify({ error: 'Missing parameters' }), { status: 400, headers: CORS_HEADERS });
  }

  const queryParams = { year, semester, exam_held: examHeld };

  const firstTwo = regNo.slice(0, 2);
  const restReg = regNo.slice(2, -3);
  const suffixNum = parseInt(regNo.slice(-3));

  let regularPrefix, lePrefix, probeUrl, isRegularStudent;
  let userBatchStartRegNo;

  if (suffixNum >= 900) {
    isRegularStudent = false;
    lePrefix = regNo.slice(0, -3);
    regularPrefix = (parseInt(firstTwo) - 1).toString() + restReg;
    probeUrl = LE_BACKEND_URL;
    const userBatchStartNum = Math.floor((suffixNum - 901) / BATCH_STEP) * BATCH_STEP + 901;
    userBatchStartRegNo = lePrefix + String(userBatchStartNum).padStart(3, '0');
  } else {
    isRegularStudent = true;
    regularPrefix = regNo.slice(0, -3);
    lePrefix = (parseInt(firstTwo) + 1).toString() + restReg;
    probeUrl = REGULAR_BACKEND_URL;
    const userBatchStartNum = Math.floor((suffixNum - 1) / BATCH_STEP) * BATCH_STEP + 1;
    userBatchStartRegNo = regularPrefix + String(userBatchStartNum).padStart(3, '0');
  }

  // 3. --- PROBE USER'S BATCH (CHECKS CACHE FIRST) ---
  console.log(`Probing user's batch first: ${userBatchStartRegNo}`);
  const probeBatchResults = await getCachedOrFetchBatch(
    probeUrl, userBatchStartRegNo, queryParams, env, ctx
  );

  // 4. Smart Range Logic
  let regRange = SMALL_REG_RANGE;
  let leRange = SMALL_LE_RANGE;
  const isBigCollegeAttempt = (isRegularStudent && suffixNum > SMALL_REG_RANGE[1]) || (!isRegularStudent && suffixNum > SMALL_LE_RANGE[1]);
  const isProbeSuccessful = Array.isArray(probeBatchResults) && probeBatchResults.some(r => r.status === 'success');

  if (isBigCollegeAttempt && isProbeSuccessful) {
    console.log(`Probe successful! CONFIRMED Big College. Expanding range.`);
    regRange = BIG_REG_RANGE;
    leRange = BIG_LE_RANGE;
  } else {
    console.log(`Standard college. Using default small range.`);
  }

  // 5. Create Fetch Swarm
  const fetchTasks = []; // A list of functions to call
  const [regStart, regEnd] = regRange;
  const [leStart, leEnd] = leRange;

  for (let i = regStart; i <= regEnd; i += BATCH_STEP) {
    const batchRegNo = regularPrefix + String(i).padStart(3, '0');
    if (isRegularStudent && batchRegNo === userBatchStartRegNo) continue; // Skip
    fetchTasks.push(() => getCachedOrFetchBatch(REGULAR_BACKEND_URL, batchRegNo, queryParams, env, ctx));
  }

  for (let i = leStart; i <= leEnd; i += BATCH_STEP) {
    const batchRegNo = lePrefix + String(i).padStart(3, '0');
    if (!isRegularStudent && batchRegNo === userBatchStartRegNo) continue; // Skip
    fetchTasks.push(() => getCachedOrFetchBatch(LE_BACKEND_URL, batchRegNo, queryParams, env, ctx));
  }

  // 6. Execute Swarm (with CONCURRENCY_LIMIT = 1)
  const allOtherResults = await fetchInBatches(fetchTasks, CONCURRENCY_LIMIT);

  // 7. Process Final Results
  let combinedData = [...probeBatchResults]; // Start with the user's batch

  allOtherResults.forEach(result => {
    if (result.status === 'fulfilled' && Array.isArray(result.value)) {
      combinedData.push(...result.value);
    } 
    else if (result.status === 'rejected') {
        combinedData.push({ regNo: 'Unknown', status: 'Error', reason: `Worker Error: ${result.reason}` });
    }
  });

  // Sort by regNo to fix the order
  const finalSortedData = combinedData.sort((a, b) => a.regNo.localeCompare(b.regNo));

  return new Response(JSON.stringify(finalSortedData), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

/**
 * --- Core Caching Function ---
 * This function wraps our Vercel fetch with cache logic.
 */
async function getCachedOrFetchBatch(baseUrl, regNo, queryParams, env, ctx) {
  const { year, semester, exam_held } = queryParams;
  const targetUrl = `${baseUrl}?reg_no=${regNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(exam_held)}`;

  // The Cache Key is a Request object
  const cacheKey = new Request(targetUrl);

  // 1. Check the main cache first
  const cachedResponse = await caches.default.match(cacheKey);
  if (cachedResponse) {
    console.log(`Cache HIT for ${regNo}`);
    return cachedResponse.json();
  }

  // 2. Cache MISS: Fetch from Vercel
  console.log(`Cache MISS for ${regNo}. Fetching from Vercel...`);
  const freshBatch = await fetchVercelBatch(baseUrl, regNo, queryParams);

  // 3. Analyze the new batch
  // A "bad batch" is one that contains a *temporary* error
  const isBadBatch = freshBatch.some(r => 
      r.status.includes('Error') || r.status.includes('Timed Out')
  );

  if (isBadBatch) {
    // --- FAILED BATCH ---
    console.log(`Fetch for ${regNo} FAILED. Queuing for retry.`);
    // Add this URL to the "to-do list" (KV)
    ctx.waitUntil(env.RETRY_QUEUE.put(targetUrl, "failed"));
    return freshBatch; 

  } else {
    // --- GOOD BATCH (all 'success' or 'Record not found') ---
    console.log(`Fetch for ${regNo} SUCCEEDED. Caching.`);

    const responseToCache = new Response(JSON.stringify(freshBatch), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': `public, max-age=${CACHE_TTL}` // Cache for 4 days
      }
    });

    // Put it in the cache in the background
    ctx.waitUntil(caches.default.put(cacheKey, responseToCache.clone()));
    return freshBatch;
  }
}

/**
* --- Helper Function: Pooled Concurrency ---
* Runs promises in small, controlled batches (size = CONCURRENCY_LIMIT)
*/
async function fetchInBatches(promiseFunctions, batchSize) {
  let allResults = [];
  for (let i = 0; i < promiseFunctions.length; i += batchSize) {
    const batch = promiseFunctions.slice(i, i + batchSize);
    const batchPromises = batch.map(fn => fn()); 
    const results = await Promise.allSettled(batchPromises);
    allResults = allResults.concat(results);
  }
  return allResults;
}

/**
* --- Helper Function: Fetch Vercel Batch ---
* This function is designed to *never* reject, only fulfill with an array.
*/
async function fetchVercelBatch(baseUrl, regNo, queryParams) {
  const { year, semester, exam_held } = queryParams;
  const targetUrl = `${baseUrl}?reg_no=${regNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(exam_held)}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000); // 30-sec timeout

  try {
    const response = await fetch(targetUrl, {
        signal: controller.signal,
        headers: { 'Accept': 'application/json', 'User-Agent': 'Cloudflare-Smart-Worker' }
    });
    clearTimeout(timeoutId);
    if (!response.ok) {
      return [{ regNo: regNo, status: 'Error', reason: `Backend Error: HTTP ${response.status}` }];
    }
    return await response.json(); 
  } catch (error) {
    clearTimeout(timeoutId);
    let reason = error.name === 'AbortError' ? 'Request Timed Out (30s)' : error.message;
    return [{ regNo: regNo, status: 'Error', reason: `Fetch Failed: ${reason}` }];
  }
      }
