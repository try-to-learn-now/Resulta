// --- Configuration: Vercel Backends ---
const REGULAR_BACKEND_URL = 'https://multi-result-beu-regular.vercel.app/api/regular/result';
const LE_BACKEND_URL = 'https://multi-result-beu-le.vercel.app/api/le/result';

// --- Configuration: Cache & Limits ---
const CACHE_TTL = 4 * 24 * 60 * 60; // 4 days
const CONCURRENCY_LIMIT = 1;       // Fetch one Vercel batch at a time
const INITIAL_FETCH_LIMIT = 20;    // Batches to fetch immediately (Try lowering ONLY if subrequest error persists)
const CRON_FETCH_LIMIT = 20;       // Batches cron job fetches per run

// --- Configuration: Ranges ---
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

export default {
  /** --- FETCH HANDLER --- */
  async fetch(request, env, ctx) {
    // Basic request validation (OPTIONS, GET)
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS_HEADERS });
    if (request.method !== 'GET') return new Response(JSON.stringify({ error: 'Method Not Allowed' }), { status: 405, headers: CORS_HEADERS });

    const requestStartTime = Date.now();
    console.log(`[${requestStartTime}] Received fetch request: ${request.url}`);

    try {
      const response = await handleGetRequest(request, env, ctx);
      const duration = Date.now() - requestStartTime;
      console.log(`[${requestStartTime}] Request finished in ${duration}ms`);
      return response;
    } catch (error) {
      const duration = Date.now() - requestStartTime;
      console.error(`[${requestStartTime}] Critical Error in fetch handler after ${duration}ms: ${error.stack}`);
      return new Response(JSON.stringify({ error: 'An unexpected server error occurred.' }), { status: 500, headers: CORS_HEADERS });
    }
  },

  /** --- SCHEDULED HANDLER (CRON) --- */
  async scheduled(event, env, ctx) {
    const cronStartTime = Date.now();
    console.log(`[${cronStartTime}] Cron job running: Processing KV retry queue...`);
    
    let processedCount = 0;
    try {
      const { keys } = await env.RETRY_QUEUE.list({ limit: CRON_FETCH_LIMIT }); 
      processedCount = keys.length;

      if (keys.length === 0) {
        console.log(`[${cronStartTime}] KV queue is empty.`);
        return; 
      }

      const tasks = []; 
      for (const key of keys) {
          const [type, targetUrl] = key.name.split("::"); 
          if (!targetUrl || (type !== "REGULAR_URL" && type !== "LE_URL")) {
              console.warn(`[${cronStartTime}] Skipping invalid KV key: ${key.name}`);
              continue; 
          }

          tasks.push(async () => {
              console.log(`[${cronStartTime}] Cron retrying: ${targetUrl}`);
              const url = new URL(targetUrl);
              const params = url.searchParams;
              const regNo = params.get('reg_no'); 
              const queryParams = { 
                  year: params.get('year'),
                  semester: params.get('semester'),
                  exam_held: params.get('exam_held')
              };
              const baseUrl = type === "REGULAR_URL" ? REGULAR_BACKEND_URL : LE_BACKEND_URL;

              // Pass `true` for isCronRetry
              const freshBatch = await getCachedOrFetchBatch(baseUrl, regNo, queryParams, env, ctx, true); 
              const isBadBatch = freshBatch.some(r => r.status.includes('Error'));

              // Logging moved inside getCachedOrFetchBatch now
              // if (!isBadBatch) console.log(`[${cronStartTime}] Cron SUCCESS for ${targetUrl}.`);
              // else console.log(`[${cronStartTime}] Cron FAILED for ${targetUrl}.`);
          });
      }
      
      // Run sequentially
      await fetchInBatches(tasks, CONCURRENCY_LIMIT); 

    } catch (error) {
        console.error(`[${cronStartTime}] Error during scheduled execution: ${error.stack}`);
    } finally {
        const duration = Date.now() - cronStartTime;
        console.log(`[${cronStartTime}] Cron job finished processing ${processedCount} potential retries in ${duration}ms.`);
    }
  }
};

/** --- Main Request Handler --- */
async function handleGetRequest(request, env, ctx) {
  const url = new URL(request.url);
  const params = url.searchParams;

  // 1. Validate Params
  const regNo = params.get('reg_no');
  const year = params.get('year');
  const semester = params.get('semester');
  const examHeld = params.get('exam_held');
  if (!regNo || !/^\d{11}$/.test(regNo)) { /* error */ }
  if (!year || !semester || !examHeld) { /* error */ }
   if (!regNo || !/^\d{11}$/.test(regNo)) { return new Response(JSON.stringify({ error: 'Invalid "reg_no"' }), { status: 400, headers: CORS_HEADERS }); }
   if (!year || !semester || !examHeld) { return new Response(JSON.stringify({ error: 'Missing parameters' }), { status: 400, headers: CORS_HEADERS }); }

  const queryParams = { year, semester, exam_held: examHeld };

  // 2. Parse Reg No & Calculate Prefixes/Probe Info
  const firstTwo = regNo.slice(0, 2);
  const restReg = regNo.slice(2, -3);
  const suffixNum = parseInt(regNo.slice(-3));
  let regularPrefix, lePrefix, probeBaseUrl, isRegularStudent;
  let userBatchStartRegNo;

  if (suffixNum >= 900) { /* LE logic */ } else { /* Regular logic */ }
   if (suffixNum >= 900) { // User entered an LE number
        isRegularStudent = false;
        lePrefix = regNo.slice(0, -3);
        regularPrefix = (parseInt(firstTwo) - 1).toString() + restReg;
        probeBaseUrl = LE_BACKEND_URL;
        const userBatchStartNum = Math.floor((suffixNum - 901) / BATCH_STEP) * BATCH_STEP + 901;
        userBatchStartRegNo = lePrefix + String(userBatchStartNum).padStart(3, '0');
    } else { // User entered a Regular number
        isRegularStudent = true;
        regularPrefix = regNo.slice(0, -3);
        lePrefix = (parseInt(firstTwo) + 1).toString() + restReg;
        probeBaseUrl = REGULAR_BACKEND_URL;
        const userBatchStartNum = Math.floor((suffixNum - 1) / BATCH_STEP) * BATCH_STEP + 1;
        userBatchStartRegNo = regularPrefix + String(userBatchStartNum).padStart(3, '0');
    }

  // 3. Probe User's Batch (checks cache)
  console.log(`[${Date.now()}] Probing user's batch first: ${userBatchStartRegNo}`);
  const probeBatchResults = await getCachedOrFetchBatch(
    probeBaseUrl, userBatchStartRegNo, queryParams, env, ctx, false 
  );
  console.log(`[${Date.now()}] Probe finished for ${userBatchStartRegNo}. Success: ${probeBatchResults.some(r=>r.status==='success')}`);
  
  // 4. Determine Ranges
  let regRange = SMALL_REG_RANGE;
  let leRange = SMALL_LE_RANGE;
  const isBigCollegeAttempt = (isRegularStudent && suffixNum > SMALL_REG_RANGE[1]) || (!isRegularStudent && suffixNum > SMALL_LE_RANGE[1]);
  const isProbeSuccessful = Array.isArray(probeBatchResults) && probeBatchResults.some(r => r.status === 'success');

  if (isBigCollegeAttempt && isProbeSuccessful) {
    console.log(`[${Date.now()}] Probe successful & Big College attempt! Expanding range.`);
    regRange = BIG_REG_RANGE;
    leRange = BIG_LE_RANGE;
  } else {
    console.log(`[${Date.now()}] Using default small range.`);
  }

  // 5. Split Tasks
  const initialFetchTasks = []; 
  const deferredFetchKeys = []; 
  const [regStart, regEnd] = regRange;
  const [leStart, leEnd] = leRange;

  // Populate tasks/keys (Regular)
  for (let i = regStart; i <= regEnd; i += BATCH_STEP) {
    const batchRegNo = regularPrefix + String(i).padStart(3, '0');
    if (isRegularStudent && batchRegNo === userBatchStartRegNo) continue; 
    const task = () => getCachedOrFetchBatch(REGULAR_BACKEND_URL, batchRegNo, queryParams, env, ctx, false);
    const targetUrl = `${REGULAR_BACKEND_URL}?reg_no=${batchRegNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(examHeld)}`;
    const kvKey = `REGULAR_URL::${targetUrl}`; 
    if (initialFetchTasks.length < INITIAL_FETCH_LIMIT) initialFetchTasks.push(task);
    else deferredFetchKeys.push(kvKey); 
  }
  // Populate tasks/keys (LE)
  for (let i = leStart; i <= leEnd; i += BATCH_STEP) {
    const batchRegNo = lePrefix + String(i).padStart(3, '0');
    if (!isRegularStudent && batchRegNo === userBatchStartRegNo) continue; 
    const task = () => getCachedOrFetchBatch(LE_BACKEND_URL, batchRegNo, queryParams, env, ctx, false);
    const targetUrl = `${LE_BACKEND_URL}?reg_no=${batchRegNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(examHeld)}`;
    const kvKey = `LE_URL::${targetUrl}`; 
    if (initialFetchTasks.length < INITIAL_FETCH_LIMIT) initialFetchTasks.push(task);
    else deferredFetchKeys.push(kvKey);
  }

  // 6. Execute Initial & Queue Deferred
  console.log(`[${Date.now()}] Executing ${initialFetchTasks.length} initial fetches serially...`);
  const initialFetchStartTime = Date.now();
  const initialResults = await fetchInBatches(initialFetchTasks, CONCURRENCY_LIMIT);
  console.log(`[${Date.now()}] Initial fetches finished in ${Date.now() - initialFetchStartTime}ms.`);

  if (deferredFetchKeys.length > 0) {
      console.log(`[${Date.now()}] Queueing ${deferredFetchKeys.length} deferred batches into KV...`);
      const checkAndPutPromises = deferredFetchKeys.map(async (key) => {
          try {
              const exists = await env.RETRY_QUEUE.get(key); 
              const cacheUrl = key.split("::")[1];
              if (!cacheUrl) return; // Skip invalid key
              const cacheKey = new Request(cacheUrl);
              const cached = await caches.default.match(cacheKey); 
              if (!exists && !cached) {
                  console.log(`[${Date.now()}] Adding deferred key to KV: ${key}`);
                  return env.RETRY_QUEUE.put(key, "deferred", { expirationTtl: CACHE_TTL }); // Add TTL to KV entry
              } else {
                 // console.log(`[${Date.now()}] Skipping KV put for ${key} (already exists or cached)`);
              }
          } catch (e) {
              console.error(`[${Date.now()}] Error checking/putting key ${key} in KV: ${e}`);
          }
      });
      // Don't wait for this to finish, let it run in background
      ctx.waitUntil(Promise.allSettled(checkAndPutPromises).then(() => console.log(`[${Date.now()}] Finished queueing deferred batches.`)));
  }

  // 7. Process Final Results
  let combinedData = [];
  // Ensure probe results are an array
  if(Array.isArray(probeBatchResults)) {
      combinedData.push(...probeBatchResults); 
  } else {
      console.error("Probe results were not an array:", probeBatchResults);
  }
  
  initialResults.forEach(result => { 
    if (result.status === 'fulfilled' && Array.isArray(result.value)) {
      combinedData.push(...result.value);
    } 
    else if (result.status === 'rejected') {
        console.error(`[${Date.now()}] Initial fetch promise rejected: ${result.reason}`);
        combinedData.push({ regNo: 'Unknown', status: 'Error', reason: `Worker Error: ${result.reason?.message || result.reason}` });
    } else if (result.status === 'fulfilled' && !Array.isArray(result.value)) {
        console.error(`[${Date.now()}] Initial fetch promise fulfilled but value is not array:`, result.value);
        // Handle non-array fulfillment if necessary, maybe push an error object
         combinedData.push({ regNo: 'Unknown', status: 'Error', reason: `Worker Error: Expected array but got other type.` });
    }
  });
  
  // Remove potential duplicates just in case (e.g., if probe batch somehow got added again)
  const uniqueData = Array.from(new Map(combinedData.map(item => [item.regNo, item])).values());

  // Sort
  const finalSortedData = uniqueData.sort((a, b) => {
      if (a.regNo === 'Unknown') return 1; 
      if (b.regNo === 'Unknown') return -1;
      // Ensure regNo exists before comparing
      return (a.regNo || "").localeCompare(b.regNo || "");
  });

  console.log(`[${Date.now()}] Returning ${finalSortedData.length} unique results immediately.`);

  // 8. Return results
  return new Response(JSON.stringify(finalSortedData), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

/**
 * --- Core Caching Function ---
 */
async function getCachedOrFetchBatch(baseUrl, regNo, queryParams, env, ctx, isCronRetry) {
  const { year, semester, exam_held } = queryParams;
  const targetUrl = `${baseUrl}?reg_no=${regNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(exam_held)}`;
  
  const cacheKey = new Request(targetUrl);
  const kvKey = `${baseUrl === REGULAR_BACKEND_URL ? 'REGULAR_URL' : 'LE_URL'}::${targetUrl}`; 

  const functionStartTime = Date.now();
  console.log(`[${functionStartTime}] getCachedOrFetchBatch called for ${regNo}. CronRetry: ${isCronRetry}`);

  try {
    // 1. Check Cache
    const cachedResponse = await caches.default.match(cacheKey);
    if (cachedResponse) {
      console.log(`[${functionStartTime}] Cache HIT for batch starting ${regNo}`);
      // Ensure removed from KV (async)
      ctx.waitUntil(env.RETRY_QUEUE.delete(kvKey).catch(e => console.error(`[${functionStartTime}] Error deleting KV key ${kvKey} after cache hit: ${e}`)));
      // Need to await json() parsing
      const jsonData = await cachedResponse.json(); 
      console.log(`[${functionStartTime}] Cache HIT finished for ${regNo} in ${Date.now() - functionStartTime}ms.`);
      return jsonData;
    }

    // 2. Cache MISS: Fetch from Vercel
    console.log(`[${functionStartTime}] Cache MISS for batch starting ${regNo}. Fetching...`);
    const fetchStartTime = Date.now();
    const freshBatch = await fetchVercelBatch(baseUrl, regNo, queryParams);
    console.log(`[${functionStartTime}] Vercel fetch finished for ${regNo} in ${Date.now() - fetchStartTime}ms.`);


    // 3. Analyze the result
    // Ensure freshBatch is an array before checking .some()
    const isBadBatch = Array.isArray(freshBatch) && freshBatch.some(r => r && r.status && r.status.includes('Error'));
    
    if (isBadBatch) {
      console.log(`[${functionStartTime}] Fetch for batch starting ${regNo} FAILED.`);
      if (!isCronRetry) {
          try {
              const exists = await env.RETRY_QUEUE.get(kvKey);
              if (!exists) {
                console.log(`[${functionStartTime}] Queuing failed batch ${kvKey} into KV.`);
                // Add TTL to KV entry so it doesn't stay forever if unfixable
                ctx.waitUntil(env.RETRY_QUEUE.put(kvKey, "failed", { expirationTtl: CACHE_TTL })); 
              } else {
                 console.log(`[${functionStartTime}] Batch ${kvKey} already in KV queue.`);
              }
          } catch(e) {
              console.error(`[${functionStartTime}] Error putting failed batch ${kvKey} into KV: ${e}`);
          }
      } else {
          console.log(`[${functionStartTime}] Cron retry failed for ${kvKey}. Will remain in KV.`);
      }
      console.log(`[${functionStartTime}] getCachedOrFetchBatch (FAIL) finished for ${regNo} in ${Date.now() - functionStartTime}ms.`);
      return freshBatch; 
      
    } else if (Array.isArray(freshBatch)) { // Check if it's a valid array before caching
      // GOOD BATCH
      console.log(`[${functionStartTime}] Fetch for batch starting ${regNo} SUCCEEDED. Caching.`);
      const responseToCache = new Response(JSON.stringify(freshBatch), {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': `public, max-age=${CACHE_TTL}`
        }
      });
      
      // Cache it & ensure removed from KV (async)
      ctx.waitUntil(caches.default.put(cacheKey, responseToCache.clone()).catch(e => console.error(`[${functionStartTime}] Error putting response into cache for ${regNo}: ${e}`)));
      ctx.waitUntil(env.RETRY_QUEUE.delete(kvKey).catch(e => console.error(`[${functionStartTime}] Error deleting KV key ${kvKey} after successful fetch: ${e}`))); 
      
      console.log(`[${functionStartTime}] getCachedOrFetchBatch (SUCCESS) finished for ${regNo} in ${Date.now() - functionStartTime}ms.`);
      return freshBatch; 
    } else {
      // Handle case where freshBatch is not an array (unexpected)
       console.error(`[${functionStartTime}] Fetch for ${regNo} returned non-array:`, freshBatch);
       return [{ regNo: regNo, status: 'Error', reason: 'Worker Error: Invalid batch format received' }];
    }
  } catch (error) {
      console.error(`[${functionStartTime}] Error in getCachedOrFetchBatch for ${regNo}: ${error.stack}`);
      return [{ regNo: regNo, status: 'Error', reason: `Worker Internal Error: ${error.message}` }];
  }
}

/** --- Helper: fetchInBatches --- */
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

/** --- Helper: fetchVercelBatch --- */
async function fetchVercelBatch(baseUrl, regNo, queryParams) {
  // ... (Keep the previous robust version with JSON parsing check) ...
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
      const errorText = await response.text(); 
      console.warn(`Vercel backend error for ${regNo}: ${response.status} ${response.statusText} - ${errorText}`);
      return [{ regNo: regNo, status: 'Error', reason: `Backend Error: HTTP ${response.status}` }];
    }

    try {
        const data = await response.json();
        return Array.isArray(data) ? data : [{ regNo: regNo, status: 'Error', reason: 'Backend Response Invalid Format' }];
    } catch (jsonError) {
        console.error(`Failed to parse JSON response from ${targetUrl}: ${jsonError}`);
        return [{ regNo: regNo, status: 'Error', reason: `Backend Response JSON Parse Error` }];
    }

  } catch (error) {
    clearTimeout(timeoutId);
    let reason = error.name === 'AbortError' ? 'Request Timed Out (30s)' : error.message;
    console.warn(`FetchVercelBatch failed for ${regNo}: ${reason}`);
    return [{ regNo: regNo, status: 'Error', reason: `Fetch Failed: ${reason}` }];
  }
}
