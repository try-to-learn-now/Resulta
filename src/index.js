// --- Configuration: Vercel Backends ---
const REGULAR_BACKEND_URL = 'https://multi-result-beu-regular.vercel.app/api/regular/result';
const LE_BACKEND_URL = 'https://multi-result-beu-le.vercel.app/api/le/result';

// --- Configuration: Cache & Limits ---
const CACHE_TTL = 4 * 24 * 60 * 60; // 4 days in seconds
const CONCURRENCY_LIMIT = 1;       // Fetch one Vercel batch at a time (Critical for Free Tier)
const INITIAL_FETCH_LIMIT = 20;    // Max batches to fetch *immediately* for user (Keeps below 50 limit)
const CRON_FETCH_LIMIT = 20;       // Max batches cron job fetches per run (Keeps below 50 limit)

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

/**
 * --- ES Module Entry Point ---
 * 1. 'fetch': Handles the user's request
 * 2. 'scheduled': Handles the cron job (every 5 minutes)
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
      console.error(`Critical Error in fetch handler: ${error.stack}`);
      return new Response(JSON.stringify({ error: 'An unexpected server error occurred.' }), {
        status: 500, headers: CORS_HEADERS
      });
    }
  },

  /**
   * --- 2. THE SCHEDULED HANDLER (for the "cron job") ---
   * This runs automatically every 5 minutes (from wrangler.toml)
   */
  async scheduled(event, env, ctx) {
    console.log("Cron job running: Processing KV retry queue...");
    
    // Get batches from the "to-do list" in KV
    const { keys } = await env.RETRY_QUEUE.list({ limit: CRON_FETCH_LIMIT }); 

    if (keys.length === 0) {
        console.log("KV queue is empty. Nothing to retry.");
        return; // Exit early if nothing to do
    }

    const tasks = []; // Array to hold the async functions to run
    for (const key of keys) {
        // The key name includes type: "REGULAR_URL::..." or "LE_URL::..."
        const [type, targetUrl] = key.name.split("::"); 
        
        // Basic validation in case of malformed keys
        if (!targetUrl || (type !== "REGULAR_URL" && type !== "LE_URL")) {
            console.warn(`Skipping invalid KV key: ${key.name}`);
            // Optionally delete the invalid key
            // ctx.waitUntil(env.RETRY_QUEUE.delete(key.name)); 
            continue; 
        }

        // Create an async function for each task
        tasks.push(async () => {
            console.log(`Cron retrying: ${targetUrl}`);
            // Rebuild params needed for fetchVercelBatch
            const url = new URL(targetUrl);
            const params = url.searchParams;
            const regNo = params.get('reg_no'); // Needed by fetchVercelBatch
            const queryParams = { // Needed by fetchVercelBatch
                year: params.get('year'),
                semester: params.get('semester'),
                exam_held: params.get('exam_held')
            };

            const baseUrl = type === "REGULAR_URL" ? REGULAR_BACKEND_URL : LE_BACKEND_URL;

            // Fetch (this also caches on success)
            // Pass `true` to indicate this is a cron retry
            const freshBatch = await getCachedOrFetchBatch(baseUrl, regNo, queryParams, env, ctx, true); 

            // If it's *still* bad, getCachedOrFetchBatch already logged it. It remains in KV.
            // If it's good, getCachedOrFetchBatch caches it and *removes it* from KV.
            const isBadBatch = freshBatch.some(r => r.status.includes('Error'));
            if (!isBadBatch) {
                console.log(`Cron SUCCESS for ${targetUrl}. Removed from KV by getCachedOrFetchBatch.`);
                // No need to delete here, getCachedOrFetchBatch does it.
            } else {
                 console.log(`Cron FAILED for ${targetUrl}. Will remain in KV.`);
            }
        });
    }

    try {
      // Run cron tasks serially (CONCURRENCY_LIMIT = 1) to avoid overwhelming Vercel/BEU
      await fetchInBatches(tasks, CONCURRENCY_LIMIT); 
    } catch (error) {
        console.error(`Error during scheduled batch execution: ${error.stack}`);
        // Don't throw, allow cron to finish and try again next time
    }
    
    console.log(`Cron job finished processing ${keys.length} potential retries.`);
  }
};

/**
 * --- Main Request Handler Logic ---
 * Handles the incoming request from the user
 */
async function handleGetRequest(request, env, ctx) {
  const url = new URL(request.url);
  const params = url.searchParams;

  // 1. Get & Validate Parameters
  const regNo = params.get('reg_no');
  const year = params.get('year');
  const semester = params.get('semester');
  const examHeld = params.get('exam_held');

  if (!regNo || !/^\d{11}$/.test(regNo)) { return new Response(JSON.stringify({ error: 'Invalid "reg_no"' }), { status: 400, headers: CORS_HEADERS }); }
  if (!year || !semester || !examHeld) { return new Response(JSON.stringify({ error: 'Missing parameters' }), { status: 400, headers: CORS_HEADERS }); }
  
  const queryParams = { year, semester, exam_held: examHeld };

  // 2. Parse Reg No
  const firstTwo = regNo.slice(0, 2);
  const restReg = regNo.slice(2, -3);
  const suffixNum = parseInt(regNo.slice(-3));

  let regularPrefix, lePrefix, probeBaseUrl, isRegularStudent;
  let userBatchStartRegNo; // The starting reg_no for the user's batch

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

  // 3. Probe User's Batch (checks cache first)
  console.log(`Probing user's batch first: ${userBatchStartRegNo}`);
  const probeBatchResults = await getCachedOrFetchBatch(
    probeBaseUrl, userBatchStartRegNo, queryParams, env, ctx, false // `false` = not a cron retry
  );
  
  // 4. Smart Range Logic (determine if small or big college ranges needed)
  let regRange = SMALL_REG_RANGE;
  let leRange = SMALL_LE_RANGE;
  const isBigCollegeAttempt = (isRegularStudent && suffixNum > SMALL_REG_RANGE[1]) || (!isRegularStudent && suffixNum > SMALL_LE_RANGE[1]);
  const isProbeSuccessful = Array.isArray(probeBatchResults) && probeBatchResults.some(r => r.status === 'success');

  if (isBigCollegeAttempt && isProbeSuccessful) {
    console.log(`Probe successful! CONFIRMED Big College. Expanding range.`);
    regRange = BIG_REG_RANGE;
    leRange = BIG_LE_RANGE;
  } else {
    console.log(`Standard college range detected. Using default small range.`);
  }

  // 5. Split Fetch Tasks into Initial (immediate) and Deferred (KV)
  const initialFetchTasks = []; // Array of functions to call immediately
  const deferredFetchKeys = []; // Array of KV keys (URLs) to put in KV for cron

  const [regStart, regEnd] = regRange;
  const [leStart, leEnd] = leRange;

  // Prepare Regular tasks/keys
  for (let i = regStart; i <= regEnd; i += BATCH_STEP) {
    const batchRegNo = regularPrefix + String(i).padStart(3, '0');
    if (isRegularStudent && batchRegNo === userBatchStartRegNo) continue; // Skip the batch we already probed

    const task = () => getCachedOrFetchBatch(REGULAR_BACKEND_URL, batchRegNo, queryParams, env, ctx, false);
    const targetUrl = `${REGULAR_BACKEND_URL}?reg_no=${batchRegNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(examHeld)}`;
    const kvKey = `REGULAR_URL::${targetUrl}`; // Prefix KV key with type

    if (initialFetchTasks.length < INITIAL_FETCH_LIMIT) {
      initialFetchTasks.push(task);
    } else {
      deferredFetchKeys.push(kvKey); 
    }
  }

  // Prepare LE tasks/keys
  for (let i = leStart; i <= leEnd; i += BATCH_STEP) {
    const batchRegNo = lePrefix + String(i).padStart(3, '0');
    if (!isRegularStudent && batchRegNo === userBatchStartRegNo) continue; // Skip the batch we already probed

    const task = () => getCachedOrFetchBatch(LE_BACKEND_URL, batchRegNo, queryParams, env, ctx, false);
    const targetUrl = `${LE_BACKEND_URL}?reg_no=${batchRegNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(examHeld)}`;
    const kvKey = `LE_URL::${targetUrl}`; // Prefix KV key with type

    if (initialFetchTasks.length < INITIAL_FETCH_LIMIT) {
      initialFetchTasks.push(task);
    } else {
      deferredFetchKeys.push(kvKey);
    }
  }

  // 6. Execute Initial Fetches & Queue Deferred Fetches
  console.log(`Executing ${initialFetchTasks.length} initial fetches serially...`);
  const initialResults = await fetchInBatches(initialFetchTasks, CONCURRENCY_LIMIT);

  // Put deferred tasks into KV (do this in background, don't wait)
  if (deferredFetchKeys.length > 0) {
      console.log(`Queueing ${deferredFetchKeys.length} deferred batches into KV...`);
      // We only put keys that aren't already likely cached or in the queue
      const checkAndPutPromises = deferredFetchKeys.map(async (key) => {
          const exists = await env.RETRY_QUEUE.get(key); // Check if already queued
          const cacheKey = new Request(key.split("::")[1]);
          const cached = await caches.default.match(cacheKey); // Check if already cached
          if (!exists && !cached) {
              return env.RETRY_QUEUE.put(key, "deferred");
          }
      });
      ctx.waitUntil(Promise.all(checkAndPutPromises));
  }

  // 7. Process Final Results (for *this* user request)
  let combinedData = [...probeBatchResults]; // Start with the user's batch
  
  initialResults.forEach(result => { // Add results from the initial synchronous fetch
    if (result.status === 'fulfilled' && Array.isArray(result.value)) {
      combinedData.push(...result.value);
    } 
    else if (result.status === 'rejected') {
        // Log the reason for the rejection
        console.error(`Initial fetch promise rejected: ${result.reason}`);
        combinedData.push({ regNo: 'Unknown', status: 'Error', reason: `Worker Error during initial fetch: ${result.reason}` });
    }
  });
  
  // Sort only the data we are returning *now*
  const finalSortedData = combinedData.sort((a, b) => {
      // Handle potential "Unknown" regNo from errors
      if (a.regNo === 'Unknown') return 1; 
      if (b.regNo === 'Unknown') return -1;
      return a.regNo.localeCompare(b.regNo);
  });

  console.log(`Returning ${finalSortedData.length} results immediately.`);

  // 8. Return results to user
  return new Response(JSON.stringify(finalSortedData), {
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
  });
}

/**
 * --- Core Caching Function ---
 * Fetches from Vercel if not in cache. Caches good results. Puts bad results in KV (if not cron).
 * Returns the fetched/cached batch data (Array).
 */
async function getCachedOrFetchBatch(baseUrl, regNo, queryParams, env, ctx, isCronRetry) {
  const { year, semester, exam_held } = queryParams;
  const targetUrl = `${baseUrl}?reg_no=${regNo}&year=${year}&semester=${semester}&exam_held=${encodeURIComponent(exam_held)}`;
  
  const cacheKey = new Request(targetUrl);
  // Define KV key with type prefix for cron job routing
  const kvKey = `${baseUrl === REGULAR_BACKEND_URL ? 'REGULAR_URL' : 'LE_URL'}::${targetUrl}`; 

  try {
    // 1. Check Cache
    const cachedResponse = await caches.default.match(cacheKey);
    if (cachedResponse) {
      console.log(`Cache HIT for batch starting ${regNo}`);
      // If found in cache, ensure it's removed from KV (in case cron hadn't yet)
      ctx.waitUntil(env.RETRY_QUEUE.delete(kvKey));
      return cachedResponse.json(); // Return cached data
    }

    // 2. Cache MISS: Fetch from Vercel
    console.log(`Cache MISS for batch starting ${regNo}. Fetching...`);
    const freshBatch = await fetchVercelBatch(baseUrl, regNo, queryParams);

    // 3. Analyze the result
    const isBadBatch = freshBatch.some(r => r.status.includes('Error'));
    
    if (isBadBatch) {
      console.log(`Fetch for batch starting ${regNo} FAILED.`);
      // Only put into KV if it's NOT already a cron retry AND not already in KV
      if (!isCronRetry) {
          const exists = await env.RETRY_QUEUE.get(kvKey);
          if (!exists) {
            console.log(`Queuing failed batch ${kvKey} into KV.`);
            ctx.waitUntil(env.RETRY_QUEUE.put(kvKey, "failed"));
          } else {
             console.log(`Batch ${kvKey} already in KV queue.`);
          }
      } else {
          console.log(`Cron retry failed for ${kvKey}. Will remain in KV.`);
      }
      return freshBatch; // Return the error batch to the caller
      
    } else {
      // GOOD BATCH (Success or RecordNotFound)
      console.log(`Fetch for batch starting ${regNo} SUCCEEDED. Caching.`);
      const responseToCache = new Response(JSON.stringify(freshBatch), {
        headers: {
          'Content-Type': 'application/json',
          'Cache-Control': `public, max-age=${CACHE_TTL}`
        }
      });
      
      // Cache it & ensure removed from KV
      ctx.waitUntil(caches.default.put(cacheKey, responseToCache.clone()));
      ctx.waitUntil(env.RETRY_QUEUE.delete(kvKey)); // Remove from retry queue if it succeeded
      return freshBatch; // Return the good batch data
    }
  } catch (error) {
      // Catch potential errors in cache operations or JSON parsing
      console.error(`Error in getCachedOrFetchBatch for ${regNo}: ${error.stack}`);
      // Return an error object similar to fetchVercelBatch's failure case
      return [{ regNo: regNo, status: 'Error', reason: `Worker Internal Error: ${error.message}` }];
  }
}

/** * --- Helper Function: Pooled Concurrency ---
 * Runs an array of async functions sequentially (since batchSize=1)
 */
async function fetchInBatches(promiseFunctions, batchSize) {
  // Note: With batchSize = 1, this effectively runs promises sequentially
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
 * Fetches one batch (5 students) from Vercel. Returns Array. Never rejects.
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
      const errorText = await response.text(); // Try to get more info
      console.warn(`Vercel backend error for ${regNo}: ${response.status} ${response.statusText} - ${errorText}`);
      return [{ regNo: regNo, status: 'Error', reason: `Backend Error: HTTP ${response.status}` }];
    }

    // Try to parse JSON, handle potential errors
    try {
        const data = await response.json();
        // Ensure it's always an array
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
