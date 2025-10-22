// Do NOT set UV_THREADPOOL_SIZE here — this will run synchronously on main thread
console.log('UV_THREADPOOL_SIZE (env):', process.env.UV_THREADPOOL_SIZE || '(not set here)');
const os = require('os');
const zlib = require('zlib');

const cpus = os.cpus().length;
console.log('CPUs available:', cpus);

const buffers = Array.from({ length: 16 }, () => Buffer.alloc(100_000_000, 'a'));
const total = buffers.length;
const start = Date.now();

console.log(`Starting synchronous gzipSync of ${total} buffers (blocking main thread)...`);

for (let i = 0; i < buffers.length; i++) {
  const before = Date.now() - start;
  console.log(`Task ${i} start at ${before} ms`);
  try {
    const out = zlib.gzipSync(buffers[i]);
    const doneAt = Date.now() - start;
    console.log(`Task ${i} done at ${doneAt} ms (output ${out.length} bytes)`);
  } catch (err) {
    console.error(`Task ${i} ERROR:`, err);
  }
}

const totalMs = Date.now() - start;
console.log(`All synchronous gzipSync tasks completed in ${totalMs} ms`);
