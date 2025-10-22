// Set thread pool size BEFORE any libuv-using module is required
process.env.UV_THREADPOOL_SIZE = 16;
console.log('UV_THREADPOOL_SIZE (set in script):', process.env.UV_THREADPOOL_SIZE);

const os = require('os');
const zlib = require('zlib');

const cpus = os.cpus().length;
console.log('CPUs available:', cpus);

const buffers = Array.from({ length: 16 }, () => Buffer.alloc(100_000_000, 'a'));
const total = buffers.length;
const start = Date.now();
let completed = 0;

console.log(`Starting async gzip of ${total} buffers... (threadpool=${process.env.UV_THREADPOOL_SIZE})`);

buffers.forEach((buf, i) => {
  const scheduledAt = Date.now() - start;
  console.log(`Task ${i} scheduled at ${scheduledAt} ms`);
  zlib.gzip(buf, (err, result) => {
    const doneAt = Date.now() - start;
    if (err) {
      console.error(`Task ${i} ERROR at ${doneAt} ms:`, err);
    } else {
      console.log(`Task ${i} done at ${doneAt} ms (output ${result.length} bytes)`);
    }
    completed++;
    if (completed === total) {
      const totalMs = Date.now() - start;
      console.log(`All async gzip tasks completed in ${totalMs} ms`);
    }
  });
});

console.log('All gzip tasks scheduled');
