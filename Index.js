const express = require("express");
const app = express();

let lastHeavyTime = null;
let lastFastTime = null;

// Heavy CPU task (simulate prime calculation or loop)
app.get("/heavy", (req, res) => {
  const start = Date.now();
  let sum = 0;
  const total = 1e10;
  for (let i = 0; i < total; i++) {
    sum += i;
    // Log every 1% progress
    if (i % (total / 100) === 0) {
      const percent = ((i / total) * 100).toFixed(2);
      console.log(`Heavy CPU running: ${percent}% complete (i=${i})`);
    }
  }
  const end = Date.now();
  const responseTime = end - start;
  lastHeavyTime = responseTime;
  res.send(`
    <html>
      <head>
        <title>Heavy Endpoint - Old OS UI</title>
        <style>
          body { font-family: monospace; background: #e0e0e0; }
          .window { border: 2px solid #333; background: #fff; width: 400px; margin: 40px auto; padding: 20px; box-shadow: 4px 4px 8px #888; }
          .title { background: #333; color: #fff; padding: 4px 8px; font-weight: bold; }
          .log { background: #222; color: #0f0; padding: 10px; margin-top: 10px; font-size: 16px; }
        </style>
      </head>
      <body>
        <div class="window">
          <div class="title">[Heavy Endpoint Log]</div>
          <div class="log">
            Task: Heavy CPU Calculation<br>
            Result: Sum is ${sum}<br>
            Response Time: ${responseTime} ms<br>
            Last Fast Response Time: ${lastFastTime !== null ? lastFastTime + ' ms' : 'N/A'}<br>
            Difference: ${lastFastTime !== null ? (responseTime - lastFastTime) + ' ms' : 'N/A'}<br>
            <hr>
            <a href="/fast">Go to Fast Endpoint</a>
          </div>
        </div>
      </body>
    </html>
  `);
});

app.get("/fast", (req, res) => {
  const start = Date.now();
  const end = Date.now();
  const responseTime = end - start;
  lastFastTime = responseTime;
  res.send(`
    <html>
      <head>
        <title>Fast Endpoint - Old OS UI</title>
        <style>
          body { font-family: monospace; background: #e0e0e0; }
          .window { border: 2px solid #333; background: #fff; width: 400px; margin: 40px auto; padding: 20px; box-shadow: 4px 4px 8px #888; }
          .title { background: #333; color: #fff; padding: 4px 8px; font-weight: bold; }
          .log { background: #222; color: #0f0; padding: 10px; margin-top: 10px; font-size: 16px; }
        </style>
      </head>
      <body>
        <div class="window">
          <div class="title">[Fast Endpoint Log]</div>
          <div class="log">
            Task: Fast Response<br>
            Response Time: ${responseTime} ms<br>
            Last Heavy Response Time: ${lastHeavyTime !== null ? lastHeavyTime + ' ms' : 'N/A'}<br>
            Difference: ${lastHeavyTime !== null ? (lastHeavyTime - responseTime) + ' ms' : 'N/A'}<br>
            <hr>
            <a href="/heavy">Go to Heavy Endpoint</a>
          </div>
        </div>
      </body>
    </html>
  `);
});

app.listen(3000, () => console.log("Server running on http://localhost:3000"));

