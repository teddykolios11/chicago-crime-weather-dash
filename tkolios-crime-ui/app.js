'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')

const url = new URL(process.argv[3]);
console.log(url)
var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port,
	protocol: url.protocol.slice(0, -1),
	encoding: 'latin1'
});

function counterToNumber(c) {
	return Number(Buffer.from(c, 'latin1').readBigInt64BE());
}

function cellToDouble(c) {
  const buf = Buffer.from(c, 'latin1');
  return buf.readDoubleBE();
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

// helper to snap value to nearest bin
function snapToBins(value, bins) {
  let best = bins[0];
  let bestDiff = Math.abs(value - best);
  for (const b of bins) {
    const d = Math.abs(value - b);
    if (d < bestDiff) {
      best = b;
      bestDiff = d;
    }
  }
  return best;
}


app.use(express.static('public'));

app.get('/crime.html', (req, res) => {
  const templatePath = __dirname + "/views/crime_dashboard.mustache";
  const template = filesystem.readFileSync(templatePath).toString();
  const html = mustache.render(template, {});
  res.send(html);
});

app.get('/api/crime-by-hour', (req, res) => {
  const rowKey = 'ALL';

  hclient.table('tkolios_crime_by_hour').row(rowKey).get((err, cells) => {
    if (err) {
      console.error(err);
      res.status(500).json({ error: 'HBase error' });
      return;
    }

    const stats = rowToMap(cells);
    const hours = [];
    for (let h = 0; h < 24; h++) {
      const col = `count:${h}`;
      const count = stats[col] || 0;
      hours.push({ hour: h, count });
    }
    res.json(hours);
  });
});

app.get('/api/crime-feature-importance', (req, res) => {
  const rowKey = 'model_v1';

  hclient
    .table('tkolios_crime_feature_importance').row(rowKey).get((err, cells) => {
      if (err) {
        console.error(err);
        res.status(500).json({ error: 'HBase error' });
        return;
      }

      const stats = rowToMap(cells);

      const features = cells
        .filter(item => item.column.startsWith('imp:'))
        .map(item => {
          const col = item.column;
          const featureName = col.split(':')[1];
          const importance = cellToDouble(item.$);  // decode as double
          return { feature: featureName, importance };
        })
        .sort((a, b) => b.importance - a.importance); // sort descending importance

      res.json(features);
    });
});

app.get('/api/predict', (req, res) => {
  try {
    const hour = parseInt(req.query.hour, 10);      
    const dow  = parseInt(req.query.dow, 10);

    const tmax = parseFloat(req.query.tmax);
    const prcp = parseFloat(req.query.prcp || '0');
    const snow = parseFloat(req.query.snow || '0');
    const snwd = parseFloat(req.query.snwd || '0');

    // snap to same bins used in scala
    const tmaxBin = snapToBins(tmax, [-10, 0, 10, 20, 30, 40]);
    const prcpBin = prcp <= 0.0 ? 0 : 5;
    const snowBin = snow <= 0.0 ? 0 : 5;
    const snwdBin = snwd <= 0.0 ? 0 : 10;

    const rowKey = `${dow}_${hour}_${tmaxBin}_${prcpBin}_${snowBin}_${snwdBin}`;

    hclient
      .table('tkolios_crime_predictor').row(rowKey).get((err, cells) => {

        console.log('HBase cells for', rowKey, JSON.stringify(cells, null, 2));

        if (err) {
          console.error(err);
          return res.status(500).json({ error: 'HBase error' });
        }

        if (!cells || cells.length === 0) {
          return res.status(404).json({ error: 'no prediction found for this combination' });
        }

        const yHatCell = cells.find(c => c.column === 'pred:y_hat');
        const yHat = cellToDouble(yHatCell.$);

        return res.json({
          rowKey,
          predicted_crime_count: yHat
        });
      });

  } catch (e) {
    console.error(e);
    res.status(400).json({ error: 'invalid input' });
  }
});


app.listen(port);
