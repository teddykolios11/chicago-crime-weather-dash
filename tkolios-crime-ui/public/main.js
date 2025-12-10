document.addEventListener('DOMContentLoaded', () => {
  const crimeHourCanvas = document.getElementById('crimeByHourChart');
  const ctx = crimeHourCanvas.getContext('2d');

  fetch('/api/crime-by-hour')
    .then(res => res.json())
    .then(data => {
      data.sort((a, b) => a.hour - b.hour);
      const labels = data.map(d => `${d.hour}:00`);
      const counts = data.map(d => d.count);

      new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: 'Crime count', data: counts }] },
        options: { responsive: true, maintainAspectRatio: false }
      });
    });

  const fiCanvas = document.getElementById('featureImportanceChart');
  const ctxFI = fiCanvas.getContext('2d');

  fetch('/api/crime-feature-importance')
      .then(res => res.json())
      .then(data => {
        const labels = data.map(d => d.feature);
        const values = data.map(d => d.importance);

        new Chart(ctxFI, {
          type: 'bar',
          data: { labels, datasets: [{ label: 'Importance', data: values, borderWidth: 1}]},
          options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: { title: { display: true, text: 'Importance' } },
              y: { title: { display: true, text: 'Feature' } }
            },
            plugins: {
              legend: { display: false },
              title: { display: true, text: 'Crime model feature importance' },
              tooltip: {
                callbacks: {
                  label: (ctx) => `Importance: ${ctx.parsed.x.toFixed(3)}`
                }
              }
            }
          }
        });
      });
});

document.getElementById('predict-form').addEventListener('submit', async function (e) {
    e.preventDefault();
    const form = e.target;

    const params = new URLSearchParams({
      dow:  form.dow.value,
      hour: form.hour.value,
      tmax: form.tmax.value,
      prcp: form.prcp.value,
      snow: form.snow.value,
      snwd: form.snwd.value
    });

    const resp = await fetch('/api/predict?' + params.toString());
    const data = await resp.json();

    const container = document.getElementById('predict-result');

    if (data.error) {
      container.innerHTML = '<p style="color:red;">' + data.error + '</p>';
    } else {
      container.innerHTML = `
        <p><strong>Predicted hourly crime count:</strong> ${data.predicted_crime_count?.toFixed(2)}</p>
      `;
    }
  });