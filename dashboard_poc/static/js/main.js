document.getElementById("btn-compare").addEventListener("click", function(){
    const startDate = document.getElementById("start-date").value;
    const endDate   = document.getElementById("end-date").value;
    const symbols   = document.getElementById("symbols").value.split(",");
    const dbkeys    = document.getElementById("dbkeys").value.split(",");

    const params = new URLSearchParams();
    params.append("start_date", startDate);
    params.append("end_date", endDate);
    params.append("symbols", symbols.join(","));
    params.append("dbkeys", dbkeys.join(","));

    fetch("/api/series?" + params.toString())
      .then(response => response.json())
      .then(data => {
        const traces = [];

        for (let i = 0; i < symbols.length; i++) {
          const sym = symbols[i];
          const dbk = dbkeys[i];
          const s = data[sym];

          // 미국 주식(stock_us)은 오른쪽 Y축(y2)에 표시
          const isUS = (dbk === "stock_us");

          traces.push({
            x: s.dates,
            y: s.close,
            name: sym + (isUS ? " (USD)" : " (KRW)"),
            mode: "lines",
            yaxis: isUS ? "y2" : "y"
          });
        }

        const layout = {
          title: "시계열 비교 (원화 vs 달러)",
          xaxis: { title: "날짜" },
          yaxis: {
            title: "원화 (₩)",
            showgrid: true,
            zeroline: true,
          },
          yaxis2: {
            title: "달러 ($)",
            overlaying: "y",
            side: "right",
            showgrid: false
          },
          legend: { x: 0, y: 1.1, orientation: "h" },
          hovermode: "x unified"
        };

        Plotly.newPlot("chart", traces, layout);
      })
      .catch(err => console.error(err));
});
