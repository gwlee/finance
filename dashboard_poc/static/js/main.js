// 전역 변수로 심볼 데이터를 저장
let allSymbols = {};
// 선택된 비교 항목 목록을 저장 (심볼: DB키 형태)
let selectedItems = {}; // 예: {"USD/KRW": "currency", "005930.KS": "stock_kr"}

// 1. 심볼 데이터 로딩
function loadSymbols() {
    fetch("/api/symbols")
        .then(response => response.json())
        .then(data => {
            allSymbols = data;
            console.log("Symbols loaded:", allSymbols);
            // 항목 유형 선택 드롭다운 활성화
            document.getElementById("db-type-select").disabled = false;
        })
        .catch(err => {
            console.error("Failed to load symbols:", err);
            alert("심볼 데이터를 불러오는 데 실패했습니다.");
        });
}

// 2. 항목 유형 변경 시 심볼 드롭다운 업데이트
document.getElementById("db-type-select").addEventListener("change", function() {
    const dbKey = this.value;
    const symbolSelect = document.getElementById("symbol-select");
    
    // 기존 옵션 삭제
    symbolSelect.innerHTML = '<option value="" disabled selected>심볼 선택</option>';
    
    if (dbKey && allSymbols[dbKey]) {
        // 심볼 옵션 추가
        allSymbols[dbKey].forEach(symbol => {
            const option = document.createElement("option");
            option.value = symbol;
            option.textContent = symbol;
            symbolSelect.appendChild(option);
        });
        symbolSelect.disabled = false;
    } else {
        symbolSelect.disabled = true;
    }
});

// 3. 비교 항목 추가 로직
document.getElementById("btn-add-item").addEventListener("click", function() {
    const dbTypeSelect = document.getElementById("db-type-select");
    const symbolSelect = document.getElementById("symbol-select");
    const dbKey = dbTypeSelect.value;
    const symbol = symbolSelect.value;

    if (!dbKey || !symbol) {
        alert("항목 유형과 심볼을 모두 선택해주세요.");
        return;
    }

    if (selectedItems.hasOwnProperty(symbol)) {
        alert(`'${symbol}'은(는) 이미 추가되었습니다.`);
        return;
    }

    // 목록에 추가
    selectedItems[symbol] = dbKey;
    updateCompareListUI();
});

// 4. 비교 항목 UI 업데이트 및 삭제 로직
function updateCompareListUI() {
    const compareList = document.getElementById("compare-list");
    compareList.innerHTML = ''; // 목록 초기화

    for (const symbol in selectedItems) {
        const dbKey = selectedItems[symbol];
        const listItem = document.createElement("li");
        
        // 항목 이름 표시 (예: USD/KRW [환율])
        let label = symbol;
        switch(dbKey) {
            case 'currency': label += ' [환율]'; break;
            case 'index':    label += ' [지수]'; break;
            case 'stock_kr': label += ' [한국/₩]'; break;
            case 'stock_us': label += ' [미국/$]'; break;
            default: break;
        }

        listItem.textContent = label;
        
        // 삭제 버튼 추가
        const deleteButton = document.createElement("button");
        deleteButton.textContent = "—";
        deleteButton.className = "delete-item-btn";
        deleteButton.dataset.symbol = symbol;

        deleteButton.addEventListener("click", function() {
            delete selectedItems[this.dataset.symbol];
            updateCompareListUI();
        });

        listItem.appendChild(deleteButton);
        compareList.appendChild(listItem);
    }
}


// 5. 차트 비교 버튼 클릭 이벤트 (메인 로직)
document.getElementById("btn-compare").addEventListener("click", function(){
    const startDate = document.getElementById("start-date").value;
    const endDate   = document.getElementById("end-date").value;
    
    const symbols = Object.keys(selectedItems);
    const dbkeys  = symbols.map(sym => selectedItems[sym]);

    if (symbols.length === 0) {
        alert("비교할 항목을 하나 이상 추가해주세요.");
        return;
    }
    if (!startDate || !endDate) {
         alert("시작일과 종료일을 모두 입력해주세요.");
         return;
    }

    const params = new URLSearchParams();
    params.append("start_date", startDate);
    params.append("end_date", endDate);
    params.append("symbols", symbols.join(","));
    params.append("dbkeys", dbkeys.join(","));

    // 데이터 가져오기 API 호출
    fetch("/api/series?" + params.toString())
      .then(response => response.json())
      .then(data => {
        const traces = [];

        for (let i = 0; i < symbols.length; i++) {
          const sym = symbols[i];
          const dbk = dbkeys[i];
          const s = data[sym];

          if (!s || s.dates.length === 0) {
            console.warn(`No data found for ${sym}`);
            continue;
          }

          // 미국 주식(stock_us)은 오른쪽 Y축(y2)에 표시 (달러 단위)
          // 나머지(환율, 지수, 한국 주식)는 왼쪽 Y축(y)에 표시 (원화/단위 없음)
          const isUS = (dbk === "stock_us");
          
          let nameLabel = sym;
          if (dbk === "currency") nameLabel += " (단위 없음)";
          else if (dbk === "index") nameLabel += " (단위 없음)";
          else if (dbk === "stock_kr") nameLabel += " (KRW)";
          else if (dbk === "stock_us") nameLabel += " (USD)";

          traces.push({
            x: s.dates,
            y: s.close,
            name: nameLabel,
            mode: "lines",
            yaxis: isUS ? "y2" : "y" // Y축 동적 지정
          });
        }
        
        if (traces.length === 0) {
            alert("선택된 기간에 대한 데이터가 없습니다.");
            Plotly.purge("chart"); // 기존 차트 제거
            return;
        }

        // Plotly 레이아웃 설정
        const layout = {
          title: "시계열 비교 (원화/단위 없음 vs 달러)",
          xaxis: { title: "날짜" },
          yaxis: {
            title: "원화 (₩) / 단위 없음", // 환율, 지수, 한국 주식
            showgrid: true,
            zeroline: true,
            side: "left"
          },
          yaxis2: {
            title: "달러 ($)", // 미국 주식
            overlaying: "y", // y축 위에 오버레이
            side: "right",   // 오른쪽에 표시
            showgrid: false,
            zeroline: false
          },
          legend: { x: 0, y: 1.1, orientation: "h" },
          hovermode: "x unified",
          margin: { t: 50, b: 50, l: 60, r: 60 } // Y축2 공간 확보
        };

        // 차트 그리기
        Plotly.newPlot("chart", traces, layout);
      })
      .catch(err => {
          console.error("Error fetching series data:", err);
          alert("데이터를 가져오는 중 오류가 발생했습니다.");
      });
});

// 페이지 로드 시 심볼 데이터 로드 시작
loadSymbols();