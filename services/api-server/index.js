const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

/**
 * DB 연결 설정
 * 환경 변수 우선 순위 및 기본값 설정
 */
const pool = new Pool({
  host: process.env.DB_HOST || 'localhost',
  database: process.env.POSTGRES_DB || 'trading_db',
  user: process.env.POSTGRES_USER || 'admin',
  password: process.env.POSTGRES_PASSWORD || 'stock123',
  port: parseInt(process.env.DB_PORT || '5432'),
});

/**
 * 실시간 시그널 조회 API
 * 정렬(sortBy) 및 테마 그룹화, 보유 종목 매핑 포함
 */
app.get('/api/signals', async (req, res) => {
  const { sortBy } = req.query;

  // 정렬 화이트리스트 및 매핑
  const sortOptions = {
    '1': 's.trade_value DESC',                  // 거래대금 순
    '2': 's.profit_rate DESC',                 // 등락률 순
    '3': 's.cap_time_3 DESC NULLS LAST',       // 신고가 포착시점 순
    '4': 's.volume DESC',                      // 거래량 순
    '5': 's.cap_time_5 DESC NULLS LAST',       // 단주거래 포착시점 순
    '6': 's.current_price ASC',                // 가격 낮은순 (BB하단)
    '7': 's.current_price DESC',               // 가격 높은순 (BB상단)
    'default': 's.scores DESC, s.profit_rate DESC' // 기본: 점수 및 등락률
  };

  const orderBy = sortOptions[sortBy] || sortOptions['default'];

  try {
    const query = `
      SELECT *
      FROM detected_signals
      ORDER BY updated_at DESC
      LIMIT 50;
    `;

    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error("API Error message:", err.message);
    console.error("API Error stack:", err.stack);
    res.status(500).json({ 
      error: '데이터베이스 조회 중 오류가 발생했습니다.',
      details: process.env.NODE_ENV === 'development' ? err.stack : undefined 
    });
  }
});

/**
 * 포트폴리오 업데이트 API (샘플)
 * 사용자가 종목을 매수/매도했을 때 동기화
 */
app.post('/api/portfolio', async (req, res) => {
  const { symbol, quantity, avg_price } = req.body;
  try {
    const query = `
      INSERT INTO my_portfolio (symbol, quantity, avg_price, updated_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (symbol) 
      DO UPDATE SET 
        quantity = EXCLUDED.quantity,
        avg_price = EXCLUDED.avg_price,
        updated_at = NOW();
    `;
    await pool.query(query, [symbol, quantity, avg_price]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 서버 기동
const PORT = process.env.PORT || 3001;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[Trading System] API Server started on port ${PORT}`);
});