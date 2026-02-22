'use client';

import React, { useState, useEffect, useMemo } from 'react';
import { 
  Activity, Search, Star, Zap, TrendingUp, 
  ChevronRight, Bell, Menu, Filter, Clock,
  ArrowUpRight, ArrowDownRight, Flame, Target,
  RefreshCw, Layers
} from 'lucide-react';

/**
 * [Type Definitions]
 */
interface Stock {
  symbol: string;
  stock_name: string;
  current_price: number;
  profit_rate: number;
  scores: number;
  themes: string;
  trade_value: number;
  is_holding: boolean;
  cap_time_1?: string; // 거래대금
  cap_time_2?: string; // 급등
  cap_time_3?: string; // 신고가
  cap_time_4?: string; // 거래량
  cap_time_5?: string; // 단주
  cap_time_6?: string; // 저가/고가
}

interface SummaryData {
  total_monitored: number;
  new_highs: number;
  surging_stocks: number;
  last_updated: string;
}

/**
 * [Main Page Component]
 */
export default function App() {
  const [signals, setSignals] = useState<Stock[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('recommend');
  const [sortBy, setSortBy] = useState('score');
  const [searchTerm, setSearchTerm] = useState('');
  const [lastUpdated, setLastUpdated] = useState<string>('');

  // 실시간 데이터 호출 함수
  const fetchSignals = async () => {
    try {
      // const res = await fetch(`http://localhost:3001/api/signals?sortBy=${sortBy}`);
      const API_BASE = process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:4000';
      // const API_BASE = 'http://13.209.49.77:4000';
      const res = await fetch(`${API_BASE}/api/signals?sortBy=${sortBy}`);
      if (!res.ok) throw new Error('데이터를 불러오는데 실패했습니다.');
      const data = await res.json();
      
      if (Array.isArray(data)) {
        setSignals(data);
      }
      setLastUpdated(new Date().toLocaleTimeString('ko-KR', { hour12: false }));
    } catch (err) {
      console.error("Fetch error:", err);
      // 에러 발생 시 사용자에게 알림을 표시하거나 빈 상태 유지
    } finally {
      setLoading(false);
    }
  };

  // 초기 로드 및 주기적 업데이트 (10초)
  useEffect(() => {
    fetchSignals();
    const interval = setInterval(fetchSignals, 10000);
    return () => clearInterval(interval);
  }, [sortBy]);

  // 검색어 필터링 및 테마별 그룹화
  const groupedData = useMemo(() => {
    const filtered = signals.filter(s => 
      s.stock_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      s.symbol.includes(searchTerm) ||
      (s.themes && s.themes.includes(searchTerm))
    );

    return filtered.reduce((acc: Record<string, { name: string, items: Stock[] }>, curr: Stock) => {
      const themeName = curr.themes || '기타/개별이슈';
      if (!acc[themeName]) acc[themeName] = { name: themeName, items: [] };
      acc[themeName].items.push(curr);
      return acc;
    }, {});
  }, [signals, searchTerm]);

  // 요약 정보 계산
  const summary = useMemo<SummaryData>(() => ({
    total_monitored: signals.length,
    new_highs: signals.filter(s => s.cap_time_3).length,
    surging_stocks: signals.filter(s => s.profit_rate >= 5).length,
    last_updated: lastUpdated
  }), [signals, lastUpdated]);

  return (
    <div className="min-h-screen bg-[#F8FAFC] text-slate-900 font-sans selection:bg-blue-100">
      {/* Header */}
      <header className="bg-white/90 backdrop-blur-xl border-b border-slate-200 sticky top-0 z-50 px-4 md:px-8 h-16 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="bg-gradient-to-br from-blue-600 to-indigo-700 p-2 rounded-xl shadow-lg shadow-blue-200">
            <Activity className="text-white w-5 h-5" />
          </div>
          <span className="text-xl font-black tracking-tighter text-slate-800 uppercase">
            TRADING<span className="text-blue-600">MASTER</span>
          </span>
        </div>

        <div className="flex-1 max-w-xl mx-8 relative hidden lg:block">
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-400 w-4 h-4" />
          <input 
            type="text"
            placeholder="종목명 또는 테마를 입력하세요..."
            className="w-full bg-slate-100 border-transparent rounded-2xl py-2.5 pl-11 pr-4 text-sm focus:bg-white focus:ring-2 focus:ring-blue-500 transition-all outline-none border hover:border-slate-200"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>

        <div className="flex items-center gap-2">
          <div className="hidden sm:flex items-center gap-2 mr-4 px-3 py-1 bg-green-50 text-green-600 rounded-full border border-green-100">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
            <span className="text-[10px] font-black uppercase tracking-wider">Live Connection</span>
          </div>
          <button className="p-2.5 text-slate-500 hover:bg-slate-100 rounded-xl transition-colors"><Bell size={20} /></button>
          <button className="p-2.5 text-slate-500 hover:bg-slate-100 rounded-xl transition-colors"><Menu size={20} /></button>
        </div>
      </header>

      <main className="max-w-7xl mx-auto p-4 md:p-8 space-y-8">
        {/* Dashboard Summary Chips */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <SummaryCard icon={<Search className="text-blue-500" />} label="실시간 감시" value={summary.total_monitored} />
          <SummaryCard icon={<Flame className="text-orange-500" />} label="당일 신고가" value={summary.new_highs} />
          <SummaryCard icon={<Zap className="text-rose-500" />} label="당일 급등(5%↑)" value={summary.surging_stocks} />
          <SummaryCard icon={<Clock className="text-slate-500" />} label="최근 업데이트" value={summary.last_updated} isTime />
        </div>

        {/* Filters & Tabs */}
        <div className="flex flex-col gap-6">
          <div className="flex gap-2 overflow-x-auto pb-1 no-scrollbar">
            <TabButton active={activeTab === 'recommend'} onClick={() => setActiveTab('recommend')} icon={<Zap size={16}/>} label="추천 종목" />
            <TabButton active={activeTab === 'leader'} onClick={() => setActiveTab('leader')} icon={<TrendingUp size={16}/>} label="실시간 주도주" />
            <TabButton active={activeTab === 'my'} onClick={() => setActiveTab('my')} icon={<Star size={16}/>} label="보유 종목" />
          </div>

          <div className="flex items-center gap-2 overflow-x-auto pb-2 no-scrollbar border-b border-slate-100">
            <div className="flex items-center gap-1.5 px-4 py-2 bg-slate-800 text-white rounded-xl text-[10px] font-black uppercase tracking-widest mr-2">
              <Filter size={12} /> SORT BY
            </div>
            <SortChip active={sortBy === 'score'} label="종합순" onClick={() => setSortBy('score')} />
            <SortChip active={sortBy === '1'} label="거래대금" onClick={() => setSortBy('1')} />
            <SortChip active={sortBy === '2'} label="상승률" onClick={() => setSortBy('2')} />
            <SortChip active={sortBy === '3'} label="신고가시점" onClick={() => setSortBy('3')} />
            <SortChip active={sortBy === '4'} label="거래량" onClick={() => setSortBy('4')} />
            <SortChip active={sortBy === '5'} label="단주거래" onClick={() => setSortBy('5')} />
          </div>
        </div>

        {/* Content Section */}
        {loading ? (
          <div className="h-64 flex flex-col items-center justify-center gap-4 text-slate-400">
            <RefreshCw className="animate-spin w-8 h-8" />
            <p className="font-bold text-sm">최신 신호를 분석 중입니다...</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-12">
            {Object.values(groupedData).map((theme) => (
              <section key={theme.name} className="space-y-6">
                <div className="flex items-center justify-between group">
                  <div className="flex items-center gap-3">
                    <div className="w-1.5 h-6 bg-blue-600 rounded-full" />
                    <h2 className="text-xl font-black text-slate-800 tracking-tight">{theme.name}</h2>
                    <span className="text-[10px] font-bold bg-blue-50 text-blue-600 px-2 py-0.5 rounded-lg border border-blue-100">
                      {theme.items.length} signals
                    </span>
                  </div>
                  <button className="text-xs font-bold text-slate-400 hover:text-blue-600 transition-colors flex items-center gap-1">
                    더보기 <ChevronRight size={14} />
                  </button>
                </div>
                
                <div className="grid gap-5">
                  {theme.items.map((stock) => (
                    <StockCard key={stock.symbol} stock={stock} />
                  ))}
                </div>
              </section>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

/**
 * [Sub Components]
 */

function SummaryCard({ icon, label, value, isTime = false }: { icon: React.ReactNode, label: string, value: string | number, isTime?: boolean }) {
  return (
    <div className="bg-white p-5 rounded-[1.5rem] border border-slate-100 shadow-sm flex items-center gap-4 hover:shadow-md transition-shadow">
      <div className="bg-slate-50 p-3 rounded-2xl">{icon}</div>
      <div>
        <p className="text-[11px] font-bold text-slate-400 mb-0.5 uppercase tracking-wide">{label}</p>
        <p className={`${isTime ? 'text-lg' : 'text-2xl'} font-black text-slate-800 tracking-tighter`}>
          {isTime && <span className="text-xs mr-1">오후</span>}
          {value}
        </p>
      </div>
    </div>
  );
}

function StockCard({ stock }: { stock: Stock }) {
  const isPositive = stock.profit_rate >= 0;
  const isHolding = stock.is_holding;
  const scorePercent = Math.min((stock.scores || 0) * 20, 100);

  return (
    <div className={`group relative bg-white rounded-[2.5rem] border-2 transition-all duration-300 p-6 overflow-hidden ${
      isHolding 
      ? 'border-amber-200 shadow-xl shadow-amber-100/30' 
      : 'border-slate-50 shadow-sm hover:shadow-xl hover:shadow-blue-900/5'
    }`}>
      {/* 상단 스코어 프로그레스 바 */}
      <div className="absolute top-0 left-0 h-1.5 bg-slate-50 w-full">
        <div 
          className={`h-full transition-all duration-1000 ease-out ${isHolding ? 'bg-amber-400' : 'bg-blue-600'}`}
          style={{ width: `${scorePercent}%` }}
        />
      </div>

      <div className="flex justify-between items-start mb-4">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <h3 className="text-xl font-black text-slate-900 tracking-tighter">{stock.stock_name}</h3>
            {isHolding && (
              <span className="bg-amber-500 text-white text-[9px] font-black px-2 py-0.5 rounded-full flex items-center gap-1">
                <Star size={10} fill="currentColor" /> MY: 100주
              </span>
            )}
          </div>
          <p className="text-[10px] font-mono font-bold text-slate-400 tracking-widest">{stock.symbol}</p>
        </div>
        <div className={`px-4 py-1.5 rounded-2xl border flex items-center gap-2 ${isHolding ? 'bg-amber-50 border-amber-100' : 'bg-slate-50 border-slate-100'}`}>
          <span className="text-[10px] font-black text-slate-400 uppercase">종합</span>
          <span className={`text-base font-black ${isHolding ? 'text-amber-600' : 'text-blue-600'}`}>{stock.scores}점</span>
        </div>
      </div>

      {/* 가격 및 등락 */}
      <div className="flex items-end justify-between mb-6">
        <div>
          <div className="text-3xl font-black text-slate-900 tracking-tighter flex items-baseline gap-1">
            {stock.current_price?.toLocaleString()}
            <span className="text-sm font-bold text-slate-400">원</span>
          </div>
          <div className={`text-sm font-black flex items-center gap-1 mt-1 ${isPositive ? 'text-rose-500' : 'text-blue-500'}`}>
            {isPositive ? <ArrowUpRight size={16} /> : <ArrowDownRight size={16} />}
            {Math.abs(stock.profit_rate || 0).toFixed(2)}%
            <span className="ml-2 text-[10px] text-slate-400 font-bold">신고가: {Math.round(stock.current_price * 1.1).toLocaleString()} <Flame size={12} className="inline text-orange-400" /></span>
          </div>
        </div>
        <div className="text-right">
          <div className="text-[10px] font-bold text-slate-400 mb-1 uppercase tracking-wider">Transaction</div>
          <div className="text-sm font-black text-slate-700">{stock.trade_value?.toLocaleString()}억</div>
        </div>
      </div>

      {/* 상태 태그 (이미지 디자인 반영) */}
      <div className="flex flex-wrap gap-1.5 mb-6">
        {stock.cap_time_1 && <Badge label="대금상위" type="rose" />}
        {stock.cap_time_2 && <Badge label="상승상위" type="orange" />}
        {stock.cap_time_3 && <Badge label="신고가" type="rose" />}
        {stock.cap_time_5 && <Badge label="단주포착" type="purple" />}
        {!stock.cap_time_1 && <Badge label="수급모멘텀" type="blue" />}
      </div>

      {/* 타임라인 로그 */}
      <div className={`pt-4 border-t flex items-center gap-3 ${isHolding ? 'border-amber-100' : 'border-slate-100'}`}>
        <Clock size={14} className="text-slate-300" />
        <div className="flex gap-4 text-[10px] font-bold text-slate-400 overflow-x-auto no-scrollbar italic">
          {stock.cap_time_1 && <span>대금({stock.cap_time_1.slice(11,16)})</span>}
          {stock.cap_time_4 && <span>량({stock.cap_time_4.slice(11,16)})</span>}
          {stock.cap_time_5 && <span>단주({stock.cap_time_5.slice(11,16)})</span>}
          {!stock.cap_time_1 && <span>데이터 집계 중...</span>}
        </div>
      </div>

      {/* 배경 장식 (이미지 스타일) */}
      <div className="absolute -bottom-6 -right-6 text-slate-50 opacity-10 pointer-events-none group-hover:scale-110 transition-transform">
        <Activity size={120} strokeWidth={1} />
      </div>
    </div>
  );
}

function Badge({ label, type }: { label: string, type: 'blue' | 'rose' | 'purple' | 'orange' | 'slate' }) {
  const styles = {
    blue: 'bg-blue-50 text-blue-600 border-blue-100',
    rose: 'bg-rose-50 text-rose-500 border-rose-100',
    purple: 'bg-purple-50 text-purple-600 border-purple-100',
    orange: 'bg-orange-50 text-orange-600 border-orange-100',
    slate: 'bg-slate-50 text-slate-500 border-slate-200',
  };
  return <span className={`px-2.5 py-1 rounded-xl text-[10px] font-black border uppercase tracking-tighter ${styles[type]}`}>{label}</span>;
}

function TabButton({ active, onClick, icon, label }: any) {
  return (
    <button 
      onClick={onClick}
      className={`flex items-center gap-2 px-6 py-3 rounded-2xl text-sm font-black transition-all whitespace-nowrap ${
        active 
        ? 'bg-blue-600 text-white shadow-xl shadow-blue-200 -translate-y-0.5' 
        : 'bg-white text-slate-400 border border-slate-100 hover:bg-slate-50'
      }`}
    >
      {icon} {label}
    </button>
  );
}

function SortChip({ active, label, onClick }: any) {
  return (
    <button 
      onClick={onClick}
      className={`px-5 py-2.5 rounded-2xl text-[11px] font-black border transition-all whitespace-nowrap ${
        active 
        ? 'bg-[#1E293B] border-[#1E293B] text-white shadow-lg' 
        : 'bg-white border-slate-200 text-slate-500 hover:border-blue-400'
      }`}
    >
      {label}
    </button>
  );
}