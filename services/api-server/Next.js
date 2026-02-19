// 테마별 고정 색상 생성 함수
const getThemeColor = (themeName) => {
  let hash = 0;
  for (let i = 0; i < themeName.length; i++) {
    hash = themeName.charCodeAt(i) + ((hash << 5) - hash);
  }
  return `hsl(${hash % 360}, 70%, 80%)`; // 일관된 파스텔톤 유지
};