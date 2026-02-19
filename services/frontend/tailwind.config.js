/** @type {import('tailwindcss').Config} */
module.exports = {
  // Next.js App Router 구조에서 Tailwind 클래스가 사용된 모든 파일을 명확히 스캔하도록 경로 보강
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/**/*.{js,ts,jsx,tsx,mdx}",
    // 혹시 모를 루트 레벨의 페이지 파일 대응
    "./page.tsx",
  ],
  theme: {
    extend: {
      // 기획안의 브랜드 컬러(Primary Blue, Accent Green 등)를 Tailwind 변수로 등록
      colors: {
        background: "var(--background)",
        foreground: "var(--foreground)",
        brand: {
          blue: "#005088",
          green: "#11caa0",
          cream: "#f3f0df",
        }
      },
      // 카드 UI 등을 위한 부드러운 그림자 설정 추가
      boxShadow: {
        'soft': '0 4px 20px -2px rgba(0, 0, 0, 0.05)',
      }
    },
  },
  plugins: [],
};