import type { Metadata } from "next";
import "./globals.css"; // 생성한 globals.css를 여기서 임포트합니다.

export const metadata: Metadata = {
  title: "Trading Master Dashboard",
  description: "Real-time stock monitoring system",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body>
        {children}
      </body>
    </html>
  );
}