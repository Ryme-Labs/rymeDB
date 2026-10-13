import type { Metadata } from "next";
import type { ReactNode } from "react";
import "../styles/globals.css";
import { Providers } from "./providers";
import { AppShell } from "@/components/studio/shell";

export const metadata: Metadata = {
  title: "rymeDB Admin",
  description: "rymeDB Studio — operator console for the rymeDB database",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body>
        <Providers>
          <AppShell>{children}</AppShell>
        </Providers>
      </body>
    </html>
  );
}
