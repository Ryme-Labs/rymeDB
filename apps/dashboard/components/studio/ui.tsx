"use client";

import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import { Check, Copy } from "lucide-react";
import { useState, type ReactNode } from "react";
import { pretty } from "@/lib/api";
import { cn } from "@/lib/cn";

/* ---------- Button (Studio-like, red primary) ---------- */

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-1.5 rounded-md text-[13px] font-medium transition-colors disabled:pointer-events-none disabled:opacity-50 [&_svg]:size-3.5 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        primary:
          "bg-brand-600 text-white shadow-sm hover:bg-brand-500 active:bg-brand-700",
        secondary:
          "border border-border-default bg-surface-300 text-foreground-default hover:border-border-muted hover:bg-surface-400",
        ghost: "text-foreground-light hover:bg-surface-300 hover:text-foreground-default",
        danger:
          "border border-danger/40 bg-danger/10 text-danger hover:bg-danger/20",
      },
      size: {
        sm: "h-7 px-2.5",
        md: "h-8 px-3.5",
        icon: "h-7 w-7",
      },
    },
    defaultVariants: { variant: "secondary", size: "sm" },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

export function Button({ className, variant, size, asChild, ...props }: ButtonProps) {
  const Comp = asChild ? Slot : "button";
  return <Comp className={cn(buttonVariants({ variant, size }), className)} {...props} />;
}

/* ---------- Card ---------- */

export function Card({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <div
      className={cn(
        "rounded-lg border border-border-default bg-surface-200 shadow-sm",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function CardHeader({
  title,
  description,
  actions,
}: {
  title: string;
  description?: string;
  actions?: ReactNode;
}) {
  return (
    <div className="flex items-start justify-between gap-3 border-b border-border-muted px-4 py-3">
      <div>
        <h3 className="text-[13px] font-semibold text-foreground-default">{title}</h3>
        {description ? (
          <p className="mt-0.5 text-xs text-foreground-muted">{description}</p>
        ) : null}
      </div>
      {actions ? <div className="flex shrink-0 items-center gap-2">{actions}</div> : null}
    </div>
  );
}

export function CardContent({ className, children }: { className?: string; children: ReactNode }) {
  return <div className={cn("px-4 py-3", className)}>{children}</div>;
}

/* ---------- Forms ---------- */

const fieldCls =
  "h-8 rounded-md border border-border-default bg-surface-100 px-2.5 text-[13px] text-foreground-default placeholder:text-foreground-muted hover:border-border-muted focus:border-brand-500";

export function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={cn(fieldCls, props.className)} />;
}

export function Textarea(props: React.TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      {...props}
      className={cn(fieldCls, "mono h-auto py-2 text-xs leading-relaxed", props.className)}
    />
  );
}

export function Field({
  label,
  children,
  className,
}: {
  label: string;
  children: ReactNode;
  className?: string;
}) {
  return (
    <label className={cn("grid gap-1 text-xs text-foreground-lighter", className)}>
      <span>{label}</span>
      {children}
    </label>
  );
}

export function Row({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("flex flex-wrap items-end gap-2", className)}>{children}</div>;
}

/* ---------- Status pill ---------- */

export function Pill({
  tone,
  children,
}: {
  tone: "ok" | "bad" | "warn" | "info" | "muted";
  children: ReactNode;
}) {
  const tones: Record<string, string> = {
    ok: "bg-success/15 text-[#7ee787] border-success/30",
    bad: "bg-danger/15 text-danger border-danger/30",
    warn: "bg-warning/15 text-warning border-warning/30",
    info: "bg-brand-600/15 text-brand-300 border-brand-500/30",
    muted: "bg-surface-400 text-foreground-muted border-border-default",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium",
        tones[tone],
      )}
    >
      {children}
    </span>
  );
}

/* ---------- Section header (Studio breadcrumb style) ---------- */

export function SectionHeader({
  title,
  description,
  actions,
}: {
  title: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
      <div>
        <p className="text-xs text-foreground-muted">rymeDB Admin</p>
        <h1 className="mt-0.5 text-xl font-semibold tracking-tight">{title}</h1>
        <p className="mt-1 max-w-2xl text-[13px] text-foreground-lighter">{description}</p>
      </div>
      {actions ? <div className="flex items-center gap-2">{actions}</div> : null}
    </div>
  );
}

/* ---------- Code / JSON output ---------- */

export function CodeBlock({ data, empty }: { data: unknown; empty?: string }) {
  const [copied, setCopied] = useState(false);
  const text = data === undefined || data === null ? (empty ?? "—") : pretty(data);
  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {
      /* clipboard unavailable */
    }
  }
  return (
    <div className="group relative">
      <pre className="mono max-h-[420px] overflow-auto whitespace-pre-wrap rounded-md border border-border-muted bg-surface-100 p-3 text-xs leading-relaxed text-foreground-light">
        {text}
      </pre>
      <button
        type="button"
        onClick={copy}
        className="absolute right-2 top-2 rounded border border-border-default bg-surface-300 p-1.5 text-foreground-muted opacity-0 transition-opacity hover:text-foreground-default group-hover:opacity-100"
        aria-label="Copy output"
      >
        {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
      </button>
    </div>
  );
}

/* ---------- Stat ---------- */

export function Stat({
  label,
  value,
  sub,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
}) {
  return (
    <Card>
      <CardContent>
        <p className="text-xs text-foreground-muted">{label}</p>
        <p className="mt-1 text-lg font-semibold tracking-tight">{value}</p>
        {sub ? <p className="mt-0.5 text-xs text-foreground-lighter">{sub}</p> : null}
      </CardContent>
    </Card>
  );
}

/* ---------- Error ---------- */

export function QueryError({ error }: { error: unknown }) {
  if (!error) return null;
  return (
    <div className="rounded-md border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger">
      {error instanceof Error ? error.message : String(error)}
    </div>
  );
}
