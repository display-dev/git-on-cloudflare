import type { ComponentProps, ReactNode } from "react";

type ButtonVariant = "primary" | "secondary" | "danger" | "ghost";
type ButtonSize = "sm" | "md";

type ButtonBaseProps = {
  variant?: ButtonVariant;
  size?: ButtonSize;
  children: ReactNode;
  className?: string;
};

type ButtonAsButton = ButtonBaseProps &
  Omit<ComponentProps<"button">, keyof ButtonBaseProps> & {
    href?: never;
  };

type ButtonAsAnchor = ButtonBaseProps &
  Omit<ComponentProps<"a">, keyof ButtonBaseProps> & {
    href: string;
  };

export type ButtonProps = ButtonAsButton | ButtonAsAnchor;

const shared =
  "inline-flex items-center justify-center gap-2 rounded-xl font-medium no-underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent-500/50 focus-visible:ring-offset-2 focus-visible:ring-offset-canvas disabled:opacity-50 disabled:pointer-events-none";

// Anchor-backed buttons still match the global link rules in app.css, so each
// variant declares text colors for hover states instead of relying on cascade.
// Hover color swaps are intentionally instant; animating them can create muddy
// intermediate colors across browsers. Only the active press scale transitions.
const variants: Record<ButtonVariant, string> = {
  primary:
    "bg-accent-600 text-white hover:bg-accent-500 hover:text-white active:scale-[0.98] transition-transform",
  secondary:
    "border border-zinc-300 bg-zinc-100 text-zinc-700 hover:bg-zinc-200 hover:text-zinc-900 dark:border-zinc-700/60 dark:bg-zinc-800/60 dark:text-zinc-300 dark:hover:bg-zinc-700/60 dark:hover:text-zinc-100 active:scale-[0.98] transition-transform",
  danger:
    "border border-red-500/20 bg-red-500/10 text-red-700 hover:bg-red-500/20 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 active:scale-[0.98] transition-transform",
  ghost:
    "text-zinc-600 hover:bg-zinc-100 hover:text-zinc-900 dark:text-zinc-400 dark:hover:bg-zinc-800/70 dark:hover:text-zinc-100 active:scale-[0.97] transition-transform",
};

const sizes: Record<ButtonSize, string> = {
  sm: "px-3 py-1.5 text-sm",
  md: "px-4 py-2.5 text-sm",
};

export function buttonClasses(variant: ButtonVariant = "primary", size: ButtonSize = "md"): string {
  return `${shared} ${variants[variant]} ${sizes[size]}`;
}

export function Button({
  variant = "primary",
  size = "md",
  className = "",
  children,
  ...rest
}: ButtonProps) {
  const classes = `${shared} ${variants[variant]} ${sizes[size]} ${className}`.trim();

  if ("href" in rest && rest.href != null) {
    const { href, ...anchorRest } = rest as ButtonAsAnchor;
    return (
      <a href={href} className={classes} {...anchorRest}>
        {children}
      </a>
    );
  }

  return (
    <button className={classes} {...(rest as Omit<ButtonAsButton, keyof ButtonBaseProps>)}>
      {children}
    </button>
  );
}
