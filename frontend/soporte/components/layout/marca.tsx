/** Isotipo del sistema: líneas de un ticket con un punto de estado. */
export function Marca({ className = 'size-8' }: { className?: string }) {
  return (
    <svg viewBox="0 0 32 32" className={className} aria-hidden="true">
      <rect width="32" height="32" rx="8" className="fill-primary" />
      <path d="M9 11.5h14M9 16h9M9 20.5h11" stroke="white" strokeWidth="2.4" strokeLinecap="round" />
      <circle cx="23" cy="20.5" r="2.2" fill="#99f6e4" />
    </svg>
  );
}
