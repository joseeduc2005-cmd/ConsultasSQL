'use client';

import ReactMarkdown from 'react-markdown';

/**
 * Contenido de un artículo, que viene en Markdown.
 *
 * Los estilos van aquí y no en una hoja global porque esta vista usa una escala
 * tipográfica más chica que la del panel de administración.
 *
 * El detalle que más cuesta: `code` se aplica tanto al código en medio de una frase
 * como al de dentro de un bloque. Dándole fondo claro a los dos, el bloque oscuro
 * salía con barras blancas encima y no se leía nada. Aquí `code` se pinta claro solo
 * cuando NO viene dentro de un `pre`.
 */
export function Markdown({ contenido }: { contenido: string }) {
  return (
    <div className="text-[14px] leading-relaxed text-(--ink-800)">
      <ReactMarkdown
        components={{
          h1: ({ children }) => (
            <h3 className="mt-5 mb-2 text-[16px] font-semibold text-(--ink-900) first:mt-0">{children}</h3>
          ),
          h2: ({ children }) => (
            <h4 className="mt-5 mb-1.5 text-[14.5px] font-semibold text-(--ink-900) first:mt-0">{children}</h4>
          ),
          h3: ({ children }) => (
            <h5 className="mt-4 mb-1 text-[14px] font-semibold text-(--ink-900) first:mt-0">{children}</h5>
          ),
          p: ({ children }) => <p className="mb-2.5 last:mb-0">{children}</p>,
          ul: ({ children }) => <ul className="mb-2.5 ml-5 list-disc space-y-1">{children}</ul>,
          ol: ({ children }) => <ol className="mb-2.5 ml-5 list-decimal space-y-1">{children}</ol>,
          strong: ({ children }) => <strong className="font-semibold text-(--ink-900)">{children}</strong>,
          a: ({ children, href }) => (
            <a href={href} className="text-(--accent) underline">
              {children}
            </a>
          ),
          // Dentro de un bloque no lleva fondo propio: lo pone el `pre`.
          code: ({ className, children }) =>
            className?.startsWith('language-') ? (
              <code className="font-mono text-[13px] whitespace-pre">{children}</code>
            ) : (
              <code className="rounded bg-(--surface-2) px-1.5 py-0.5 font-mono text-[13px] text-(--ink-900)">
                {children}
              </code>
            ),
          pre: ({ children }) => (
            <pre className="mb-3 overflow-x-auto rounded-xl bg-slate-900 px-4 py-3 text-slate-100">{children}</pre>
          ),
          blockquote: ({ children }) => (
            <blockquote className="mb-2.5 border-l-2 border-(--accent)/40 bg-(--accent)/5 py-1.5 pl-3 text-(--ink-700)">
              {children}
            </blockquote>
          ),
          hr: () => <hr className="my-4 border-(--line)" />,
          table: ({ children }) => (
            <div className="mb-3 overflow-x-auto rounded-lg border border-(--line)">
              <table className="w-full text-left text-[14px]">{children}</table>
            </div>
          ),
          th: ({ children }) => (
            <th className="border-b border-(--line) bg-(--surface-2) px-3 py-1.5 font-medium">{children}</th>
          ),
          td: ({ children }) => <td className="border-b border-(--line) px-3 py-1.5 last:border-b-0">{children}</td>,
        }}
      >
        {contenido}
      </ReactMarkdown>
    </div>
  );
}

/**
 * Resumen de una línea, sin marcas de Markdown.
 *
 * Varios artículos traen la descripción en Markdown —hay uno cuyo resumen empieza con
 * «## Ranking salarial»— y en una vista previa esas marcas solo estorban.
 * Renderizarlas tampoco sirve: un título dentro de una línea se ve peor.
 */
export function resumenPlano(texto: string): string {
  return texto
    .replace(/^#{1,6}\s*/gm, '')
    .replace(/\*\*|__|\*|_/g, '')
    .replace(/`/g, '')
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/^>\s*/gm, '')
    .replace(/\s+/g, ' ')
    .trim();
}
