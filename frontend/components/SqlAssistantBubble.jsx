'use client';

import React, { useState } from 'react';
import SqlAssistantPanel from './SqlAssistantPanel';

export default function SqlAssistantBubble({ onQuerySelected = (_query) => {} }) {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <>
      <button
        onClick={() => setIsOpen(!isOpen)}
        title="Panel SQL Automático"
        className="fixed bottom-8 right-8 z-40 flex h-14 w-14 items-center justify-center rounded-full border border-white/20 text-white transition-all duration-300"
        style={{
          background: isOpen
            ? 'linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)'
            : 'linear-gradient(135deg, #0284c7 0%, #3b82f6 50%, #7c3aed 100%)',
          boxShadow: isOpen
            ? '0 0 0 4px rgba(56,189,248,0.22), 0 8px 32px rgba(59,130,246,0.5), 0 0 40px rgba(56,189,248,0.2)'
            : '0 0 0 2px rgba(56,189,248,0.12), 0 4px 20px rgba(59,130,246,0.35)',
          transform: isOpen ? 'scale(1.06) rotate(90deg)' : 'scale(1)',
        }}
        onMouseEnter={(e) => {
          if (!isOpen) e.currentTarget.style.boxShadow = '0 0 0 3px rgba(56,189,248,0.22), 0 6px 28px rgba(59,130,246,0.5), 0 0 50px rgba(56,189,248,0.18)';
        }}
        onMouseLeave={(e) => {
          if (!isOpen) e.currentTarget.style.boxShadow = '0 0 0 2px rgba(56,189,248,0.12), 0 4px 20px rgba(59,130,246,0.35)';
        }}
      >
        <span className="text-xl transition-transform duration-300">{isOpen ? '✕' : '⚡'}</span>
      </button>

      <SqlAssistantPanel
        isOpen={isOpen}
        onClose={() => setIsOpen(false)}
        onQuerySelected={onQuerySelected}
      />
    </>
  );
}
