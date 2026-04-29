'use client';

import { useState, useEffect, useCallback } from 'react';
import { sanitizeUserText } from '../lib/sanitize';

interface Comment {
  id: number;
  article_id: string;
  parent_id: number | null;
  author_username: string;
  author_role: string;
  content: string;
  created_at: string;
}

interface Props {
  articleId: string | number;
  currentUser: { username: string; role: string } | null;
  collapsed?: boolean;
  onToggleCollapse?: () => void;
}

function timeAgo(dateStr: string): string {
  const diff = Math.floor((Date.now() - new Date(dateStr).getTime()) / 1000);
  if (diff < 60) return 'hace un momento';
  if (diff < 3600) return `hace ${Math.floor(diff / 60)} min`;
  if (diff < 86400) return `hace ${Math.floor(diff / 3600)} h`;
  return `hace ${Math.floor(diff / 86400)} días`;
}

function Avatar({ name, role }: { name: string; role: string }) {
  const isAdmin = role === 'admin';
  return (
    <div
      className={`w-8 h-8 rounded-full flex items-center justify-center text-white text-xs font-bold uppercase flex-shrink-0 ${
        isAdmin ? 'bg-gradient-to-br from-[#2363eb] to-[#1b4fcb]' : 'bg-gradient-to-br from-[#5f84ba] to-[#46618f]'
      }`}
    >
      {name.charAt(0)}
    </div>
  );
}

function CommentItem({
  comment,
  replies,
  allComments,
  currentUser,
  onReply,
  onDelete,
}: {
  comment: Comment;
  replies: Comment[];
  allComments: Comment[];
  currentUser: Props['currentUser'];
  onReply: (parentId: number, content: string) => Promise<void>;
  onDelete: (id: number) => Promise<void>;
}) {
  const [showReplyBox, setShowReplyBox] = useState(false);
  const [replyText, setReplyText] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const handleReply = async () => {
    if (!replyText.trim()) return;
    setSubmitting(true);
    await onReply(comment.id, replyText.trim());
    setReplyText('');
    setShowReplyBox(false);
    setSubmitting(false);
  };

  const canDelete =
    currentUser &&
    (currentUser.username === comment.author_username || currentUser.role === 'admin');

  return (
    <div className="flex gap-3">
      <Avatar name={comment.author_username} role={comment.author_role} />
      <div className="flex-1 min-w-0">
        <div className="glass-panel rounded-xl px-4 py-3">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-sm font-semibold text-[color:var(--ink-900)]">{comment.author_username}</span>
            {comment.author_role === 'admin' && (
              <span className="text-xs bg-[#2b60df] text-white px-1.5 py-0.5 rounded-full">Admin</span>
            )}
            <span className="text-xs text-[color:var(--ink-600)] ml-auto">{timeAgo(comment.created_at)}</span>
          </div>
          <p className="text-sm text-[color:var(--ink-800)] whitespace-pre-wrap">{comment.content}</p>
        </div>

        <div className="flex items-center gap-3 mt-1 ml-1">
          {currentUser && (
            <button
              onClick={() => setShowReplyBox((v) => !v)}
              className="text-xs text-[color:var(--accent-strong)] hover:underline font-medium"
            >
              Responder
            </button>
          )}
          {canDelete && (
            <button
              onClick={() => onDelete(comment.id)}
              className="text-xs text-red-500 hover:underline"
            >
              Eliminar
            </button>
          )}
        </div>

        {showReplyBox && (
          <div className="mt-2 flex gap-2">
            <textarea
              value={replyText}
              onChange={(e) => setReplyText(e.target.value)}
              placeholder="Escribe tu respuesta..."
              rows={2}
              className="input-glass flex-1 text-sm px-3 py-2 rounded-lg resize-none"
            />
            <div className="flex flex-col gap-1">
              <button
                onClick={handleReply}
                disabled={submitting || !replyText.trim()}
                className="btn-accent px-3 py-1.5 text-xs rounded-lg disabled:opacity-50"
              >
                {submitting ? '...' : 'Enviar'}
              </button>
              <button
                onClick={() => { setShowReplyBox(false); setReplyText(''); }}
                className="px-3 py-1.5 text-xs glass-pill text-[color:var(--accent-strong)] rounded-lg"
              >
                Cancelar
              </button>
            </div>
          </div>
        )}

        {/* Nested replies */}
        {replies.length > 0 && (
          <div className="mt-3 pl-4 border-l-2 border-[color:var(--line)] space-y-3">
            {replies.map((reply) => (
              <CommentItem
                key={reply.id}
                comment={reply}
                replies={allComments.filter((c) => c.parent_id === reply.id)}
                allComments={allComments}
                currentUser={currentUser}
                onReply={onReply}
                onDelete={onDelete}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default function CommentsSection({ articleId, currentUser, collapsed, onToggleCollapse }: Props) {
  const [comments, setComments] = useState<Comment[]>([]);
  const [loading, setLoading] = useState(true);
  const [newComment, setNewComment] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [resolvedUser, setResolvedUser] = useState<Props['currentUser']>(currentUser);
  const [internalCollapsed, setInternalCollapsed] = useState(false);
  const isCollapsed = collapsed ?? internalCollapsed;

  const handleToggleCollapse = () => {
    if (onToggleCollapse) {
      onToggleCollapse();
      return;
    }
    setInternalCollapsed((prev) => !prev);
  };

  useEffect(() => {
    if (currentUser?.username && currentUser?.role) {
      setResolvedUser(currentUser);
      return;
    }

    const userJson = localStorage.getItem('user');
    if (!userJson) {
      setResolvedUser(null);
      return;
    }

    try {
      const parsed = JSON.parse(userJson);
      if (parsed?.username && parsed?.role) {
        setResolvedUser({ username: parsed.username, role: parsed.role });
      } else {
        setResolvedUser(null);
      }
    } catch (error) {
      console.error('Error parseando usuario en CommentsSection:', error);
      setResolvedUser(null);
    }
  }, [currentUser]);

  const fetchComments = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/comments?article_id=${articleId}`);
      const json = await res.json();
      setComments(json.data || []);
    } catch {
      setComments([]);
    } finally {
      setLoading(false);
    }
  }, [articleId]);

  useEffect(() => {
    fetchComments();
  }, [fetchComments]);

  const handlePost = async () => {
    const safeComment = sanitizeUserText(newComment, 1000);
    if (!safeComment || !resolvedUser) return;
    setSubmitting(true);
    try {
      await fetch('/api/comments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          article_id: String(articleId),
          parent_id: null,
          author_username: resolvedUser.username,
          author_role: resolvedUser.role,
          content: safeComment,
        }),
      });
      setNewComment('');
      await fetchComments();
    } finally {
      setSubmitting(false);
    }
  };

  const handleReply = async (parentId: number, content: string) => {
    if (!resolvedUser) return;
    const safeReply = sanitizeUserText(content, 1000);
    if (!safeReply) return;
    await fetch('/api/comments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        article_id: String(articleId),
        parent_id: parentId,
        author_username: resolvedUser.username,
        author_role: resolvedUser.role,
        content: safeReply,
      }),
    });
    await fetchComments();
  };

  const handleDelete = async (id: number) => {
    await fetch(`/api/comments/${id}`, { method: 'DELETE' });
    await fetchComments();
  };

  const rootComments = comments.filter((c) => c.parent_id === null);

  return (
    <div className="flex flex-col h-full">
      <div className={`mb-4 pb-3 border-b border-[color:var(--line)] flex items-center ${isCollapsed ? 'justify-center' : 'justify-between'} gap-2`}>
        {!isCollapsed && (
          <h3 className="text-sm font-bold text-[color:var(--ink-900)] uppercase tracking-wide">
            Comentarios ({comments.length})
          </h3>
        )}
        <button
          type="button"
          onClick={handleToggleCollapse}
          className="inline-flex items-center justify-center text-[color:var(--ink-700)] hover:text-[color:var(--ink-900)] border border-[color:var(--line)] rounded-md w-8 h-8 hover:bg-white/70 transition-colors dark:hover:bg-slate-800/70"
          title={isCollapsed ? 'Mostrar comentarios' : 'Ocultar comentarios'}
          aria-label={isCollapsed ? 'Mostrar comentarios' : 'Ocultar comentarios'}
        >
          <svg
            className="w-4 h-4"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            {isCollapsed ? (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
            ) : (
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            )}
          </svg>
        </button>
      </div>

      {isCollapsed ? (
        <div className="hidden" />
      ) : (
        <>

          {/* New comment box */}
          {resolvedUser ? (
            <div className="flex gap-3 mb-6">
              <Avatar name={resolvedUser.username} role={resolvedUser.role} />
              <div className="flex-1">
                <textarea
                  value={newComment}
                  onChange={(e) => setNewComment(e.target.value)}
                  placeholder="¿Cómo te funcionó esta solución? Deja tu comentario..."
                  rows={3}
                  className="input-glass w-full text-sm px-3 py-2 rounded-xl resize-none"
                />
                <div className="flex justify-end mt-2">
                  <button
                    onClick={handlePost}
                    disabled={submitting || !newComment.trim()}
                    className="btn-accent px-4 py-2 text-sm rounded-lg disabled:opacity-50 font-medium"
                  >
                    {submitting ? 'Publicando...' : 'Publicar comentario'}
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <p className="text-sm text-[color:var(--ink-700)] mb-4">Inicia sesión para dejar un comentario.</p>
          )}

          {/* Comments list */}
          {loading ? (
            <p className="text-sm text-[color:var(--ink-600)]">Cargando comentarios...</p>
          ) : rootComments.length === 0 ? (
            <p className="text-sm text-[color:var(--ink-600)]">Sé el primero en comentar esta solución.</p>
          ) : (
            <div className="flex-1 min-h-0 overflow-y-auto space-y-5 pr-1">
              {rootComments.map((comment) => (
                <CommentItem
                  key={comment.id}
                  comment={comment}
                  replies={comments.filter((c) => c.parent_id === comment.id)}
                  allComments={comments}
                  currentUser={resolvedUser}
                  onReply={handleReply}
                  onDelete={handleDelete}
                />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

