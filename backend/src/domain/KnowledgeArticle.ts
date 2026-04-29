// src/domain/KnowledgeArticle.ts

export interface IKnowledgeArticle {
  id: string;
  titulo: string;
  tags: string[];
  contenido: string;
  descripcion?: string;
  categoria?: string;
  subcategoria?: string;
  pasos?: any[];
  camposFormulario?: Array<{ name: string; label: string; type: string; required?: boolean }>;
  script?: string;
  creadoPor: string;
  fecha: Date;
  actualizado?: Date;
}

export class KnowledgeArticle implements IKnowledgeArticle {
  id: string;
  titulo: string;
  tags: string[];
  contenido: string;
  descripcion?: string;
  categoria?: string;
  subcategoria?: string;
  pasos?: any[];
  camposFormulario?: Array<{ name: string; label: string; type: string; required?: boolean }>;
  script?: string;
  creadoPor: string;
  fecha: Date;
  actualizado?: Date;

  constructor(
    id: string,
    titulo: string,
    tags: string[],
    contenido: string,
    creadoPor: string,
    fecha: Date = new Date(),
    actualizado?: Date,
    descripcion?: string,
    categoria?: string,
    subcategoria?: string,
    pasos?: any[],
    camposFormulario?: Array<{ name: string; label: string; type: string; required?: boolean }>,
    script?: string
  ) {
    this.id = id;
    this.titulo = titulo;
    this.tags = tags;
    this.contenido = contenido;
    this.descripcion = descripcion;
    this.creadoPor = creadoPor;
    this.fecha = fecha;
    this.actualizado = actualizado;
    this.categoria = categoria;
    this.subcategoria = subcategoria;
    this.pasos = pasos;
    this.camposFormulario = camposFormulario;
    this.script = script;
  }

  hasTag(tag: string): boolean {
    return this.tags.includes(tag.toLowerCase());
  }
}
