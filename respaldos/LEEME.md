# Respaldos de la base de conocimiento

Aquí deja sus copias el servicio `db-backup` del `docker-compose.yml`: una al
arrancar y luego cada `RESPALDO_CADA_HORAS` (24 por defecto), conservando las
últimas `RESPALDO_CONSERVAR` (14 por defecto).

Lo que la gente escribe en el sistema no se puede regenerar: son los casos que
resolvieron, con sus scripts. Por eso las copias van a esta carpeta del host y no
a un volumen de Docker — para que se puedan llevar a otra máquina sin Docker de
por medio.

## Restaurar una copia

```bash
docker compose exec -T postgres pg_restore -U postgres -d knowledge_base \
  --clean --if-exists < respaldos/knowledge_base-AAAAMMDD-HHMMSS.dump
```

## Lo que hay que comprobar antes de abrir el sistema a la gente

Una copia que nadie ha restaurado nunca no es una copia de seguridad, es un
fichero. Restaure una contra una base de prueba y compruebe que los artículos
están. Hasta que eso se haya hecho una vez, el respaldo no cuenta.
