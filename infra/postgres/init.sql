-- ============================================================
-- Esquema do banco (arquitetura medalhão + estrela):
--   bronze = Kafka (cru)
--   silver = eventos limpos
--   gold   = esquema estrela agregado para os dashboards
--
-- Autor: Beatriz
-- Criado em: 23/06/2026
-- Atualizado em: 08/07/2026
-- ============================================================

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ===================== SILVER =====================
-- um registro por evento (apenas posts)
CREATE TABLE IF NOT EXISTS silver.eventos (
    event_id       BIGINT GENERATED ALWAYS AS IDENTITY,
    event_ts       TIMESTAMPTZ NOT NULL,
    ingest_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    tipo_evento    TEXT NOT NULL,          -- post
    autor_did      TEXT NOT NULL,          -- autor do post
    idioma         TEXT,                   -- ex: pt, en
    tipo_conteudo  TEXT,                   -- texto/imagem/link/citacao/video
    texto          TEXT                   
) PARTITION BY RANGE (event_ts);           --partição pela data do evento

CREATE INDEX IF NOT EXISTS idx_silver_eventos_ts ON silver.eventos (event_ts);

-- Partição DEFAULT como rede de segurança.
CREATE TABLE IF NOT EXISTS silver.eventos_default
    PARTITION OF silver.eventos DEFAULT;


-- ===================== GOLD: dimensões =====================
CREATE TABLE IF NOT EXISTS gold.dim_tipo_evento (
    codigo     TEXT PRIMARY KEY,
    descricao  TEXT NOT NULL
);
INSERT INTO gold.dim_tipo_evento (codigo, descricao) VALUES
    ('post',   'Publicação'),
    ('like',   'Curtida'),
    ('repost', 'Republicação'),
    ('follow', 'Novo seguidor'),
    ('na',     'Não aplicável')
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS gold.dim_tipo_conteudo (
    codigo     TEXT PRIMARY KEY,
    descricao  TEXT NOT NULL
);
INSERT INTO gold.dim_tipo_conteudo (codigo, descricao) VALUES
    ('txt',  'Texto'),
    ('img',  'Imagem'),
    ('link', 'Link'),
    ('cit',  'Citação'),
    ('vid',  'Vídeo'),
    ('na',   'Não aplicável')
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS gold.dim_idioma (
    codigo     TEXT PRIMARY KEY,
    descricao  TEXT NOT NULL
);
INSERT INTO gold.dim_idioma (codigo, descricao) VALUES
    ('pt', 'Português'), ('en', 'Inglês'),    ('es', 'Espanhol'),
    ('ja', 'Japonês'),   ('fr', 'Francês'),   ('de', 'Alemão'),
    ('it', 'Italiano'),  ('ru', 'Russo'),     ('ko', 'Coreano'),
    ('zh', 'Chinês'),    ('nl', 'Holandês'),  ('pl', 'Polonês'),
    ('tr', 'Turco'),     ('ar', 'Árabe'),     ('id', 'Indonésio'),
    ('uk', 'Ucraniano'), ('sv', 'Sueco'),     ('fa', 'Persa'),
    ('th', 'Tailandês'), ('vi', 'Vietnamita'),('hi', 'Hindi'),
    ('cs', 'Tcheco'),    ('fi', 'Finlandês'), ('da', 'Dinamarquês'),
    ('no', 'Norueguês'), ('hu', 'Húngaro'),   ('ro', 'Romeno'),
    ('el', 'Grego'),     ('he', 'Hebraico'),  ('ca', 'Catalão'),
    ('ne', 'Nepalês'),
    ('und', 'Indefinido'), ('outro', 'Outro'), ('na', 'Não aplicável')
ON CONFLICT DO NOTHING;


-- ===================== GOLD: lista de bloqueio (+18) =====================
-- Termos/domínios impróprios filtrados na leitura (dashboard). A lista é dado,
-- não código: carregada de um seed externo (seeds/termo_bloqueado.csv, fora do
-- git). Escopo 'termo' cobre palavra e hashtag; 'dominio' casa por sufixo.
CREATE TABLE IF NOT EXISTS gold.termo_bloqueado (
    termo   TEXT NOT NULL,
    escopo  TEXT NOT NULL,
    PRIMARY KEY (termo, escopo)
);
DO $$
BEGIN
    IF pg_stat_file('/seeds/termo_bloqueado.csv', true) IS NOT NULL THEN
        COPY gold.termo_bloqueado FROM '/seeds/termo_bloqueado.csv' WITH (FORMAT csv, HEADER true);
    END IF;
END $$;


-- ===================== GOLD: fato eventos (principal) =====================
-- Grão: janela de 30s × tipo_evento × idioma × tipo_conteudo.
CREATE TABLE IF NOT EXISTS gold.fato_eventos (
    id             BIGSERIAL PRIMARY KEY,
    janela_inicio  TIMESTAMPTZ NOT NULL,
    janela_fim     TIMESTAMPTZ NOT NULL,
    tipo_evento    TEXT NOT NULL,
    idioma         TEXT NOT NULL,
    tipo_conteudo  TEXT NOT NULL,
    qtd            INTEGER NOT NULL,
    UNIQUE (janela_inicio, tipo_evento, idioma, tipo_conteudo)
);
CREATE INDEX IF NOT EXISTS idx_fato_eventos_janela ON gold.fato_eventos (janela_inicio);


-- ===================== GOLD: fato palavra =====================
-- Grão: janela de 30s × idioma × palavra (top-N por janela). Particionada por
-- dia; partições e retenção gerenciadas pelo Spark, igual à silver.
CREATE TABLE IF NOT EXISTS gold.fato_palavra (
    janela_inicio  TIMESTAMPTZ NOT NULL,
    janela_fim     TIMESTAMPTZ NOT NULL,
    idioma         TEXT NOT NULL,
    palavra        TEXT NOT NULL,
    qtd            INTEGER NOT NULL,
    PRIMARY KEY (janela_inicio, idioma, palavra)
) PARTITION BY RANGE (janela_inicio);
CREATE TABLE IF NOT EXISTS gold.fato_palavra_default PARTITION OF gold.fato_palavra DEFAULT;


-- ===================== GOLD: fato hashtag =====================
-- Grão: janela de 30s × idioma × hashtag.
CREATE TABLE IF NOT EXISTS gold.fato_hashtag (
    id             BIGSERIAL PRIMARY KEY,
    janela_inicio  TIMESTAMPTZ NOT NULL,
    janela_fim     TIMESTAMPTZ NOT NULL,
    idioma         TEXT NOT NULL,
    hashtag        TEXT NOT NULL,
    qtd            INTEGER NOT NULL,
    UNIQUE (janela_inicio, idioma, hashtag)
);
CREATE INDEX IF NOT EXISTS idx_fato_hashtag_janela ON gold.fato_hashtag (janela_inicio);


-- ===================== GOLD: fato domínio =====================
-- Grão: janela de 30s × idioma × dominio.
CREATE TABLE IF NOT EXISTS gold.fato_dominio (
    id             BIGSERIAL PRIMARY KEY,
    janela_inicio  TIMESTAMPTZ NOT NULL,
    janela_fim     TIMESTAMPTZ NOT NULL,
    idioma         TEXT NOT NULL,
    dominio        TEXT NOT NULL,
    qtd            INTEGER NOT NULL,
    UNIQUE (janela_inicio, idioma, dominio)
);
CREATE INDEX IF NOT EXISTS idx_fato_dominio_janela ON gold.fato_dominio (janela_inicio);


-- ===================== GOLD: dimensão tempo =====================
-- Derivada das janelas de tempo que existem nos fatos.
CREATE OR REPLACE VIEW gold.dim_tempo AS
SELECT
    janela_inicio                                   AS tempo,
    janela_inicio::date                             AS data,
    EXTRACT(HOUR   FROM janela_inicio)::int         AS hora,
    EXTRACT(DOW    FROM janela_inicio)::int         AS dia_semana_num,
    TRIM(TO_CHAR(janela_inicio, 'TMDay'))           AS dia_semana,
    CASE
        WHEN EXTRACT(HOUR FROM janela_inicio) <  6 THEN 'madrugada'
        WHEN EXTRACT(HOUR FROM janela_inicio) < 12 THEN 'manha'
        WHEN EXTRACT(HOUR FROM janela_inicio) < 18 THEN 'tarde'
        ELSE 'noite'
    END                                             AS periodo
FROM gold.fato_eventos
GROUP BY janela_inicio;
