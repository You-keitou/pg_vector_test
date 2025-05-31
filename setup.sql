-- 著作権者テーブル
CREATE TABLE copyright_holders (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ソーステーブル
CREATE TABLE sources (
    id SERIAL PRIMARY KEY,
    copyright_holder_id INTEGER REFERENCES copyright_holders(id),
    url TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- チャンクテーブル
CREATE TABLE chunks (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    chunk_text TEXT NOT NULL,
    embedding VECTOR(1536),
    metadata JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- pgaiの設定
SELECT ai.create_vectorizer(
     'chunks'::regclass,
     if_not_exists => true,
     loading => ai.loading_column(column_name=>'chunk_text'),
     destination => ai.destination_table(target_table=>'chunks',
         target_column=>'embedding'),
     embedding => ai.embedding_openai(model=>'text-embedding-small', dimensions=>'1536')
    )
