"""
Data processing utilities for handling Q&A data and database operations using SQLAlchemy ORM.
"""

import time
from typing import Any, Optional

import polars as pl
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from db.models.schema import Chunks, CopyrightHolders, Sources

from .embedding import EmbeddingManager
from .logger import DualLogger
from .text_processing import TextProcessor


class DataProcessor:
    """データ処理とデータベース操作を担当するクラス（SQLAlchemy ORM使用）"""

    def __init__(self, connection_string: str, logger: Optional[DualLogger] = None):
        """
        Args:
            connection_string: データベース接続文字列
            logger: ログ出力用のロガー
        """
        self.engine = create_engine(connection_string)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.text_processor = TextProcessor()
        self.embedding_manager = EmbeddingManager()
        self.logger = logger or DualLogger()

    async def initialize(self) -> bool:
        """データプロセッサーを初期化"""
        # embedding管理の初期化
        embedding_available = self.embedding_manager.initialize()

        if embedding_available:
            self.logger.info("✅ OpenAI embedding service is available")
        else:
            self.logger.warning("⚠️  OpenAI embedding service is not available")

        return embedding_available

    def insert_copyright_holder(self, session: Session, name: str) -> int:
        """著作権者を挿入"""
        try:
            # 既存の著作権者を検索
            existing = session.execute(
                select(CopyrightHolders).where(CopyrightHolders.name == name)
            ).scalar_one_or_none()

            if existing:
                return existing.id

            # 新しい著作権者を作成
            copyright_holder = CopyrightHolders(name=name)
            session.add(copyright_holder)
            session.flush()  # IDを取得するためにflush
            return copyright_holder.id

        except IntegrityError:
            session.rollback()
            # 重複エラーの場合、再度検索
            existing = session.execute(
                select(CopyrightHolders).where(CopyrightHolders.name == name)
            ).scalar_one()
            return existing.id

    def insert_source(
        self, session: Session, copyright_holder_id: int, url: str
    ) -> int:
        """ソースを挿入"""
        try:
            # 既存のソースを検索
            existing = session.execute(
                select(Sources).where(Sources.url == url)
            ).scalar_one_or_none()

            if existing:
                return existing.id

            # 新しいソースを作成
            source = Sources(copyright_holder_id=copyright_holder_id, url=url)
            session.add(source)
            session.flush()  # IDを取得するためにflush
            return source.id

        except IntegrityError:
            session.rollback()
            # 重複エラーの場合、再度検索
            existing = session.execute(
                select(Sources).where(Sources.url == url)
            ).scalar_one()
            return existing.id

    async def insert_chunks_with_embeddings(
        self, session: Session, source_id: int, chunks_data: list[dict[str, Any]]
    ):
        """embeddingを生成してチャンクを挿入"""
        if not chunks_data:
            return

        # テキストを抽出してembeddingを一括生成
        texts = [chunk_data["text"] for chunk_data in chunks_data]
        embeddings = await self.embedding_manager.create_embeddings_batch(texts)

        # チャンクを作成
        chunks = []
        for chunk_data, embedding in zip(chunks_data, embeddings):
            chunk = Chunks(
                source_id=source_id,
                content=chunk_data["text"],
                embeding=embedding,  # スキーマのtypoに合わせる
                metadata_=chunk_data["metadata"],
            )
            chunks.append(chunk)

        session.add_all(chunks)

    async def process_row(
        self, session: Session, row: dict[str, Any], chunk_strategy: str = "token"
    ) -> int:
        """1行のデータを処理"""
        try:
            # 著作権者とソース挿入
            copyright_holder_id = self.insert_copyright_holder(
                session, row["copyright"]
            )
            source_id = self.insert_source(session, copyright_holder_id, row["url"])

            chunks_data = []

            # 1. 質問を1つのチャンクとして追加
            question_metadata = {
                "type": "question",
                "question": row["Question"],
                "answer": row["Answer"],
                "chunk_info": {
                    "chunk_method": "single",
                    "chunk_index": 0,
                    "is_question": True,
                },
            }

            chunks_data.append({"text": row["Question"], "metadata": question_metadata})

            # 2. 回答をチャンキング
            answer_chunks = self.text_processor.chunk_text(
                row["Answer"], chunk_strategy
            )

            for i, chunk_text in enumerate(answer_chunks):
                answer_metadata = {
                    "type": "answer",
                    "question": row["Question"],
                    "answer": row["Answer"],
                    "answer_chunk": chunk_text,
                    "chunk_info": {
                        "chunk_method": chunk_strategy,
                        "chunk_index": i + 1,  # question is index 0
                        "total_answer_chunks": len(answer_chunks),
                        "original_length": len(row["Answer"]),
                        "is_question": False,
                    },
                }

                chunks_data.append({"text": chunk_text, "metadata": answer_metadata})

            # チャンク挿入（embeddingを生成）
            if self.embedding_manager.is_available():
                await self.insert_chunks_with_embeddings(
                    session, source_id, chunks_data
                )
            else:
                raise RuntimeError("Embedding service not available")

            return len(chunks_data)

        except Exception as e:
            self.logger.log_error(
                e, f"processing row with URL: {row.get('url', 'unknown')}"
            )
            session.rollback()
            return 0

    async def insert_dataframe(
        self,
        df: pl.DataFrame,
        chunk_strategy: str = "token",
        limit: Optional[int] = None,
        progress_interval: int = 100,
        commit_interval: int = 50,
    ) -> dict[str, int]:
        """DataFrameからデータを挿入"""
        data = df.to_dicts()
        total_rows = len(data)

        # limitが指定されている場合は制限
        if limit is not None:
            data = data[:limit]
            total_rows = len(data)

        # 処理開始ログ
        self.logger.log_processing_start(total_rows, chunk_strategy)

        if self.embedding_manager.is_available():
            self.logger.info("OpenAI APIを使用してembeddingを生成します")
        else:
            self.logger.error("Embedding service not available")
            return {"processed_rows": 0, "total_chunks": 0}

        total_chunks = 0
        processed = 0
        start_time = time.time()

        with self.SessionLocal() as session:
            try:
                for i, row in enumerate(data):
                    chunks_count = await self.process_row(session, row, chunk_strategy)
                    total_chunks += chunks_count
                    processed += 1

                    # 定期的にコミット
                    if processed % commit_interval == 0:
                        session.commit()
                        self.logger.log_commit(processed, total_chunks)

                    # 進捗ログ
                    if processed % progress_interval == 0:
                        elapsed_time = time.time() - start_time
                        self.logger.log_processing_progress(
                            processed, total_rows, total_chunks, elapsed_time
                        )

                # 最終コミット
                session.commit()
                self.logger.log_commit(processed, total_chunks)

            except Exception as e:
                session.rollback()
                self.logger.log_error(e, "データ挿入処理")
                raise

        # 処理完了ログ
        elapsed_time = time.time() - start_time
        self.logger.log_processing_complete(processed, total_chunks, elapsed_time)

        return {"processed_rows": processed, "total_chunks": total_chunks}

    def get_database_statistics(self) -> dict[str, int]:
        """データベースの統計情報を取得"""
        with self.SessionLocal() as session:
            copyright_count = session.query(CopyrightHolders).count()
            source_count = session.query(Sources).count()
            chunk_count = session.query(Chunks).count()

            return {
                "copyright_holders": copyright_count,
                "sources": source_count,
                "chunks": chunk_count,
            }

    def check_embeddings(self) -> dict[str, Any]:
        """embeddingが正しく生成されているか確認"""
        with self.SessionLocal() as session:
            chunk = session.query(Chunks).filter(Chunks.embeding.isnot(None)).first()

            if chunk:
                return {
                    "has_embeddings": True,
                    "embedding_dimension": len(chunk.embeding),
                    "sample_text": chunk.content[:100] + "...",
                }
            else:
                return {
                    "has_embeddings": False,
                    "embedding_dimension": None,
                    "sample_text": None,
                }
