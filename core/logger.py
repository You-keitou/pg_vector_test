"""
Logging utilities for data processing operations.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


class DualLogger:
    """標準出力とファイルの両方にログを出力するクラス"""

    def __init__(self, log_file: str = "log.txt", level: int = logging.INFO):
        """
        Args:
            log_file: ログファイルのパス
            level: ログレベル
        """
        self.log_file = Path(log_file)
        self.logger = logging.getLogger("data_processor")
        self.logger.setLevel(level)

        # 既存のハンドラーをクリア
        self.logger.handlers.clear()

        # フォーマッターを設定
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # コンソールハンドラー
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # ファイルハンドラー
        file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # プロパゲーションを無効にして重複を防ぐ
        self.logger.propagate = False

    def info(self, message: str):
        """情報レベルのログを出力"""
        self.logger.info(message)

    def warning(self, message: str):
        """警告レベルのログを出力"""
        self.logger.warning(message)

    def error(self, message: str):
        """エラーレベルのログを出力"""
        self.logger.error(message)

    def debug(self, message: str):
        """デバッグレベルのログを出力"""
        self.logger.debug(message)

    def log_processing_start(self, total_rows: int, chunk_strategy: str):
        """処理開始のログ"""
        self.info("=" * 60)
        self.info("データ処理を開始します")
        self.info(f"総行数: {total_rows:,}")
        self.info(f"チャンキング戦略: {chunk_strategy}")
        self.info(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.info("=" * 60)

    def log_processing_progress(
        self, processed: int, total: int, chunks_created: int, elapsed_time: float
    ):
        """処理進捗のログ"""
        percentage = (processed / total) * 100
        rate = processed / elapsed_time if elapsed_time > 0 else 0
        eta = (total - processed) / rate if rate > 0 else 0

        self.info(
            f"進捗: {processed:,}/{total:,} ({percentage:.1f}%) | "
            f"チャンク作成数: {chunks_created:,} | "
            f"処理速度: {rate:.1f} rows/sec | "
            f"推定残り時間: {eta:.0f}秒"
        )

    def log_processing_complete(
        self, processed: int, chunks_created: int, elapsed_time: float
    ):
        """処理完了のログ"""
        self.info("=" * 60)
        self.info("データ処理が完了しました")
        self.info(f"処理行数: {processed:,}")
        self.info(f"作成チャンク数: {chunks_created:,}")
        self.info(f"総処理時間: {elapsed_time:.2f}秒")
        self.info(f"平均処理速度: {processed / elapsed_time:.1f} rows/sec")
        self.info(f"完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.info("=" * 60)

    def log_error(self, error: Exception, context: Optional[str] = None):
        """エラーログ"""
        if context:
            self.error(f"エラーが発生しました ({context}): {str(error)}")
        else:
            self.error(f"エラーが発生しました: {str(error)}")

    def log_statistics(self, stats: dict):
        """統計情報のログ"""
        self.info("データベース統計:")
        for key, value in stats.items():
            self.info(f"  - {key}: {value:,}")

    def log_embedding_info(self, embedding_info: dict):
        """embedding情報のログ"""
        if embedding_info["has_embeddings"]:
            self.info("✅ Embeddings generated successfully!")
            self.info(
                f"  - Embedding dimension: {embedding_info['embedding_dimension']}"
            )
            self.info(f"  - Sample text: {embedding_info['sample_text']}")
        else:
            self.warning("⚠️  No embeddings found")

    def log_commit(self, processed_rows: int, total_chunks: int):
        """コミット時のログ"""
        self.info(f"💾 データベースにコミット完了: {processed_rows} 行処理済み, {total_chunks} チャンク作成済み")

    def log_batch_split(self, total_texts: int, batch_size: int):
        """バッチ分割時のログ"""
        num_batches = (total_texts + batch_size - 1) // batch_size
        self.info(f"📦 大きなバッチを分割: {total_texts} テキスト → {num_batches} バッチ (サイズ: {batch_size})")

    def log_rate_limit_retry(self, attempt: int, wait_time: float):
        """Rate limit再試行時のログ"""
        self.warning(f"⏳ Rate limit検出 - {wait_time:.1f}秒後に再試行 (試行回数: {attempt}/5)")
