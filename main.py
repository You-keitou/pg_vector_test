import asyncio

import polars as pl
from dotenv import find_dotenv, load_dotenv

from config.settings import settings
from core import DataProcessor, DualLogger

_ = load_dotenv(find_dotenv())

CONNECTION = settings.DATABASE_URL_SYNC


async def main():
    """メイン処理関数"""
    # ログ初期化
    logger = DualLogger("log.txt")

    # 接続文字列の確認
    if not CONNECTION:
        logger.error("❌ DATABASE_URL_SYNC environment variable not set")
        return

    logger.info("🚀 データ処理プログラムを開始します")

    try:
        # データ読み込み
        logger.info("Hugging Faceからデータを読み込み中...")
        df = pl.read_ndjson("hf://datasets/matsuxr/JaGovFaqs-22k/data.jsonl")
        logger.info(f"データ読み込み完了: {len(df):,} 行")

        # データプロセッサーの初期化（ロガーを渡す）
        processor = DataProcessor(CONNECTION, logger)
        embedding_available = await processor.initialize()

        if not embedding_available:
            logger.error(
                "⚠️  OpenAI embedding service is not available - 処理を中止します"
            )
            return

        # 全データセットを処理（limitを削除）
        logger.info("全データセットの処理を開始します...")
        await processor.insert_dataframe(
            df,
            chunk_strategy="token",
            limit=None,  # 無制限
            progress_interval=500,  # 500行ごとに進捗表示
            commit_interval=100,  # 100行ごとにコミット
        )

        # 統計情報の表示
        logger.info("データベース統計情報を取得中...")
        stats = processor.get_database_statistics()
        logger.log_statistics(stats)

        # embedding確認
        logger.info("Embedding生成状況を確認中...")
        embedding_info = processor.check_embeddings()
        logger.log_embedding_info(embedding_info)

        logger.info("🎉 全ての処理が正常に完了しました！")

    except KeyboardInterrupt:
        logger.warning("⚠️  ユーザーによって処理が中断されました")
    except Exception as e:
        logger.log_error(e, "メイン処理")
        raise
    finally:
        logger.info("プログラムを終了します")


if __name__ == "__main__":
    asyncio.run(main())
