"""
Embedding management utilities using OpenAI API.
"""

import logging
import os
import time
from typing import List

from openai import OpenAI, RateLimitError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class EmbeddingManager:
    """OpenAI APIを使用したembedding管理クラス"""

    def __init__(self):
        """
        OpenAI APIクライアントを初期化
        """
        self.client: OpenAI
        self.model = "text-embedding-3-small"
        self.dimensions = 1536

    def initialize(self) -> bool:
        """OpenAI APIクライアントを初期化"""
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            print("⚠️  OpenAI API key not found in environment")
            return False

        try:
            self.client = OpenAI(api_key=api_key)
            print("✅ OpenAI API client initialized")
            return True
        except Exception as e:
            print(f"❌ Failed to initialize OpenAI client: {e}")
            return False

    @retry(
        retry=retry_if_exception_type((RateLimitError, Exception)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
    )
    async def create_embedding(self, text: str) -> List[float]:
        """
        テキストからembeddingを生成（rate limit対策付き）

        Args:
            text: embedding化するテキスト

        Returns:
            embedding vector
        """
        if not self.client:
            raise RuntimeError("OpenAI client not initialized")

        try:
            response = self.client.embeddings.create(
                model=self.model, input=text, dimensions=self.dimensions
            )
            return response.data[0].embedding
        except RateLimitError as e:
            print(f"Rate limit exceeded, retrying: {e}")
            raise
        except Exception as e:
            print(f"Error creating embedding: {e}")
            raise

    @retry(
        retry=retry_if_exception_type((RateLimitError, Exception)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
    )
    async def create_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        複数のテキストからembeddingを一括生成（rate limit対策付き）

        Args:
            texts: embedding化するテキストのリスト

        Returns:
            embedding vectorsのリスト
        """
        if not self.client:
            raise RuntimeError("OpenAI client not initialized")

        # バッチサイズが大きすぎる場合は分割処理
        max_batch_size = 100  # OpenAI APIの推奨バッチサイズ
        if len(texts) > max_batch_size:
            print(
                f"Large batch detected ({len(texts)} texts), splitting into smaller batches"
            )
            all_embeddings = []
            for i in range(0, len(texts), max_batch_size):
                batch = texts[i : i + max_batch_size]
                batch_embeddings = await self._create_embeddings_batch_internal(batch)
                all_embeddings.extend(batch_embeddings)
                # バッチ間で少し待機してrate limitを回避
                if i + max_batch_size < len(texts):
                    time.sleep(0.1)
            return all_embeddings
        else:
            return await self._create_embeddings_batch_internal(texts)

    @retry(
        retry=retry_if_exception_type((RateLimitError, Exception)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
    )
    async def _create_embeddings_batch_internal(
        self, texts: List[str]
    ) -> List[List[float]]:
        """
        内部的なバッチ処理メソッド
        """
        try:
            response = self.client.embeddings.create(
                model=self.model, input=texts, dimensions=self.dimensions
            )
            return [data.embedding for data in response.data]
        except RateLimitError as e:
            print(f"Rate limit exceeded for batch of {len(texts)} texts, retrying: {e}")
            raise
        except Exception as e:
            print(f"Error creating batch embeddings for {len(texts)} texts: {e}")
            raise

    def is_available(self) -> bool:
        """OpenAI APIが利用可能かどうかを返す"""
        return self.client is not None

    def get_dimensions(self) -> int:
        """embedding次元数を返す"""
        return self.dimensions
