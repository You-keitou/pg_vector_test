"""
Embedding management utilities using OpenAI API.
"""

import os
from typing import List

from openai import OpenAI


class EmbeddingManager:
    """OpenAI APIを使用したembedding管理クラス"""

    def __init__(self):
        """
        OpenAI APIクライアントを初期化
        """
        self.client = None
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

    async def create_embedding(self, text: str) -> List[float]:
        """
        テキストからembeddingを生成

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
        except Exception as e:
            print(f"Error creating embedding: {e}")
            raise

    async def create_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        複数のテキストからembeddingを一括生成

        Args:
            texts: embedding化するテキストのリスト

        Returns:
            embedding vectorsのリスト
        """
        if not self.client:
            raise RuntimeError("OpenAI client not initialized")

        try:
            response = self.client.embeddings.create(
                model=self.model, input=texts, dimensions=self.dimensions
            )
            return [data.embedding for data in response.data]
        except Exception as e:
            print(f"Error creating batch embeddings: {e}")
            raise

    def is_available(self) -> bool:
        """OpenAI APIが利用可能かどうかを返す"""
        return self.client is not None

    def get_dimensions(self) -> int:
        """embedding次元数を返す"""
        return self.dimensions
