"""
Text processing and chunking utilities.
"""

from langchain.text_splitter import RecursiveCharacterTextSplitter, TokenTextSplitter
from langchain_text_splitters import CharacterTextSplitter


class TextProcessor:
    """テキストのチャンキング処理を担当するクラス"""

    def __init__(self):
        """チャンキング戦略を初期化"""
        self.text_splitters = {
            "recursive": RecursiveCharacterTextSplitter(
                chunk_size=500,
                chunk_overlap=50,
                separators=["\n\n", "\n", "。", ".", " ", ""],
            ),
            "token": TokenTextSplitter(chunk_size=400, chunk_overlap=40),
            "character": CharacterTextSplitter(
                chunk_size=600, chunk_overlap=60, separator="\n"
            ),
        }

    def chunk_text(self, text: str, strategy: str = "recursive") -> list[str]:
        """
        テキストをチャンキング

        Args:
            text: チャンキングするテキスト
            strategy: チャンキング戦略 ("recursive", "token", "character")

        Returns:
            チャンクされたテキストのリスト
        """
        splitter = self.text_splitters.get(strategy, self.text_splitters["recursive"])
        chunks = splitter.split_text(text)
        return chunks

    def get_available_strategies(self) -> list[str]:
        """利用可能なチャンキング戦略を取得"""
        return list(self.text_splitters.keys())

    def add_custom_splitter(self, name: str, splitter) -> None:
        """カスタムスプリッターを追加"""
        self.text_splitters[name] = splitter
