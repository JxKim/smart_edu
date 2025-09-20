class CharVocabCleaner:
    """
    按字符粒度清洗：只保留 vocab.txt 中存在的字符，其余全部过滤掉。
    适用于字符级模型（如 Char-RNN、部分中文模型等）。
    """

    def __init__(self, vocab_file: str):
        """
        :param vocab_file: 词表文件路径，每行一个字符（或你允许保留的单元）
        """
        with open(vocab_file, "r", encoding="utf-8") as f:
            # 读取所有行，去掉换行，构建字符集合
            self.vocab = set()
            for line in f:
                char = line.strip()
                if char:  # 忽略空行
                    self.vocab.add(char)

    def clean_text(self, text: str) -> str:
        """
        逐字符过滤：只保留词表中存在的字符

        :param text: 原始字符串
        :return: 过滤后字符串
        """
        if not isinstance(text, str):
            return ""
        return ''.join(char for char in text if char in self.vocab)

    def clean_batch(self, texts: list) -> list:
        """
        批量清洗
        """
        return [self.clean_text(text) for text in texts]

    def __repr__(self):
        return f"<CharVocabCleaner vocab_size={len(self.vocab)}>"