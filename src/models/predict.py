import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer
from configuration import config


class Predictor:
    def __init__(self, model, tokenizer, device):
        # 保存传入的模型（加载到指定 device 上），并切换到推理模式
        self.model = model.to(device)
        self.model.eval()
        self.tokenizer = tokenizer
        self.device = device

    def predict(self, inputs: str | list):
        """
        预测输入文本对应的标签序列（逐 token 的类别）。
        :param inputs: 单个字符串 或 字符串列表
        :return: 每个 token 对应的标签序列
        """
        # 判断输入是否是单个字符串，如果是，转成列表方便处理
        is_str = isinstance(inputs, str)
        if is_str:
            inputs = [inputs]

        # 将每个输入句子拆成字符级 token 列表（例："苹果手机" -> ["苹","果","手","机"]）
        tokens_list = [list(input) for input in inputs]

        # 使用 tokenizer 编码，保持拆好的字符粒度
        inputs_tensor = self.tokenizer(
            tokens_list,
            is_split_into_words=True,   # 表示输入已经拆分好，不要再分词
            truncation=True,            # 超长截断
            padding=True,               # 批量对齐
            return_tensors='pt'         # 返回 PyTorch tensor
        )

        # 将输入 tensor 放到指定设备（CPU/GPU）
        inputs_tensor = {k: v.to(self.device) for k, v in inputs_tensor.items()}

        # 前向推理，禁用梯度计算（更快，省显存）
        with torch.no_grad():
            outputs = self.model(**inputs_tensor)   # 模型输出 (logits)
            logits = outputs.logits                 # shape = [batch_size, seq_len, num_labels]
            predictions = torch.argmax(logits, dim=-1).tolist()  # 取最大概率类别 → [batch_size, seq_len]

        # 后处理：将预测的 id 转成标签字符串
        final_predictions = []
        for tokens, prediction in zip(tokens_list, predictions):
            # 去掉 tokenizer 自动加的特殊符号 [CLS]、[SEP] 的预测，只保留和原 tokens 对应的部分
            prediction = prediction[1:1 + len(tokens)]
            # id → 标签名 (比如 0 → "O"，1 → "B"，2 → "I")
            final_prediction = [self.model.config.id2label[id] for id in prediction]
            final_predictions.append(final_prediction)

        # 如果输入是单个字符串，返回单个预测结果
        if is_str:
            return final_predictions[0]
        return final_predictions

    def extract(self, inputs: str | list):
        """
        基于 predict 结果，抽取出最终的实体（连续的 B/I 标签拼接成一个实体）。
        :param inputs: 输入文本 或 文本列表
        :return: 实体字符串列表
        """
        is_str = isinstance(inputs, str)
        if is_str:
            inputs = [inputs]

        all_entities = []

        # 先预测标签
        predictions = self.predict(inputs)

        # 遍历每条输入和对应预测结果
        for input, labels in zip(inputs, predictions):
            entities = self._extract_entity(list(input), labels)
            all_entities.append(entities)

        if is_str:
            return all_entities[0]
        return all_entities

    def _extract_entity(self, tokens, labels):
        """
        从 token 和对应的 B/I/O 标签里提取实体。
        - B 开始新的实体
        - I 继续当前实体
        - O 表示非实体
        """
        entities = []
        current_entity = ""
        for token, label in zip(tokens, labels):
            if label == 'B':  # 开始一个新实体
                if current_entity:  # 如果之前有实体，先收集
                    entities.append(current_entity)
                current_entity = token
            elif label == 'I':  # 当前实体继续
                if current_entity:
                    current_entity += token
            else:  # O 或其他情况：结束实体
                if current_entity:
                    entities.append(current_entity)
                current_entity = ""
        # 循环结束后，最后一个实体如果没保存，补上
        if current_entity:
            entities.append(current_entity)
        return entities


if __name__ == '__main__':
    # 1. 加载 NER 模型和分词器
    model = AutoModelForTokenClassification.from_pretrained(
        str(config.CHECKPOINT_DIR / 'ner' / 'best_model')
    )
    tokenizer = AutoTokenizer.from_pretrained(
        str(config.CHECKPOINT_DIR / 'ner' / 'best_model')
    )

    # 2. 选择设备
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    # 3. 初始化预测器
    predictor = Predictor(model, tokenizer, device)

    # 4. 测试一条文本
    text = "麦德龙德国进口双心多维叶黄素护眼营养软胶囊30粒x3盒眼干涩"

    # 5. 抽取实体
    entities = predictor.extract(text)
    print(entities)
