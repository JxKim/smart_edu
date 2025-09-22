import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer

from configuration import config


class Predictor:
    def __init__(self, model, tokenizer, device):
        self.model = model.to(device)
        self.model.eval()
        self.tokenizer = tokenizer
        self.device = device

    def predict(self, inputs):
        """
        预测token标签
        :param inputs: 输入文本
        :return: token标签
        """
        is_str = isinstance(inputs, str)
        if is_str:
            inputs = [inputs]

        # 二维分词列表
        tokens_list = [list(input) for input in inputs]
        inputs_tensors = self.tokenizer(tokens_list, is_split_into_words=True, padding=True, truncation=True,
                                        return_tensors='pt')
        inputs_tensors = {key: value.to(self.device) for key, value in inputs_tensors.items()}
        # 预测标签id
        with torch.no_grad():
            outputs = self.model(**inputs_tensors)
            logits = outputs.logits
            predictions = torch.argmax(logits, dim=-1).tolist()

        # 截取预测结果并转为label
        final_predictions = []
        for tokens, prediction in zip(tokens_list, predictions):
            prediction = prediction[1:1 + len(tokens)]
            final_prediction = [self.model.config.id2label[id] for id in prediction]
            final_predictions.append(final_prediction)

        if is_str:
            return final_predictions[0]
        return final_predictions

    def extract(self, inputs):
        """
        提取商品描述中的实体
        :param inputs: 商品描述
        :return: 实体列表
        """
        is_str = isinstance(inputs, str)
        if is_str:
            inputs = [inputs]
        all_entities = []
        for input, prediction in zip(inputs, self.predict(inputs)):
            entities = self._extract(input, prediction)
            all_entities.append(entities)
        if is_str:
            return all_entities[0]
        return all_entities

    def _extract(self, tokens, labels):
        """
        单条商品描述的实体提取逻辑
        :param tokens: 商品描述分词后列表
        :param labels: 商品预测标签列表
        :return:
        """
        entities = []
        cur_entity = ''
        for token, label in zip(tokens, labels):
            if label == 'B':
                if cur_entity:
                    entities.append(cur_entity)
                cur_entity = token
            elif label == 'I':
                if cur_entity:
                    cur_entity += token
            else:
                if cur_entity:
                    entities.append(cur_entity)
                    cur_entity = ''
        if cur_entity:
            entities.append(cur_entity)
        return entities


if __name__ == '__main__':
    BEST_MODEL = str(config.CHECK_POINT / 'ner' / 'best_model')
    model = AutoModelForTokenClassification.from_pretrained(BEST_MODEL)
    tokenizer = AutoTokenizer.from_pretrained(BEST_MODEL)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    predictor = Predictor(model, tokenizer, device)
    text = '09-尚硅谷-Flume监控本地文件上传HDFS-配置信息'
    # predictions = predictor.predict(text)
    # for c, label in zip(text, predictions):
    #     print(c, label)
    print(predictor.extract(text))
