import torch
import unicodedata
from transformers import AutoModelForTokenClassification, AutoTokenizer

from configuration import config


class Predictor:
    def __init__(self, model, tokenizer, device):
        self.model = model.to(device)
        self.model.eval()
        self.tokenizer = tokenizer
        self.device = device

    def _preprocess_text(self, text: str) -> str:
        '''用户输入文本预处理:全角转半角+去除所有空格'''
        # 1.全角转半角
        processed = []
        for char in text:
            normalized_char = unicodedata.normalize('NFKC', char)
            # 处理全角符号(除空格外)
            if '\uFF01' <= normalized_char <= '\uFF5E':
                processed.append(chr(ord(normalized_char) - 0xfee0))
            # 处理全角空格
            elif normalized_char == '\u3000':
                processed.append(' ')
            else:
                processed.append(normalized_char)
        text = ''.join(processed)

        # 2.去除所有空格(包括半角空格)
        text = text.replace(' ', '')
        return text

    def predict(self, inputs: str | list):
        is_str = isinstance(inputs, str)
        inputs = [inputs] if is_str else inputs

        #🔥对每个输入先预处理 转半角+删除空格
        processed_inputs = [self._preprocess_text(text) for text in inputs]

        tokens_list = [list(text.lower()) for text in processed_inputs] #❗转小写
        inputs_tensor = self.tokenizer(tokens_list, is_split_into_words=True, padding=True, truncation=True,
                                       return_tensors='pt')
        inputs_tensor = {k: v.to(self.device) for k, v in inputs_tensor.items()}
        with torch.no_grad():
            outputs = self.model(**inputs_tensor)
            logits = outputs.logits
            predictions = torch.argmax(logits, dim=-1).tolist()

        final_predictions = []
        for tokens, prediction in zip(tokens_list, predictions):
            prediction = prediction[1:1 + len(tokens)]
            final_prediction = [self.model.config.id2label[id] for id in prediction]
            final_predictions.append(final_prediction)

        if is_str:
            return processed_inputs[0],final_predictions[0]
        return processed_inputs,final_predictions

    def extract(self, inputs: str | list):
        is_str = isinstance(inputs, str)
        if is_str:
            inputs = [inputs]

        processed_inputs,final_predictions = self.predict(inputs)
        entities = []
        for text, final_prediction in zip(processed_inputs, final_predictions):
            # import pdb;pdb.set_trace()
            entity = self._extract_entity(text, final_prediction)
            entities.append(entity)
        if is_str:
            return entities[0]
        return entities

    def _extract_entity(self, text, final_prediction): #单个样本 和 对应的预测标签
        all_entities = []
        cur_entity = ''
        for char, tag in zip(list(text), final_prediction):
            if tag == 'B':
                if cur_entity:
                    all_entities.append(cur_entity)
                    cur_entity = ''
                cur_entity += char

            elif tag == 'I':
                if cur_entity:
                    cur_entity += char

            else:
                if cur_entity:
                    all_entities.append(cur_entity)
                cur_entity = ''

        if cur_entity:
            all_entities.append(cur_entity)
        return all_entities




if __name__ == '__main__':
    model = AutoModelForTokenClassification.from_pretrained(config.CHECKPOINT_DIR / 'ner' / 'best_model')
    tokenizer = AutoTokenizer.from_pretrained(config.CHECKPOINT_DIR / 'ner' / 'best_model')
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    predictor = Predictor(model=model, tokenizer=tokenizer, device=device)


    texts = '讲解ApacheSpark分布式计算 框架的 使用,包括RDD、DataFrame、SQL查询、流式处理、任务调度等。'
    texts2 = ['138_尚硅谷_Hadoop_Yarn_容量调度器任务优先级','讲解ApacheSpark分布式计算 框架的 使用,包括RDD、DataFrame、SQL查询、流式处理、任务调度等。']
    texts3 ='<p><strong>以下关于方法调用的代码的执行结果是</strong></p><p>&nbsp;</p><p>&nbsp;public class Test {</p><p>&nbsp;&nbsp;&nbsp;public static void main(String args[]) {</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;int i = 99;</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;mb_operate(i);</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;System.out.print(i + 100);</p><p>&nbsp;&nbsp;&nbsp;}</p><p>&nbsp;&nbsp;&nbsp;static int mb_operate(int i) {</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;return&nbsp;i + 100;</p><p>&nbsp;&nbsp;&nbsp;}</p><p>}</p>'
    texts4 = '以下关于方法调用的代码的执行结果是public class Test '
    texts5 = 'question_txt: "<p>在进行 Java 代码编译和运行的过程中，哪种格式的文件可以被编译成字节码文件（）</p>"'
    # processed_texts,labels = predictor.predict(texts2)
    # for text, label in zip(processed_texts,labels):
    #     # import pdb;pdb.set_trace()
    #     print(text,'→',label)

    #----------------------------------------------------------
    print(predictor.extract(texts))
    #💡['ApacheSpark', 'RDD', 'DataFrame', 'SQL查询']
    print(predictor.extract(texts2))
    #💡[['Hadoop', 'Yarn', '容量', '调度器', '任务', '优先级'], ['ApacheSpark', 'RDD', 'DataFrame', 'SQL查询']]
    print(predictor.extract(texts3))
    print(predictor.extract(texts5))