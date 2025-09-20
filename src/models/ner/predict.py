import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer

from configuration import config


class Predictor:
    def __init__(self,model,tokenizer,device):
        self.model = model.to(device)
        self.model.eval()
        self.tokenizer = tokenizer
        self.device = device

    def predict(self,inputs:str|list):
        is_str = isinstance(inputs,str)
        if is_str:
            inputs = [inputs]
        tokens_list = [list(input) for input in inputs]
        inputs_tensor = self.tokenizer(tokens_list,is_split_into_words=True,truncation=True,padding=True,return_tensors='pt')
        inputs_tensor = {k:v.to(self.device) for k,v in inputs_tensor.items()}
        with torch.no_grad():
            outputs = self.model(**inputs_tensor)
            logits = outputs.logits
            predictions = torch.argmax(logits,dim=-1).tolist()
        final_predictions = []
        for tokens,prediction in zip(tokens_list,predictions):
            prediction = prediction[1:1+len(tokens)]
            final_prediction = [self.model.config.id2label[id] for id in prediction]
            final_predictions.append(final_prediction)
        if is_str:
            return final_predictions[0]
        return final_predictions


    def extract(self,inputs:str|list):
        is_str = isinstance(inputs,str)
        if is_str:
            inputs = [inputs]
        predictions = self.predict(inputs)
        all_entities = []
        for input,labels in zip(inputs,predictions):
            entities = self._extract_entity(list(input),labels)
            all_entities.append(entities)
        if is_str:
            return all_entities[0]
        return all_entities

    def _extract_entity(self,tokens,labels):
        entities = []
        current_entity = ""
        for token,label in zip(tokens,labels):
            if label == 'B':
                if current_entity:
                    entities.append(current_entity)
                current_entity = token
            elif label == 'I':
                if current_entity:
                    current_entity += token
            else:
                if current_entity:
                    entities.append(current_entity)
                current_entity = ""
        if current_entity:
            entities.append(current_entity)
        return entities
if __name__ == '__main__':
    model = AutoModelForTokenClassification.from_pretrained(str(config.CHECKPOINT_DIR/'ner'/'best_model'))
    tokenizer = AutoTokenizer.from_pretrained(str(config.CHECKPOINT_DIR/'ner'/'best_model'))
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    predictor = Predictor(model,tokenizer,device)
    text ="系统讲解Linux操作系统常用命令、用户权限管理、软件安装、Shell脚本编写等内容。"
    labels = predictor.extract(text)
    print(labels)