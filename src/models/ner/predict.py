import torch
from transformers import BertForTokenClassification, BertTokenizer

from configuration import config


class Predictor:
    def __init__(self,device):
        self.device = device
        self.model = BertForTokenClassification.from_pretrained(str(config.CHECKPOINT_DIR /'ner'/'best_model')).to(self.device)
        self.tokenizer = BertTokenizer.from_pretrained(str(config.CHECKPOINT_DIR /'ner'/'best_model'))
    def predict(self,texts):
        type_text=isinstance(texts, str)
        if type_text :
            texts = [texts]
        text_list=[list(text) for text in texts]
        inputs=self.tokenizer(text_list,return_tensors='pt',padding=True,truncation=True,is_split_into_words=True)
        inputs={k:v.to(self.device) for k,v in inputs.items()}
        with torch.no_grad():
            with torch.autocast(str(self.device),dtype=torch.float16):
                outputs = self.model(**inputs)
        logits = outputs.logits.argmax(dim=-1)
        logits = [logit[input != self.tokenizer.pad_token_id][1:-1].tolist() for input,logit in zip(inputs['input_ids'],logits)]
        logits=[[self.model.config.id2label[idx] for idx in logit] for logit in logits]
        if type_text:
            return logits[0]
        return logits

    def extract(self,texts):
        type_text = isinstance(texts, str)
        if type_text:
            texts = [texts]
        texts=[text.replace(' ','') for text in texts]
        tokens=self.predict(texts)
        final_pred=[]
        for token,text in zip(tokens,texts):
            cur_list=[]
            cur_pred=''
            for tok,tex in zip(token,text):
                if tok=='B':
                    if cur_pred:
                        cur_list.append(cur_pred)
                    cur_pred=tex
                if tok=='I':
                    if cur_pred:
                        cur_pred+=tex
                if tok=='O':
                    if cur_pred:
                        cur_list.append(cur_pred)
                    cur_pred=''
            if cur_pred:
                cur_list.append(cur_pred)
            final_pred.append(cur_list)
        if type_text:
            return final_pred[0]
        return final_pred


if __name__ == '__main__':
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    predictor=Predictor(device)
    res=predictor.extract('16- 尚 硅 谷 -Flume进阶-架构原理')
    print(res)
    print(len(res))
