import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer
from conf import config


class Predictor:
    def __init__(self,model,tokenizer,device):
        self.model=model
        self.tokenizer=tokenizer
        self.model.to(device)
        self.device=device

    def predict(self,text):
        #为了批量处理，如果数据是单一字符串则同样转为字符串列表
        is_str=isinstance(text,str)
        if is_str:
            text=[text]
        text_list=[list(item) for item in text]
        token_list=self.tokenizer(text_list,is_split_into_words=True,truncation=True,padding=True,return_tensors='pt')
        token_list={key:value.to(device) for key,value in token_list.items()}
        with torch.no_grad():
            output=self.model(**token_list)
            logits=output.logits
            pres=torch.argmax(logits,dim=-1).tolist()

            # 接收最终的预测结果
            final_pres = []
            # pre 是预测结果的索引列表 需要把索引的列表转为对应的BIO标签
            for i, (pre, token) in enumerate(zip(pres, text_list)):
                #-------------------ai逻辑-------------------------
                # 正确处理长度：使用attention_mask来确定实际长度
                attention_mask = token_list['attention_mask'][i]
                actual_length = attention_mask.sum().item()

                # 去除[CLS]和[SEP]标记
                if actual_length > 2:
                    pre = pre[1:actual_length - 1]  # 去掉第一个[CLS]和最后一个[SEP]
                else:
                    pre = pre[1:]  # 至少去掉[CLS]

                # 确保预测结果长度与输入文本长度一致
                if len(pre) > len(token):
                    pre = pre[:len(token)]
                elif len(pre) < len(token):
                    # 如果预测结果不够，补充'O'标签
                    pre = pre + [0] * (len(token) - len(pre))
                #---------------------------------------------------

                final_pre = [self.model.config.id2label[id] for id in pre]
                final_pres.append(final_pre)

            if is_str:
                return final_pres[0]
            return final_pres


    # 提取的方法将模型最后预测出来的结果中的标签按BIO提取
    def extract(self,inputs: list | str):
        is_str = isinstance(inputs, str)
        if is_str:
            inputs = [inputs]
        labels=self.predict(inputs)
        labels=[''.join(item) for item in labels ]

        #储存最终的标签的列表
        tag_lists=[]
        for i in range(len(labels)):
            label_str=''
            inputs_str=''
            for label,inp in zip(labels[i],inputs[i]):
                if label!='O':
                    label_str+=label
                    inputs_str+=inp
            #得到的结果是label_str='BIIIBII',inputs_str='tag1tag2'
            label_strs=['B'+part for part in label_str.split('B')[1:]]
            label_strs_index=[len(part) for part in label_strs]
            tag_list=[]
            for index in label_strs_index:
                tag_list.append(inputs_str[:index])
                inputs_str=inputs_str[index:]
            tag_lists.append(tag_list)
        return tag_lists






if __name__ == '__main__':
    model=AutoModelForTokenClassification.from_pretrained(config.CHECKPOINT_DIR/'ner/best_model')
    tokenizer=AutoTokenizer.from_pretrained(config.MODEL_NAME)
    device=torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    predictor=Predictor(model,tokenizer,device)
    print(predictor.extract('day09_14static关键字的练习'))

