import time

import evaluate
from datasets import load_from_disk
from transformers import AutoModelForTokenClassification, Trainer, TrainingArguments, \
    DataCollatorForTokenClassification, AutoTokenizer, EvalPrediction, EarlyStoppingCallback

from configuration import config

# 模型
model = AutoModelForTokenClassification.from_pretrained(config.CHECKPOINT_DIR / 'ner' / 'best_model')

# 数据
test_dataset = load_from_disk(config.DATA_DIR / 'ner' / 'processed' / 'test')

# 分词器
tokenizer = AutoTokenizer.from_pretrained(config.CHECKPOINT_DIR / 'ner' / 'best_model')
# 数据整理器
data_collator = DataCollatorForTokenClassification(tokenizer=tokenizer, padding=True, return_tensors='pt')

# 指标计算函数
seqeval = evaluate.load('seqeval')


def compute_metrics(prediction: EvalPrediction):
    logits = prediction.predictions
    preds = logits.argmax(axis=-1)
    labels = prediction.label_ids
    unpad_labels = []
    unpad_preds = []
    for pred, label in zip(preds, labels):
        # 去掉填充
        unpad_label = label[label != -100]
        unpad_pred = pred[label != -100]
        # 转标签
        unpad_pred = [model.config.id2label[id] for id in unpad_pred]
        unpad_label = [model.config.id2label[id] for id in unpad_label]
        unpad_labels.append(unpad_label)
        unpad_preds.append(unpad_pred)
    return seqeval.compute(predictions=unpad_preds, references=unpad_labels)


# 训练器
trainer = Trainer(model=model,
                  eval_dataset=test_dataset,
                  data_collator=data_collator,
                  compute_metrics=compute_metrics)

print(trainer.evaluate())
