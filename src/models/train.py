import time

import datasets
import torch
import evaluate
from transformers import Trainer, AutoModelForTokenClassification, TrainingArguments, \
    DataCollatorForTokenClassification, AutoTokenizer, EvalPrediction, EarlyStoppingCallback

from conf import config

#加载训练集和验证集
train_dataset=datasets.load_from_disk(config.DATA_DIR/'ner'/'processed'/'train')
val_dataset=datasets.load_from_disk(config.DATA_DIR/'ner'/'processed'/'val')

#标签的映射
id2label={id:label for id,label in enumerate(config.LABELS)}
label2id={label:id for id,label in enumerate(config.LABELS)}
#模型
model=AutoModelForTokenClassification.from_pretrained(config.MODEL_NAME,
                                                      num_labels=len(config.LABELS),
                                                      id2label=id2label,
                                                      label2id=label2id
                                                      )
#优化器
optimizer=torch.optim.AdamW(model.parameters(),lr=5e-5,weight_decay=0.01)
#调度器
scheduler=torch.optim.lr_scheduler.LinearLR(optimizer)
#分词器
tokenizer=AutoTokenizer.from_pretrained(config.MODEL_NAME)

#数据处理器
data_collator=DataCollatorForTokenClassification(padding=True,
                                                 return_tensors='pt',
                                                 tokenizer=tokenizer)

#指标计算函数
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
        unpad_pred = [id2label[id] for id in unpad_pred]
        unpad_label = [id2label[id] for id in unpad_label]
        unpad_labels.append(unpad_label)
        unpad_preds.append(unpad_pred)
    return seqeval.compute(predictions=unpad_preds, references=unpad_labels)

#早停机制
early_stopping_callback=EarlyStoppingCallback(early_stopping_patience=10)

#训练参数

args=TrainingArguments(output_dir=str(config.CHECKPOINT_DIR/'ner'),
                       logging_dir=str(config.LOG_DIR / 'ner' / time.strftime("%Y-%m-%d-%H-%M-%S")),
                       num_train_epochs=30,
                       per_device_train_batch_size=16,
                       per_device_eval_batch_size=16,
                       save_strategy='steps',
                       eval_strategy='steps',
                       logging_strategy='steps',
                       logging_steps=20,
                       eval_steps=50,
                       save_steps=50,
                       warmup_steps=100,
                       save_total_limit=4,
                       bf16=True,
                       greater_is_better=True,
                       load_best_model_at_end=True,
                       metric_for_best_model='eval_overall_f1'
                       )
trainer=Trainer(model=model,
                data_collator=data_collator,
                args=args,
                train_dataset=train_dataset,
                eval_dataset =val_dataset,
                optimizers=(optimizer, scheduler),
                compute_metrics=compute_metrics,
                callbacks=[early_stopping_callback]
                )
trainer.train()
trainer.save_model(config.CHECKPOINT_DIR / 'ner' / 'best_model')
