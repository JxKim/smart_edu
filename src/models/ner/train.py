import dotenv
dotenv.load_dotenv()

import time

import evaluate
from datasets import load_from_disk
from transformers import AutoModelForTokenClassification, AutoTokenizer, DataCollatorForTokenClassification, \
    TrainingArguments, EvalPrediction, EarlyStoppingCallback, Trainer

from configuration import config

# 标签映射
id2label = {id: label for id, label in enumerate(config.LABELS)}
label2id = {label: id for id, label in enumerate(config.LABELS)}

# 模型
model = AutoModelForTokenClassification.from_pretrained(config.PRE_MODEL_NAME,
                                                        num_labels=len(config.LABELS),
                                                        id2label=id2label,
                                                        label2id=label2id)
# 数据
train_dataset = load_from_disk(config.DATA_DIR / 'ner' / 'processed' / 'train')
val_dataset = load_from_disk(config.DATA_DIR / 'ner' / 'processed' / 'val')

# 分词器
tokenizer = AutoTokenizer.from_pretrained(config.PRE_MODEL_NAME)

# 数据整理器
data_collator = DataCollatorForTokenClassification(tokenizer, padding=True, return_tensors='pt')

# 训练参数
args = TrainingArguments(
    output_dir=str(config.CHECKPOINT_DIR / 'ner'),  # 模型输出路径
    logging_dir=str(config.LOGS_DIR / 'ner' / time.strftime('%Y-%m-%d-%H-%M-%S')),  # 日志保存路径
    num_train_epochs=20,
    per_device_train_batch_size=16,
    save_strategy='steps',  # 保存策略
    save_steps=50,  # 保存模型间隔
    save_total_limit=2,  # 保存模型数量
    fp16=True,  # 混合精度训练
    logging_strategy='steps',
    logging_steps=50,
    eval_strategy='steps',  # 评估策略
    eval_steps=50,  # 评估间隔
    load_best_model_at_end=True,  # 加载最佳模型
    metric_for_best_model='eval_overall_f1',
    greater_is_better=True
)

# 指标计算函数
##seqval是一个专门为序列标注任务设计的评估框架，主要用于评估命名实体识别(NER)、词性标注等序列标注模型的性能。
seqeval = evaluate.load('seqeval')


def compute_metrics(prediction: EvalPrediction):
    logits = prediction.predictions
    preds = logits.argmax(axis=-1)  # [barch,seq_len,vocab]-->[batch,seq_len]
    labels = prediction.label_ids

    unpad_preds = []
    unpad_labels = []
    for pred, label in zip(preds, labels):
        # 去掉填充的pad
        unpad_label = label[label != -100]
        unpad_pred = pred[label != -100]
        # 转标签
        unpad_pred = [id2label[id] for id in unpad_pred]
        unpad_label = [id2label[id] for id in unpad_label]
        unpad_preds.append(unpad_pred)
        unpad_labels.append(unpad_label)
    return seqeval.compute(predictions=unpad_preds, references=unpad_labels)


# 早停回调
early_stopping = EarlyStoppingCallback(early_stopping_patience=5)

# 训练器
trainer = Trainer(model=model,
                  args=args,
                  data_collator=data_collator,
                  train_dataset=train_dataset,
                  eval_dataset=val_dataset,
                  compute_metrics=compute_metrics,
                  callbacks=[early_stopping])
trainer.train()
trainer.save_model(str(config.CHECKPOINT_DIR / 'ner' / 'best_model'))
