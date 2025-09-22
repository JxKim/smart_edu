from dotenv import load_dotenv
load_dotenv()
import time

import evaluate
from datasets import load_from_disk
from transformers import AutoModelForTokenClassification, AutoTokenizer, DataCollatorForTokenClassification, Trainer, \
    TrainingArguments, EvalPrediction

from configuration import config

# 标签映射
id2label = {i: label for i, label in enumerate(config.LABELS)}
label2id = {label: i for i, label in enumerate(config.LABELS)}

# 模型
model = AutoModelForTokenClassification.from_pretrained("google-bert/bert-base-chinese",
                                                        num_labels=len(config.LABELS),
                                                        id2label=id2label,
                                                        label2id=label2id)
# 数据
train_dataset = load_from_disk(str(config.DATA_DIR_PROCESSED / 'chapter'/ 'train'))
valid_dataset = load_from_disk(str(config.DATA_DIR_PROCESSED / 'chapter'/ 'valid'))

# 数据pad处理
tokenizer = AutoTokenizer.from_pretrained("google-bert/bert-base-chinese")
data_loader = DataCollatorForTokenClassification(tokenizer=tokenizer,padding=True,return_tensors="pt")

# 训练参数
args = TrainingArguments(
    output_dir=str(config.CHECKPOINTS_DIR / 'chapter'),
    logging_dir=str(config.LOGS_DIR / 'chapter' / time.strftime("%Y-%m-%d-%H-%M-%S")),
    num_train_epochs=20,
    per_device_train_batch_size=2,
    save_strategy='steps',
    save_steps=20,
    save_total_limit=1,
    fp16=True,
    logging_strategy='steps',
    logging_steps=20,
    eval_strategy='steps',
    eval_steps=20,
    load_best_model_at_end=True,
    metric_for_best_model='eval_overall_f1',
    greater_is_better=True
)

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
        unpad_pred = [id2label[id] for id in unpad_pred]
        unpad_label = [id2label[id] for id in unpad_label]
        unpad_labels.append(unpad_label)
        unpad_preds.append(unpad_pred)
    return seqeval.compute(predictions=unpad_preds, references=unpad_labels)


trainer = Trainer(model=model,
                  args=args,
                  train_dataset=train_dataset,
                  eval_dataset=valid_dataset,
                  data_collator=data_loader,
                  compute_metrics=compute_metrics)
trainer.train()
trainer.save_model(config.CHECKPOINTS_DIR / 'chapter' / 'best_model')
