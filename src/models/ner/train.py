import time

import dotenv
import evaluate

dotenv.load_dotenv()
from datasets import load_from_disk
from transformers import AutoTokenizer, DataCollatorForTokenClassification, EvalPrediction, TrainingArguments, Trainer, \
    AutoModelForTokenClassification

from configuration import config


def train():
    train_dataset = load_from_disk(str(config.DATA_DIR / 'ner' / 'processed' / 'train'))
    valid_dataset = load_from_disk(str(config.DATA_DIR / 'ner' / 'processed' / 'valid'))

    id2label = {i: label for i, label in enumerate(config.LABELS)}
    label2id = {label: i for i, label in enumerate(config.LABELS)}

    model = AutoModelForTokenClassification.from_pretrained(config.MODEL_NAME,
                                          num_labels=len(config.LABELS),
                                          id2label=id2label,
                                          label2id=label2id)
    tokenizer = AutoTokenizer.from_pretrained(config.MODEL_NAME)

    # 修改数据整理器，添加padding策略
    data_collator = DataCollatorForTokenClassification(
        tokenizer=tokenizer, 
        padding=True, 
        return_tensors='pt',
        label_pad_token_id=-100  # 明确指定标签的padding值
    )
    seqeval = evaluate.load('seqeval')

    def compute_metrics(prediction: EvalPrediction):
        logits = prediction.predictions
        predictions = logits.argmax(axis=-1)
        labels = prediction.label_ids

        unpad_predictions, unpad_labels = [], []
        for pred, label in zip(predictions, labels):
            pred = pred[label != -100]
            label = label[label != -100]

            pred = [id2label[i] for i in pred]
            label = [id2label[i] for i in label]

            unpad_predictions.append(pred)
            unpad_labels.append(label)
        return seqeval.compute(predictions=unpad_predictions, references=unpad_labels)

    args = TrainingArguments(
        output_dir=str(config.CHECK_POINT / 'ner'),
        logging_dir=str(config.LOG_DIR / 'ner' / time.strftime("%Y-%m-%d_%H-%M-%S")),
        logging_strategy="steps",
        logging_steps=20,
        per_device_train_batch_size=2,
        num_train_epochs=10,
        save_strategy="steps",
        save_steps=20,
        save_total_limit=3,
        eval_strategy="steps",
        eval_steps=20,
        fp16=True,
        load_best_model_at_end=True,
        metric_for_best_model="eval_overall_f1",
        greater_is_better=True
    )

    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=train_dataset,
        eval_dataset=valid_dataset,
        data_collator=data_collator,
        compute_metrics=compute_metrics
    )
    trainer.train()
    trainer.save_model(str(config.CHECK_POINT / 'ner' / 'best_model'))


if __name__ == '__main__':
    train()
