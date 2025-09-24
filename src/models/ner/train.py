import time

import evaluate
from datasets import load_from_disk
from transformers import (AutoTokenizer, BertForTokenClassification, TrainingArguments,
                          DataCollatorForTokenClassification, EarlyStoppingCallback, AutoModel, AutoModelForTokenClassification)
from transformers import Trainer

from src.configuration import config

if __name__ == '__main__':
    id2label={idx:label for idx,label in enumerate(config.LABELS)}
    label2id={label:idx for idx,label in enumerate(config.LABELS)}
    model=AutoModelForTokenClassification.from_pretrained('google-bert/bert-base-chinese',num_labels=len(config.LABELS),
                                                     id2label=id2label,label2id=label2id)
    tokenizer = AutoTokenizer.from_pretrained('google-bert/bert-base-chinese')
    def compute_metrics(EvalPrediction):
        final_predictions=[]
        final_references = []
        predictions = EvalPrediction.predictions.argmax(-1)
        references =EvalPrediction.label_ids
        for prediction, reference in zip(predictions, references):
            mask=reference != -100
            prediction=prediction[mask].tolist()
            reference=reference[mask].tolist()
            final_predictions.append(prediction)
            final_references.append(reference)
        final_predictions=[[config.LABELS[idx] for idx in prediction] for prediction in final_predictions]
        final_references=[[config.LABELS[idx] for idx in reference] for reference in final_references]
        seqeval = evaluate.load("seqeval")
        return seqeval.compute(predictions=final_predictions, references=final_references)

    data_collator=DataCollatorForTokenClassification(tokenizer=tokenizer,padding=True,return_tensors="pt")
    train_dataset=load_from_disk(config.DATA_DIR/'ner'/'processed'/'train')
    eval_dataset=load_from_disk(config.DATA_DIR/'ner'/'processed'/'valid')
    early_stop=EarlyStoppingCallback(early_stopping_patience=5)

    args=TrainingArguments(output_dir=config.CHECKPOINT_DIR/'ner',per_device_train_batch_size=2,per_device_eval_batch_size=2,
                           save_strategy='steps',save_steps=20,save_total_limit=3,load_best_model_at_end=True,
                           eval_steps=20,eval_strategy='steps',metric_for_best_model='eval_overall_f1',greater_is_better=True,
                           logging_steps=20,logging_strategy='steps',logging_dir=config.LOGS_DIR/'ner'/time.strftime("%Y/%m/%d-%H-%M-%S"),
                           num_train_epochs=5,bf16=True)

    trainer = Trainer(model=model,args=args,data_collator=data_collator,
                      train_dataset=train_dataset,eval_dataset=eval_dataset,
                      compute_metrics=compute_metrics,callbacks=[early_stop])
    # trainer.train(resume_from_checkpoint=True)
    trainer.train()
    trainer.save_model(config.CHECKPOINT_DIR/'ner'/'best_model')