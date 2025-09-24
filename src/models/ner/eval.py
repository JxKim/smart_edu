import time

import evaluate
from datasets import load_from_disk
from transformers import AutoTokenizer, BertForTokenClassification, TrainingArguments, \
    DataCollatorForTokenClassification, EarlyStoppingCallback, AutoModel, AutoModelForTokenClassification
from transformers import Trainer

from src.configuration import config

if __name__ == '__main__':
    model=AutoModelForTokenClassification.from_pretrained(config.CHECKPOINT_DIR/'ner'/'best_model')
    tokenizer = AutoTokenizer.from_pretrained(config.CHECKPOINT_DIR/'ner'/'best_model')
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
    test_dataset=load_from_disk(config.DATA_DIR/'ner'/'processed'/'test')

    trainer = Trainer(model=model,data_collator=data_collator,
                     eval_dataset=test_dataset,
                    compute_metrics=compute_metrics)
    print(trainer.evaluate())