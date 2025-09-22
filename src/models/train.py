import time
from os import truncate

from datasets import load_from_disk
from transformers import (Trainer, AutoModelForTokenClassification, AutoTokenizer, EarlyStoppingCallback,
                          DataCollatorForTokenClassification, TrainingArguments, EvalPrediction)
from configurations import config
import evaluate

# 模型
id2label = {i: label for i, label in enumerate(config.BIO_LABELS)}
label2id = {label: i for i, label in enumerate(config.BIO_LABELS)}

# 模型
model = AutoModelForTokenClassification.from_pretrained("google-bert/bert-base-chinese",
                                                        num_labels=len(config.BIO_LABELS),
                                                        id2label=id2label,
                                                        label2id=label2id)
# 训练数据
train_dataset = load_from_disk(config.PROCESSED_PATH / "train")
# 测试数据
eval_dataset = load_from_disk(config.PROCESSED_PATH / "valid")
# 分词器
processing_class = AutoTokenizer.from_pretrained("google-bert/bert-base-chinese")

# 处理数据填充函数
collator_fn = DataCollatorForTokenClassification(tokenizer=processing_class, padding=True, return_tensors="pt")


# 这个在评估的时候，是以实体为单位的（以Ner为例）;也就是说实体的开始和结束位置,实体识别的连续性，实体的类型，必须完全一致，才会算是预测正确
seqeval = evaluate.load('seqeval')
early_stopping_callback = EarlyStoppingCallback(early_stopping_patience=20)

# 如果使用huggingface提供的Trainer,他一定会传入一个EvalPrediction的对象,你可以用任意的变量接受，包含的字段，可以在官网，或者点进去就能看到
args = TrainingArguments(
    # 保存检查点的路径
    output_dir=str(config.CHECKPOINT_PATH),
    # 只保留最近的三个检查点
    save_total_limit=3,
    # 训练多少个轮次
    num_train_epochs=5,
    # 单设备的时候,就是batch_size
    per_device_train_batch_size=2,

    # 日志文件的路径
    logging_dir=str(config.LOG_PATH / time.strftime("%Y%m%d-%H%M%S")),
    # 下面两个参数结合着看，每处理20个batch,记录一下日志；如果不给，默认值是500
    logging_strategy="steps",
    logging_steps=20,

    # 下面三个参数结合着看，每计算20个batch,保存一下模型（也可以说是保存一下检查点）
    # 目的是为了断点续训,会保存一些优化器的状态,模型参数等信息
    # 第三个参数,加载最佳模型,训练结束之后，自动加载训练过程中的最佳模型
    save_strategy='steps',
    save_steps=20,
    load_best_model_at_end=True,

    # 下面四个参数结合着看,每训练20个batch就做一次评估，目的是为了防止过拟合
    # 第三个参数,早停的判断标准，是验证集的f1分数
    # 第四个参数,如何定义指标变化（等于True的意思是，对于f1分数来说，越大越好）
    eval_strategy='steps',
    eval_steps=20,
    metric_for_best_model='eval_overall_f1',
    greater_is_better=True,

    # 学习率衰减的模式,线性衰减 初期的时候很大,为了让参数快速更新，后期的时候相对小,防止收敛过程过度震荡
    lr_scheduler_type="linear",
    # 混合精度训练
    fp16=True,
)
def compute_metrics(prediction: EvalPrediction):
    # 这里可以用模型来推测，对与没带任务头的模型来说
    # 这里就应该是batch_size,seq_len,num_layers（分类任务的预测，就是走了一遍前向传播）
    logits = prediction.predictions
    # batch_size seq_len
    preds = logits.argmax(axis=-1)
    # 真实标签,batch_size,seq_len
    # 每一个label是这样的形式[-100,1,3,2,-100,-100]
    # -100是保证cls，sep，填充的-100 不参与损失计算
    labels = prediction.label_ids
    unpad_labels = []
    unpad_preds = []

    for pred, label in zip(preds, labels):
        # 去掉填充 以及前面cls对应的位置
        unpad_label = label[label != -100]
        # 去掉预测值前面的cls和-100 这里用的是同样的布尔索引，所以去掉的是同样的位置
        unpad_pred = pred[label != -100]
        # 转标签
        unpad_pred = [id2label[id] for id in unpad_pred]
        unpad_label = [id2label[id] for id in unpad_label]
        unpad_labels.append(unpad_label)
        unpad_preds.append(unpad_pred)

    return seqeval.compute(predictions=unpad_preds, references=unpad_labels)

trainer = Trainer(
                  # 模型
                  model=model,
                  # 训练集
                  train_dataset=train_dataset,
                  # 处理用户的输入（在nlp中就是分词器）;同时也可以保证预测时候是一致的
                  processing_class=processing_class,
                  # 数据处理函数,也就是构建dataloader中的collator_fn，用于将一批样本,填充到相同的长度
                  data_collator=collator_fn,
                  # 训练时候需要用到的参数
                  args = args,
                  # 回调函数，目前只用到了早停
                  callbacks=[early_stopping_callback],
                  # 验证集，用于早停
                  eval_dataset=eval_dataset,
                  # 1) 计算中间结果
                  # 2) 计算最终结果
                  # 3) 在这个案例中,早停配合，计算验证集的指标，判断是否早停（可能是f1，准确率等）
                  compute_metrics=compute_metrics)

trainer.train()
# 保存最佳模型，以为设置了保存最佳模型的一系列参数，所以这里直接保存模型，就是模型在训练中的最佳参数
trainer.save_model(config.CHECKPOINT_PATH / 'best_model')

