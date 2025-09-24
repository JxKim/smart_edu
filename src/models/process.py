from datasets import load_dataset
from transformers import  AutoTokenizer
from configurations import config


class Process:
    def __init__(self):
        # 分词器
        self.tokenizer=AutoTokenizer.from_pretrained("google-bert/bert-base-chinese")
    def process_data(self):
        # 读取原始数据
        origin_dataset=load_dataset(str(config.LABEL_STUDIO_DATA))["train"]
        origin_dataset=origin_dataset.remove_columns(["id","annotator","annotation_id","created_at","updated_at","lead_time"])

        # 创建BIO列表
        bio_labels=["B","I","O"]

        # 映射函数
        def map_fn(batch):
            data_list = [list(example) for example in batch["text"]]
            inputs = self.tokenizer(text=data_list, truncation=True, is_split_into_words=True,padding=True)
            for index, (input_ids, label) in enumerate(zip(inputs["input_ids"], batch["label"])):
                # 拿到起始和结束的列表（因为一个label包含多个其实和结束）
                start_list = []
                end_list = []

                # 把每个样本的起始和结束加入列表
                for lab in label:
                    start = lab["start"]
                    end = lab["end"]
                    start_list.append(start)
                    end_list.append(end)

                # 给原始的列表赋值,直接变为长度为原数据长度的列表，用o填充
                batch["label"][index] = [bio_labels.index("O")] * len(input_ids)
                for start, end in zip(start_list, end_list):
                    batch["label"][index][start] = bio_labels.index("B")
                    batch["label"][index][start + 1:end] = [bio_labels.index("I")] * (end - start - 1)
                # 左右两边加-100
                batch["label"][index][0] = -100
                batch["label"][index][-1] = -100
            # 把新字段加入到字典中,统一返回
            inputs["labels"] = batch["label"]
            return inputs
        # 对原始的数据做转化,准备给到模型
        processed_data=origin_dataset.map(function=map_fn,batched=True,remove_columns=["label","text"])

        # 划分数据集
        final_dataset_dict = processed_data.train_test_split(test_size=0.2)
        final_dataset_dict["test"], final_dataset_dict["valid"] = final_dataset_dict["test"].train_test_split(
            test_size=0.5).values()
        final_dataset_dict.save_to_disk(str(config.PROCESSED_PATH))
if __name__ == "__main__":
    process=Process()
    process.process_data()