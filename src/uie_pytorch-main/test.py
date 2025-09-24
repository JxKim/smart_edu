
from uie_predictor import UIEPredictor
from pprint import pprint
schema = ['TAG'] # Define the schema for entity extraction
ie = UIEPredictor(model='uie-base', task_path='./checkpoint/course/model_best', schema=schema)
# pprint(ie("2月8日上午北京冬奥会自由式滑雪女子大跳台决赛中中国选手谷爱凌以188.25分获得金牌！"))
pprint(ie(["本阶段课程深入剖析 LLaMA 和 Qwen 系列模型核心原理，全面阐述大模型微调的各个方面，从核心要素到具体技术，再到数据收集评估、参数设置及代码详解，为学生构建扎实的大模型知识体系。 同时，涵盖 NLP 常规任务方案设计与训练环境搭建，提升学生的实践能力。引入多模态技术，详解多种多模态模型核心原理。 通过电商客服和多模态电商风控项目，将理论与实践紧密结合，使学生在掌握前沿技术的同时，能够应对实际应用场景，提升在人工智能领域的综合竞争力。","本阶段课程深入剖析 LLaMA 和 Qwen 系列模型核心原理，全面阐述大模型微调的各个方面，从核心要素到具体技术，再到数据收集评估、参数设置及代码详解，为学生构建扎实的大模型知识体系。 同时，涵盖 NLP 常规任务方案设计与训练环境搭建，提升学生的实践能力。引入多模态技术，详解多种多模态模型核心原理。 通过电商客服和多模态电商风控项目，将理论与实践紧密结合，使学生在掌握前沿技术的同时，能够应对实际应用场景，提升在人工智能领域的综合竞争力。"]))
# python finetune.py --train_path "./data/train.txt" --dev_path "./data/dev.txt" --save_dir "./checkpoint/course" --learning_rate 1e-5 --batch_size 4 --max_seq_len 512 --num_epochs 100 --model "uie_base_pytorch" --seed 1000 --logging_steps 10 --valid_steps 100 --device "gpu"