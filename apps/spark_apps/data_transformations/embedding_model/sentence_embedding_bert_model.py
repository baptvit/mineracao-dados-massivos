import torch
import random
from typing import Any, Dict, List
from transformers import BertTokenizer, BertModel
from spark_apps.data_transformations.embedding_model.sentence_embedding_model import (
    SentenceEmbeddingModel,
)


class BertSentenceEmbedding(SentenceEmbeddingModel):
    """Concrete class for BERT sentence embedding."""

    def __init__(self) -> None:
        super().__init__()

        # Set a random seed
        random_seed = 42
        random.seed(random_seed)

        # Set a random seed for PyTorch (for GPU as well)
        torch.manual_seed(random_seed)

        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(random_seed)

        # (Implementation details for BERT preprocessing and embedding logic)
        self.model_name = "bert-base-uncased"  # Replace with desired model name
        self.tokenizer = BertTokenizer.from_pretrained(self.model_name)
        self.model = BertModel.from_pretrained(self.model_name)

    def pre_process_sentece(self, sentece: str) -> Dict[str, Any]:
        return self.tokenizer(
            sentece,  # List of input texts
            padding=True,  # Pad to the maximum sequence length
            truncation=True,  # Truncate to the maximum sequence length if necessary
            return_tensors="pt",  # Return PyTorch tensors
            add_special_tokens=True,  # Add special tokens CLS and SEP
            is_split_into_words=True,
        )

    def bert_sentence_embedding(self, setence: str) -> List[float]:
        encoding = self.pre_process_sentece(setence)

        input_ids = encoding["input_ids"]

        attention_mask = encoding["attention_mask"]

        with torch.no_grad():
            outputs = self.model(input_ids, attention_mask=attention_mask)

        # Choose the desired embedding strategy
        # Option 1: Last hidden layer output for all tokens
        # sentence_embedding = outputs[0][:, 0]  # CLS token embedding

        # Option 2: Pooled output (average)
        sentence_embedding = torch.mean(outputs[0], dim=1)

        # Option 3: Pooled output (concatenate)
        # sentence_embedding = torch.cat((outputs[0][:, 0], torch.mean(outputs[0], dim=1)), dim=1)

        # print(f"Sentence Embeddings: {sentence_embedding}")
        print(f"Shape of Sentence Embeddings: {sentence_embedding.shape}")
        return sentence_embedding.tolist()

    def get_sentence_embedding(self, sentence: str) -> List[float]:
        return self.bert_sentence_embedding(sentence)
