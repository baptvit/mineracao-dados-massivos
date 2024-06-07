import torch
import random
from typing import Any, Dict, List
from pyspark.sql import Row
from transformers import BertTokenizer, BertModel


class BertSentenceEmbedding:
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
        self.model_name = "bert-large-uncased"  # Replace with desired model name
        self.tokenizer = BertTokenizer.from_pretrained(self.model_name)
        self.model = BertModel.from_pretrained(self.model_name)

    def preprocess(self, row) -> Dict[str, Any]:
        # row: Dict[str, Any] = row.asDict()
        question = row.get("question")
        answer = row.get("answer")
        options = row.get("options")
        breakpoint()
        sentece = [
            question,
            f"The answer is: {answer}",
            f"The given options were: {options}",
        ]
        encoding = self.tokenizer.batch_encode_plus(
            sentece,  # List of input texts
            padding=True,  # Pad to the maximum sequence length
            truncation=True,  # Truncate to the maximum sequence length if necessary
            return_tensors="pt",  # Return PyTorch tensors
            add_special_tokens=True,  # Add special tokens CLS and SEP
        )

        input_ids = encoding["input_ids"]  # Token IDs
        # print input IDs
        print(f"Input ID: {input_ids}")
        attention_mask = encoding["attention_mask"]  # Attention mask
        # print attention mask
        print(f"Attention mask: {attention_mask}")
        return encoding

    def embed(self, encoding: Dict[str, Any]) -> List[float]:
        with torch.no_grad():
            outputs = self.model(
                encoding["input_ids"], attention_mask=encoding["attention_mask"]
            )
        word_embeddings = outputs.last_hidden_state  # This contains the embeddings

        # Output the shape of word embeddings
        print(f"Shape of Word Embeddings: {word_embeddings.shape}")
        sentence_embedding = word_embeddings.mean(dim=1)
        return sentence_embedding

    def bert_embedding_sentece(self, dict) -> List[float]:
        encoding = self.preprocess(dict)
        sentence_embedding = self.embed(encoding)
        return sentence_embedding


# def test_bard_embedding() -> None:

text = {
    "question": "Hello worl is?",
    "answer": "very cool.",
    "options": "very cool, very sad, very sorry or not cool.",
}

model = BertSentenceEmbedding()
embedding = model.bert_embedding_sentece(text)
breakpoint()
