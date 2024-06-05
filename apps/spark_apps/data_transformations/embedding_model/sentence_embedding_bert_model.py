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
        self.model_name = "bert-base-uncased"  # Replace with desired model name
        self.tokenizer = BertTokenizer.from_pretrained(self.model_name)
        self.model = BertModel.from_pretrained(self.model_name)

    def preprocess(self, row: Row) -> Dict[str, Any]:
        row: Dict[str, Any] = row.asDict()
        question = row.get("question")
        answer = row.get("answer")
        options = row.get("options")
        sentece = [
            question,
            f"The answer is: {answer}",
            f"The given options were: {options}",
        ]

        # preprocessed_sentence = (
        #     "[CLS] "
        #     + question
        #     + " [SEP] The answer is: "
        #     + answer
        #     + " [SEP] The given options were: "
        #     + options
        # )

        # sentece = [
        #     question,
        #     f"The answer is: {answer}",
        #     f"The given options were: {options}",
        # ]

        encoding = self.tokenizer.batch_encode_plus(
            sentece,  # List of input texts
            padding=True,  # Pad to the maximum sequence length
            truncation=True,  # Truncate to the maximum sequence length if necessary
            return_tensors="pt",  # Return PyTorch tensors
            add_special_tokens=True,  # Add special tokens CLS and SEP
        )

        # input_ids = encoding["input_ids"]  # Token IDs
        # # print input IDs
        # print(f"Input ID: {input_ids}")
        # attention_mask = encoding["attention_mask"]  # Attention mask
        # # print attention mask
        # print(f"Attention mask: {attention_mask}")
        return encoding

    def embed(self, encoding: Dict[str, Any]):
        with torch.no_grad():
            outputs = self.model(
                encoding["input_ids"], attention_mask=encoding["attention_mask"]
            )
        word_embeddings = outputs.last_hidden_state  # This contains the embeddings

        # Output the shape of word embeddings
        sentence_embedding = word_embeddings.mean(dim=1)
        print(f"Sentence Embeddings: {sentence_embedding}")
        print(f"Shape of Sentence Embeddings: {sentence_embedding.shape}")
        print(f"Type Sentence Embeddings: {type(sentence_embedding)}")
        print(f"Type Sentence Embeddings to numpy: {sentence_embedding.tolist()}")

        return sentence_embedding.tolist()

    def bert_embedding_sentece(self, row: Row) -> List[float]:
        encoding = self.preprocess(row)
        sentence_embedding = self.embed(encoding)
        return sentence_embedding

    # def preprocess1(self, row: Row):
    #     # ... (BERT specific preprocessing)
    #     """
    #     Preprocesses a dictionary containing medical question and answer for BERT embedding.

    #     Args:
    #         text_dict: A dictionary with keys "question", "answer", etc.

    #     Returns:
    #         A string representing the preprocessed text for BERT.
    #     """
    #     # Extract relevant information
    #     row: Dict[str, Any] = row.asDict()
    #     question = row.get("question")
    #     answer = row.get("answer")
    #     options = row.get("options")

    #     # Concatenate question and answer with a delimiter
    #     preprocessed_sentence = (
    #         "[CLS] "
    #         + question
    #         + " [SEP] The answer is: "
    #         + answer
    #         + " [SEP] The given options were: "
    #         + options
    #     )

    #     # Add special tokens (Optional: Lowercasing based on tokenizer)
    #     encoded_text = self.tokenizer(
    #         preprocessed_sentence, add_special_tokens=True, return_tensors="pt"
    #     )

    #     input_ids = encoded_text["input_ids"].squeeze(0)  # Remove batch dimension
    #     attention_mask = encoded_text["attention_mask"].squeeze(0)

    #     return input_ids, attention_mask

    # def embed1(self, input_ids, attention_mask):
    #     # ... (BERT specific embedding logic)
    #     with torch.no_grad():  # Disable gradient calculation for efficiency
    #         outputs = self.model(input_ids, attention_mask=attention_mask)

    #     # Extract the CLS token embedding (First token)
    #     sentence_embedding = outputs[0][:, 0, :]  # CLS token embedding

    #     # Print the sentence embedding (768 dimensions for BERT-base-uncased)
    #     print(sentence_embedding.shape)  # Output: torch.Size([768])
    #     print(sentence_embedding)
