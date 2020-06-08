# Modified from original source
# https://github.com/qlyu001/nlp_label_prediction/blob/master/nlp_helper/labeling.py

import zipfile
import nltk
import numpy as np
from transformers import RobertaTokenizer, RobertaForTokenClassification
import torch
import pickle
import os
import gdown
import collections.abc

root_dir = '/usr/local/airflow'


class BertLabeller:

    def __init__(self, model_dir: str = None, url: str = 'https://drive.google.com/uc?id=16Neexi-QyX-WettcN6Oc6mNNuK4NlsIr'):
        self.dim = 512
        self.model_dir = model_dir or os.path.join(root_dir, 'model_save')
        self.tag2idx_path = os.path.join(self.model_dir, 'tag2idx.pckl')
        self.download_file = 'roberta.zip'
        self.url = url
        self.device = torch.device("cpu")
        self.__download_model(self.url, self.model_dir)
        self.idx2tag = self.__load_idx2tag()
        self.model = self.__load_model()
        nltk.download('punkt')
        self.paragraph_tokenizer = nltk.data.load(
            'tokenizers/punkt/english.pickle')
        self.tokenizer = self.__load_tokenizer()

    def __validate_input_dim(self, input_tokens) -> bool:
        return len(input_tokens) <= self.dim

    def __download_model(self, url: str, output_path: str) -> None:
        if not os.path.exists(output_path):
            gdown.download(url, self.download_file)
            with zipfile.ZipFile(self.download_file, 'r') as zip_ref:
                zip_ref.extractall(root_dir)
        if os.path.exists(self.download_file):
            os.remove(self.download_file)

    def __load_idx2tag(self, path: str = None):
        f = open(path or self.tag2idx_path, 'rb')
        tag2idx = pickle.load(f)
        return dict((v, k) for k, v in tag2idx.items())

    def __load_model(self, model_dir: str = None):
        model = RobertaForTokenClassification.from_pretrained(
            model_dir or self.model_dir)
        model.aux_logits = False
        model.to(self.device)
        return model

    def __load_tokenizer(self, model_dir: str = None):
        return RobertaTokenizer.from_pretrained(self.model_dir)

    def __location_index(self, sentences):
        start = 0
        end = 0
        location = []
        for word in sentences:
            end = start + len(word)
            location.append([start, end])
            start = end + 1
        return location

    def __sentence_label(self, sentence) -> (list, list):
        all_tokens = []
        origin_tokens = sentence.split(' ')
        all_entities = []
        entity_types = []
        tokenized_sentence = self.tokenizer.encode(sentence)
        if not self.__validate_input_dim(tokenized_sentence):
            return None, None

        input_ids = torch.tensor([tokenized_sentence]).to(self.device)

        predictions = []
        with torch.no_grad():
            output = self.model(input_ids)
            output = output[0].detach().cpu().numpy()
            predictions.extend([list(p) for p in np.argmax(output, axis=2)])

        tags_predictions = []
        for x in predictions[0]:
            tags_predictions.append(self.idx2tag[int(x)])

        tokens = []
        count = 0

        # get tokens from ids
        for x in self.tokenizer.convert_ids_to_tokens(tokenized_sentence):
            if count == 1:
                tokens.append(x)
            elif x[0] == 'Ġ':
                tokens.append(x[1:])
            else:
                tokens.append(x)
            count += 1

        wordIndex = 0
        startIndex = 0
        entityIndex = 0
        entity_types.append(tags_predictions[1:-1])

        for x in tokens[1:-1]:
            entity = entity_types[0][entityIndex]
            entityIndex += 1
            if wordIndex == len(origin_tokens):
                break
            if x in origin_tokens[wordIndex].lower():
                if startIndex == 0:
                    all_tokens.append(origin_tokens[wordIndex])
                    if(len(entity) < 2):
                        all_entities.append(entity)
                    else:
                        all_entities.append(entity[2:])
                startIndex = startIndex + len(x)
                if startIndex >= len(origin_tokens[wordIndex]):
                    wordIndex += 1
                    startIndex = 0

        return all_tokens, all_entities

    def paragraph_label(self, paragraph: str) -> (list, list):
        if not paragraph:
            return [], [], []
        tokenized_paragraph = self.paragraph_tokenizer.tokenize(paragraph)
        sentences = []
        tokens = []
        for sentence in tokenized_paragraph:
            all_tokens, all_entities = self.__sentence_label(sentence)
            if not (all_tokens and all_entities):
                return None, None
            sentences += all_tokens
            tokens += all_entities

        location = self.__location_index(sentences)

        return tokens, location
