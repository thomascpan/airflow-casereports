# Modified from original source
# https://github.com/qlyu001/nlp_label_prediction/blob/master/nlp_helper/flair_label.py

from flair.data import Sentence
from flair.models import SequenceTagger
import nltk
import os
import gdown
import collections.abc
import logging
from typing import Generator


class FlairLabeller:

    def __init__(self, model_path: str, url: str = 'https://drive.google.com/uc?id=1kQExz-YngdZU8OhRnOhEstlEP2vbGvQZ', tokenizer=None):
        self.model_path = model_path
        self.url = url
        nltk.download('punkt')
        self.__download_model(url, model_path)
        self.model = self.__load_model(model_path)
        self.tokenizer = tokenizer or nltk.data.load(
            'tokenizers/punkt/english.pickle')

    def __download_model(self, url: str, output_path: str) -> None:
        logging.info("***__download_model***")
        if not os.path.exists(output_path):
            gdown.download(url, output_path)

    def __load_model(self, path: str) -> SequenceTagger:
        logging.info("***__load_model***")
        return SequenceTagger.load_from_file(path)

    def __tokenize(self, s: str):
        logging.info("***__tokenize***")
        s = s.replace('-', ' - ')       # deal with special case 17-year-old
        return ' '.join(nltk.word_tokenize(s))

    def __location_index(self, sentences):
        logging.info("***__location_index***")
        start = 0
        end = 0
        location = []
        for sentence in sentences:
            for words in sentence:
                for word in words:
                    end = start + len(word)
                    location.append([start, end])
                    start = end + 1
        return location

    def __sentence_label(self, sentence) -> (list, list):
        logging.info("***__sentence_label***")
        sentence = [q for q in sentence.split('\\n')]
        all_tokens = []
        all_entities = []

        for q in sentence:
            sen = Sentence(self.__tokenize(q))
            text = q.split(' ')
            self.model.predict(sen)
            tokens = []
            entity_types = []
            prev = ''
            for t in sen.tokens:
                if prev == '-' or t.text == '-':
                    tokens[-1] = tokens[-1] + t.text
                    prev = t.text
                    # print(tokens[-1])
                    continue
                token = t.text
                entity = t.tags['ner'].value
                tokens.append(token)
                if(len(entity) < 2):
                    entity_types.append(entity)
                else:
                    entity_types.append(entity[2:])

            all_tokens.append(tokens)
            all_entities.append(entity_types)
        return all_tokens, all_entities

    def flatten(self, l: list) -> Generator:
        logging.info("***flatten***")
        """Flattens list

        Args:
            l (list): list to be flattened
        Returns:
            Generator: Returns generator object. Can be type casted to list.
        """
        for el in l:
            if isinstance(el, collections.abc.Iterable) and not isinstance(el, (str, bytes)):
                yield from self.flatten(el)
            else:
                yield el

    def paragraph_label(self, paragraph: str) -> (list, list, list):
        logging.info("***paragraph_label***")
        if not paragraph:
            return [], [], []
        tokenized_paragraph = self.tokenizer.tokenize(paragraph)
        sentences = []
        tokens = []
        for sentence in tokenized_paragraph:
            all_tokens, all_entities = self.__sentence_label(sentence)
            sentences.append(all_tokens)
            tokens.append(all_entities)

        location = self.__location_index(sentences)

        return list(self.flatten(sentences)), list(self.flatten(tokens)), location
