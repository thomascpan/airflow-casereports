
import numpy as np
from transformers import BertTokenizer, BertConfig, RobertaTokenizer
from transformers import BertForTokenClassification, RobertaForTokenClassification
import torch
import pickle
import codecs

def locationIndex(sentences):
    start = 0
    end = 0
    location = []
    for sentence in sentences:
        for words in sentence:
            for word in words:
                end = start + len(word)
                print(word)
                location.append([start,end])
                start = end + 1
    return location

def sentenceLabel(sentence):
    f = open('./model_save/tag2idx.pckl', 'rb')
    tag2idx = pickle.load(f)
    device = torch.device("cpu")
    output_dir = './model_save/'
    idx2tag = dict((v,k) for k, v in tag2idx.items())
    tokenizer = RobertaTokenizer.from_pretrained(output_dir)
    model = RobertaForTokenClassification.from_pretrained(output_dir)
    model.to(device)

    # predict
    all_tokens = []
    all_entities = []

    tokenized_sentence = tokenizer.encode(sentence)
    input_ids = torch.tensor([tokenized_sentence]).to(device)

    predictions = []
    with torch.no_grad():
        output = model(input_ids)
        output = output[0].detach().cpu().numpy()
        predictions.extend([list(p) for p in np.argmax(output, axis=2)])


    tags_predictions = []
    for x in predictions[0]:
        tags_predictions.append(idx2tag[int(x)])

    tokens = []
    count = 0

    ### get tokens from ids
    for x in tokenizer.convert_ids_to_tokens(tokenized_sentence):
        if count == 1:
            tokens.append(x)
        else:
            tokens.append(x[1:])
        count+=1

    all_entities.append(tags_predictions[1:-1])
    all_tokens.append(tokens[1:-1])
    return all_tokens,all_entities
