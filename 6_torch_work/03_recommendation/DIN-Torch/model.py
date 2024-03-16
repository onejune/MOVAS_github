import torch.nn as nn
import torch

from embedding import EmbeddingLayer
from fc import FullyConnectedLayer
from attention import AttentionSequencePoolingLayer



dim_config = {
    'user_exposed_time': 24,
    'user_gender': 2,
    'user_age': 9,
    'history_article_id': 53932,   # multi-hot
    'history_image_feature': 2048,
    'history_categories': 23,
    'query_article_id': 1856,    # one-hot
    'query_image_feature': 2048,
    'query_categories': 23
}

que_embed_features = ['query_article_id']
que_image_features = ['query_image_feature']
que_category =  ['query_categories']

his_embed_features = ['history_article_id']
his_image_features = ['history_image_feature']
his_category =  ['history_categories']

image_hidden_dim = 64
category_dim = 23
embedding_size = 64
embed_features = [k for k, _ in dim_config.items() if 'user' in k]

class DeepInterestNetwork(nn.Module):
    def __init__(self):
        super().__init__()
        

        self.query_feature_embedding_dict = dict()
        for feature in que_embed_features:
            self.query_feature_embedding_dict[feature] = EmbeddingLayer(feature_dim=dim_config[feature],
                                                                        embedding_dim=embedding_size)
        self.query_image_fc = FullyConnectedLayer(input_size=2048,
                                                  hidden_size=[image_hidden_dim],
                                                  bias=[True],
                                                  activation='relu')
        
        self.history_feature_embedding_dict = dict()
        for feature in his_embed_features:
            self.history_feature_embedding_dict[feature] = EmbeddingLayer(feature_dim=dim_config[feature],
                                                                          embedding_dim=embedding_size)     
        self.history_image_fc = FullyConnectedLayer(input_size=2048,
                                                    hidden_size=[image_hidden_dim],
                                                    bias=[True],
                                                    activation='relu')                                                      

        self.attn = AttentionSequencePoolingLayer(embedding_dim=image_hidden_dim + embedding_size + category_dim)
        input_size = 2 * (image_hidden_dim + embedding_size + category_dim) + sum([dim_config[k] for k in embed_features])
        self.fc_layer = FullyConnectedLayer(input_size,
                                            hidden_size=[200, 80, 1],
                                            bias=[True, True, False],
                                            activation='relu',
                                            sigmoid=True)

    def forward(self, user_features):
        # user_features -> dict (key:feature name, value: feature tensor)

        # deep input embedding for user feature
        user_feature_embedded = []

        for feature in embed_features:
            user_feature_embedded.append(user_features[feature])

        user_feature_embedded = torch.cat(user_feature_embedded, dim=1)
        print('user_feature_embedded:', user_feature_embedded)
        print('User_feature_embed size', user_feature_embedded.size()) # batch_size * (feature_size * embedding_size)
        print('----User feature done')

        #build query embedding
        print("build query embedding....")
        query_feature_embedded = []

        for feature in que_embed_features:
            fea = user_features[feature]
            emb = self.query_feature_embedding_dict[feature](fea.squeeze())
            print("que_embed_features:", fea.shape, fea.size(), emb.shape, emb.size())
            query_feature_embedded.append(emb)
        for feature in que_image_features:
            fea = user_features[feature]
            emb = self.query_image_fc(fea)
            print("que_image_features:", fea.shape, fea.size(), emb.shape, emb.size())
            query_feature_embedded.append(emb)
        for feature in que_category:
            fea = user_features[feature]
            emb = fea
            print("que_category:", fea.shape, fea.size(), emb.shape, emb.size())
            query_feature_embedded.append(emb)

        for ele in query_feature_embedded:
            print(ele.size())
            
        query_feature_embedded = torch.cat(query_feature_embedded, dim=1)
        print('Query feature_embed size', query_feature_embedded.size()) # batch_size * (feature_size * embedding_size)
        print('----Query feature done')
        # exit()

        print("build history feature embedding....")
        history_feature_embedded = []
        for feature in his_embed_features:
            fea = user_features[feature]
            emb = self.history_feature_embedding_dict[feature](fea)
            history_feature_embedded.append(emb)
            print("his_embed_features:", feature, fea.size(), emb.size())

        for feature in his_image_features:
            fea = user_features[feature]
            emb = self.history_image_fc(fea)
            history_feature_embedded.append(emb)
            print("his_image_features:", feature, fea.size(), emb.size())
            
        for feature in his_category:
            fea = user_features[feature]
            history_feature_embedded.append(fea)
            print("his_category:", feature, fea.size())
            
        for ele in history_feature_embedded:
            print(ele.size())
            
        history_feature_embedded = torch.cat(history_feature_embedded, dim=2)
        print('History feature_embed size', history_feature_embedded.size()) # batch_size * T * (feature_size * embedding_size)
        print('History feature done')
        
        print(user_features['history_len'])
        print(user_features['history_len'].size())
        
        
        history = self.attn(query_feature_embedded.unsqueeze(1), 
                            history_feature_embedded, 
                            user_features['history_len']) 
        
        concat_feature = torch.cat([user_feature_embedded, query_feature_embedded, history.squeeze()], dim=1)
        
        # fully-connected layers
        print(concat_feature.size())
        output = self.fc_layer(concat_feature)
        return output


if __name__ == "__main__":
    a = DeepInterestNetwork()
    import torch
    import numpy as np
    
    batch_size = 100
    user_feature = {
        'user_exposed_time': torch.LongTensor(np.zeros(shape=(batch_size, embedding_size))),
        'user_gender': torch.LongTensor(np.zeros(shape=(batch_size, embedding_size))),
        'user_age': torch.LongTensor(np.zeros(shape=(batch_size, embedding_size))),
        'query_article_id': torch.LongTensor(np.zeros(shape=(batch_size, 1))),
        'query_image_feature': torch.FloatTensor(np.zeros(shape=(batch_size, 1))),
        'query_categories': torch.LongTensor(np.zeros(shape=(batch_size, 1))),
        'history_article_id' : torch.LongTensor(np.zeros(shape=(batch_size, 8))),
        'history_image_feature' : torch.FloatTensor(np.zeros(shape=(batch_size, 12))),
        'history_categories': torch.LongTensor(np.zeros(shape=(batch_size, 3))),
    }
    a(user_feature)