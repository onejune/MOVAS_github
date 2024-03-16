import ps
import torch
import torch.nn as nn
import torch.nn.functional as F

class Normalization(nn.modules.batchnorm._BatchNorm):
    def _check_input_dim(self, input):
        if input.dim() != 2 and input.dim() != 3:
            raise ValueError('expected 3D or 3D input (got {}D input)'.format(input.dim()))


    def forward(self, input):
        if not self.training:
            return F.batch_norm(input, self.running_mean, self.running_var, self.weight, self.bias, False)
        self._check_input_dim(input)
        if self.momentum is None:
            exponential_average_factor = 0.0 
        else:
            exponential_average_factor = self.momentum

        if self.track_running_stats:
            if self.num_batches_tracked is not None:
                self.num_batches_tracked = self.num_batches_tracked + 1 
                if self.momentum is None:  # use cumulative moving average
                    exponential_average_factor = 1.0 / float(self.num_batches_tracked)
                else:  # use exponential moving average
                    exponential_average_factor = self.momentum
        output = (input - self.running_mean)/(self.running_var + self.eps).sqrt()
        with torch.no_grad():
            rate = input.shape[0]/1000.0 #batch_size
            batch_mean = input.mean(dim=0)
            batch_var = ((input-self.running_mean) * (input-self.running_mean)).mean(dim=0)
            self.running_mean[...] = (1-exponential_average_factor*rate) * self.running_mean + exponential_average_factor*rate * batch_mean
            self.running_var[...] = (1-exponential_average_factor*rate) * self.running_var + exponential_average_factor*rate * batch_var
            #self.running_mean[...] = batch_mean
            #self.running_var[...] = batch_var
        output1 = output * self.weight + self.bias
        return output1


class NNRankModel(nn.Module):
    def __init__(self,column_name, combine_schema):
        super().__init__()
        self._embedding_size = 16
        self._sparse = ps.EmbeddingSumConcat(self._embedding_size, column_name, combine_schema)
        self._sparse.updater = ps.FTRLTensorUpdater(alpha=0.01,l1=1.0)
        self._sparse.initializer = ps.NormalTensorInitializer(var=0.01)
        
        
        self._sparse_param = ps.EmbeddingSumConcat(self._embedding_size, column_name, combine_schema)
        self._sparse_param.updater = ps.FTRLTensorUpdater(alpha=0.01,l1=1.0)
        self._sparse_param.initializer = ps.NormalTensorInitializer(var=0.01)        
        
        self._feature_index=30
        
        self._sparse_feature_count=self._sparse.feature_count
        self._sparse_feature_num=self._sparse.feature_count*self._embedding_size
        self._sparse_feature_num_att=self._feature_index*self._embedding_size
        self._dense = torch.nn.Sequential(
            torch.nn.Linear(self._sparse_feature_num, 1024),
            torch.nn.GELU(),
        )
        self._dense2 = torch.nn.Sequential(
            torch.nn.Linear(1024, 512),
            torch.nn.GELU(),
        ) 
        self._dense3 = torch.nn.Sequential(
            torch.nn.Linear(512, 1),
        )
        
        self._dense_field_fg = torch.nn.Sequential(
            torch.nn.Linear(self._sparse_feature_num, self._sparse.feature_count),
            torch.nn.Sigmoid(),
        )        
        
        
        mid_size=100
        self._dense_field_encode = torch.nn.Sequential(
            torch.nn.Linear(self._sparse_feature_num, mid_size),
            torch.nn.Sigmoid(),
        ) 
        
        all_mid_size=mid_size+self._sparse_feature_num_att
        
        self._dense_field_mid = torch.nn.Sequential(
            torch.nn.Linear(all_mid_size, self._sparse_feature_num_att+mid_size),
            torch.nn.Sigmoid(),
        )        
        
        self._dense_field = torch.nn.Sequential(
            torch.nn.Linear(all_mid_size, self._sparse_feature_num),
            torch.nn.Sigmoid(),
        )     
        
        self._dense_field2 = torch.nn.Sequential(
            torch.nn.Linear(1024+all_mid_size, 1024),
            torch.nn.Sigmoid(),
        )    
        self._dense_field3 = torch.nn.Sequential(
            torch.nn.Linear(512+all_mid_size, 512),
            torch.nn.Sigmoid(),
        )         
        
        #self._bn = torch.nn.BatchNorm1d(152 * self._embedding_size, momentum=0.01, eps=1e-5, affine=True)
        self._bn = Normalization(self._sparse_feature_num, momentum=0.01, eps=1e-5, affine=True)
        self._bn_param = Normalization(self._sparse_feature_num, momentum=0.01, eps=1e-5, affine=True)
        
        
        self._dense[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense2[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense3[0].bias.initializer = ps.ZeroTensorInitializer()
        
        self._dense_field_fg[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense_field_encode[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense_field_mid[0].bias.initializer = ps.ZeroTensorInitializer()
         
        self._dense_field[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense_field2[0].bias.initializer = ps.ZeroTensorInitializer()
        self._dense_field3[0].bias.initializer = ps.ZeroTensorInitializer()
        #self._dense_nn_param[0].bias.initializer = ps.ZeroTensorInitializer()

        self._sparse2 = ps.EmbeddingSumConcat(1, column_name, combine_schema)
        self._sparse2.updater = ps.FTRLTensorUpdater(alpha=0.005,l1=1.0)
        self._sparse2.initializer = ps.NormalTensorInitializer(var=0.001)
        self._batch_id=0


    def forward(self, x):
        #LR
        emb1rd=self._sparse2(x)
        #emb1rd_fx=self._dense_LR_param(bno) * emb1rd       
        #print('xxx',ff.shape,field_emb.shape,emb1rd.shape,emb1rd_fx.shape)
        emb1rd_fx=torch.sum(emb1rd,dim=1,keepdim=True)   
        
        
        emb = self._sparse(x)
        bno = self._bn(emb)        
        
        emb_param=self._sparse_param(x)
        bno_param=self._bn_param(emb_param)
        #field param
        param_fg=self._dense_field_fg(bno_param)
        param_fg=param_fg.unsqueeze(2)
        bno_fg=torch.reshape(bno_param,(bno_param.shape[0],self._sparse_feature_count,self._embedding_size))
        bno_fg=param_fg*bno_fg
        bno_fg=torch.reshape(bno_fg,(bno_fg.shape[0],self._sparse_feature_num))
        
        #dim param
        #bn_other=bno[:,self._sparse_feature_num_att:]
        bn_att=bno_fg[:,:self._sparse_feature_num_att]
        bn_other_encode=self._dense_field_encode(bno_fg)
        
        bno_d=torch.cat([bn_other_encode,bn_att],1)
        bno_d_param=self._dense_field_mid(bno_d)
        bno_d=bno_d_param*bno_d
        param=self._dense_field(bno_d)
        new_emb=param*emb
        bno=param*bno
        bn_att=bno_d

        #FM
        #print('fm_param.shape',fm_param.shape,field_emb.shape)
        
        fm_input=torch.reshape(new_emb,(new_emb.shape[0],self._sparse_feature_count,self._embedding_size))
        #new_emb=torch.reshape(fm_input,(fm_input.shape[0],self._sparse.feature_count*self._embedding_size))

        square_of_sum = torch.pow(torch.sum(fm_input, dim=1, keepdim=True), 2)


        sum_of_square = torch.sum(fm_input * fm_input, dim=1, keepdim=True)


        cross_term = square_of_sum - sum_of_square

        cross_term = 0.5 * torch.sum(cross_term, dim=2, keepdim=False)        

        
        d=self._dense(bno)
        
        bno_d2=torch.cat([d,bn_att],1)
        p2=self._dense_field2(bno_d2)
        d=p2*d
        d2=self._dense2(d)
        
        bno_d3=torch.cat([d2,bn_att],1)
        p3=self._dense_field3(bno_d3)
        d3=p3*d2
        x = self._dense3(d3)
        
        #if self._batch_id % 20 == 0:
            #zx=x[:10,:]
            #zy=emb1rd_fx[:10,:]
            #zc=cross_term[:10,:]
            #zz=torch.cat([zx,zy,zc],1)
            #print ('zzzzzz',zz)
            #print ('param',torch.max(param),torch.min(param),param[0,:])
            #print ('p2',torch.max(p2),torch.min(p2),p2[0,:])
            #print ('p3',torch.max(p3),torch.min(p3))




        self._batch_id+=1
        return torch.sigmoid(x+cross_term+emb1rd_fx)

