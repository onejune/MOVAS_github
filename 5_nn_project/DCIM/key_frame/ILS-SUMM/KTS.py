import os
import glob
import numpy as np
import cv2
import re

from cpd_nonlin import cpd_nonlin
from cpd_auto import cpd_auto

def get_data(file_path = 'data\\330075001.mp4'):
    cap = cv2.VideoCapture(file_path)
    total_num_of_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    images = int(total_num_of_frames/5)
    BINS_NUMBER_PER_CHANNEL = 32
    features = np.zeros((images, BINS_NUMBER_PER_CHANNEL * 3), dtype=float)
    count = 0
    i = 0
    success = True
    while(success):
        success,frame = cap.read()
        if (count % 5 == 0):
            r_values = frame[:,:,0].flatten()
            g_values= frame[:, :, 1].flatten()
            b_values = frame[:, :, 2].flatten()

            r_hist, _ = np.histogram(r_values, BINS_NUMBER_PER_CHANNEL, [0, 256])
            normalized_r_hist =  r_hist / np.sum(r_hist)
            g_hist, _ = np.histogram(g_values, BINS_NUMBER_PER_CHANNEL, [0, 256])
            normalized_g_hist =  g_hist / np.sum(g_hist)
            b_hist, _ = np.histogram(b_values, BINS_NUMBER_PER_CHANNEL, [0, 256])
            normalized_b_hist =  b_hist / np.sum(b_hist)

            features[i,:] = np.concatenate((normalized_r_hist, normalized_g_hist, normalized_b_hist))
            i = i + 1
            if (i >=  images):
                break
        count = count + 1
        if(count >= total_num_of_frames):
            break

    return features

# def gen_data(n, m, d=1):
    # """Generates data with change points
    # n - number of samples
    # m - number of change-points
    # WARN: sigma is proportional to m
    # Returns:
    #     X - data array (n X d)
    #     cps - change-points array, including 0 and n"""
    # np.random.seed(1)
    # # Select changes at some distance from the boundaries
    # cps = np.random.permutation(int(n*3/4)-1)[0:m] + 1 + n/8
    # cps = np.sort(cps)
    # cps = [0] + list(cps) + [n]
    # mus = np.random.rand(m+1, d)*(m/2)  # make sigma = m/2
    # X = np.zeros((n, d))
    # for k in range(m+1):
    #     X[int(cps[k]):int(cps[k+1]), :] = mus[k, :][np.newaxis, :] +  np.random.rand(int(cps[k+1]-cps[k]), d) 
    # return (X, np.array(cps))

    
def KTS(file_path = '/home/ubuntu/dcim/dataset/video_cluster'):
    m = 10
    X = get_data(file_path)
    K = np.dot(X, X.T)
    cps, scores = cpd_auto(K, 2*m, 1)
    return cps
    


 


