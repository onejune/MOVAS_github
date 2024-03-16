import numpy as np
import os
import sys
import glob
from ILS_SUMM import ILS_SUMM
# import matplotlib.pyplot as plt
# from moviepy.editor import VideoFileClip, concatenate_videoclips
from ffprob_shot_segmentation import ffprob_shot_segmentation
# from extract_features import extract_features
# from moviepy.editor import *
import cv2
#from KTS import KTS
from pathlib import Path

def demo(video_path ='/home/ubuntu/dcim/dataset/video_cluster', summ_ratio=0.6):
    SUMM_RATIO = 0.1  # The maximum allowed ratio between the summary video and the full video.
    video_path = '/home/ubuntu/dcim/dataset/video_cluster'
    fileCount = 0
    for file in glob.glob(os.path.join(video_path, "*.mp4")) :
        video_name  = file.split('/')[6]
        video_id = video_name.split('_')[3]
        video_id = video_id.split('.')[0]
        ILS_output_path = os.path.join('/home/ubuntu/wanjun/m_script_collection/5_NN/DCIM/key_frame/ILS-SUMM/data/ILS', video_id)
        # KTS_output_path = os.path.join('/home/ubuntu/wanjun/m_script_collection/5_NN/DCIM/key_frame/ILS-SUMM/data/KTS', video_id)
        ffprobe_output_path = os.path.join('/home/ubuntu/wanjun/m_script_collection/5_NN/DCIM/key_frame/ILS-SUMM/data/ffprobe', video_id)
        if Path(ILS_output_path).exists():
            print("ILS_Path is exists !")
        else:
            os.makedirs(ILS_output_path)
        # if Path(KTS_output_path).exists():
        #     print("KTS_Path is exists !")
        # else:
        #     os.makedirs(KTS_output_path)
        if Path(ffprobe_output_path).exists():
            print("ffprobe_Path is exists !")
        else:
            os.makedirs(ffprobe_output_path)

        C = []
        X = []
        indx = []
        C,X,ffshot,framesRate = ffprob_shot_segmentation(video_path,video_name)
        print(C)
        if(len(ffshot) > 1):
            for i in range(len(ffshot)):
                if (i > 0):
                    indx.append([ffshot[i],1])
            budget = float(summ_ratio) * np.sum(C)      
            representative_points, total_distance = ILS_SUMM(np.asarray(X),np.asarray(C), budget)
            shotIdx = np.concatenate(([0], np.cumsum(C[:-1])))
            for i in range(len(representative_points)):
                indx.append([shotIdx[representative_points[i]],2])
                if (i >= 4):
                    break
                

 #       cps = KTS(file)
 #       if(len(cps) > 0):
 #           for i in range(len(cps)):
 #               indx.append([cps[i]*5,3])
                
        indx = np.asarray(indx)
        if(indx.shape[0] == 0):
            cap = cv2.VideoCapture(file)
            total_num_of_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            success = True
            count = 0
            while(success):
                success,frame = cap.read()
                if (count == 0):
                    params = []
                    params.append(int(cv2.IMWRITE_JPEG_QUALITY))
                    params.append(95)
                    cv2.imwrite(ffprobe_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count) , frame , params)
                    cv2.imwrite(ILS_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count) , frame , params)
                    # cv2.imwrite(KTS_output_path + '/' + video_id + '_%d.jpg' % count , frame , params)
                if (count == int(total_num_of_frames/2)):
                    params = []
                    params.append(int(cv2.IMWRITE_JPEG_QUALITY))
                    params.append(95)
                    cv2.imwrite(ffprobe_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count) , frame , params)
                    cv2.imwrite(ILS_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count ), frame , params)
                    # cv2.imwrite(KTS_output_path + '/' + video_id + '_%d.jpg' % count , frame , params)
                count  = count + 1
                if(count >= int(total_num_of_frames/2)+1):
                    break
        else:
            indx = indx[np.argsort(indx[:,0])]
            indxCount = indx.shape[0]

            cap = cv2.VideoCapture(file)
            total_num_of_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            success = True
            count = 0
            for i in range(indxCount):
                    frame_gap = indx[i,0]
                    while(success):
                        success,frame = cap.read()
                        if (count == 0):
                            params = []
                            params.append(int(cv2.IMWRITE_JPEG_QUALITY))
                            params.append(95)
                            cv2.imwrite(ffprobe_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count) , frame , params)
                            cv2.imwrite(ILS_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count) , frame , params)
    #                        cv2.imwrite(KTS_output_path + '/' + video_id + '_%d.jpg' % count , frame , params)
                        if(count == frame_gap):
                            params = []
                            params.append(int(cv2.IMWRITE_JPEG_QUALITY))
                            params.append(95)
                            if(indx[i,1] == 1):
                                cv2.imwrite(ffprobe_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count) , frame , params)
                            if(indx[i,1] == 2):
                                cv2.imwrite(ILS_output_path + '/' + video_id + '_%d_%d_%d.jpg' %( total_num_of_frames,framesRate,count) , frame , params)
     #                       if(indx[i,1] == 3):
     #                           cv2.imwrite(KTS_output_path + '/' + video_id + '_%d.jpg' % count , frame , params)
                            count = count + 1
                            break
                        count  = count + 1
                        if(count >= total_num_of_frames):
                            break
                    if(count >= total_num_of_frames):
                        break
        cap.release()
        if(fileCount == 5):
            break
        fileCount = fileCount + 1
        # X = extract_features(ffprobe_output_path)
        # ffprob_shot_segmentation('data',video_name)
        # X = np.load(os.path.join('data', 'shots_features.npy'))  # Load n x d feature matrix. n - number of shots, d - feature dimension.
        # C = np.load(os.path.join('data', 'shots_durations.npy'))  # Load n x 1 shots duration array (number of frames per shot).
        # Display Results:
        #representative_points = np.sort(representative_points)
        #print("The selected shots are: " + str(representative_points))
        #print("The achieved total distance is: " +str(np.round(total_distance,3)))
        # u, s, vh = np.linalg.svd(X)
        # plt.figure()
        # point_size = np.divide(C, np.max(C)) * 100
        # plt.scatter(u[:, 1], u[:, 2], s=point_size, c='lawngreen', marker='o')
        # plt.scatter(u[representative_points, 1], u[representative_points, 2], s=point_size[representative_points],
        #            c='blue', marker='o')
        # plt.title('Solution Visualization (total distance = ' + str(total_distance) + ')')
        # plt.savefig(os.path.join(output_path, 'Solution_Visualization'))

        # Generate the video summary file
        


        # video_file_path = os.path.join(video_path, video_name)
        # video_clip = VideoFileClip(video_file_path)
        # shotIdx = np.concatenate(([0], np.cumsum(C[:-1])))
        # print(video_clip.duration)
        # print(shotIdx)
        # frames_per_seconds = np.sum(C)/ video_clip.duration
        # chosen_shots_clips = []
        # for i in range(len(representative_points)):
        #     curr_start_time = shotIdx[representative_points[i]] / frames_per_seconds  # [Sec]
        #     if representative_points[i] == (shotIdx.__len__() - 1):
        #         curr_end_time = video_clip.duration
        #     else:
        #         curr_end_time = (shotIdx[representative_points[i] + 1] - 1) / frames_per_seconds  # [Sec]
        #     chosen_shots_clips.append(VideoFileClip(video_file_path).subclip(curr_start_time, curr_end_time))
        # if chosen_shots_clips == []:
        #     print("The length of the shortest shots exceeds the allotted summarization time")
        # else:
        #     summ_clip = concatenate_videoclips(chosen_shots_clips)

        #     summ_clip.write_videofile(os.path.join(ffprobe_output_path, "video_summary.mp4"))

if __name__ == "__main__":
    demo()
    # demo(sys.argv[1],float(sys.argv[2]))
