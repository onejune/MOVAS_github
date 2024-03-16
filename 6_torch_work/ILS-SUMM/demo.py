import numpy as np
import os
import sys
import glob
from ILS_SUMM import ILS_SUMM
import matplotlib.pyplot as plt
from moviepy.editor import VideoFileClip, concatenate_videoclips
from ffprob_shot_segmentation import ffprob_shot_segmentation
from extract_features import extract_features
from moviepy.editor import *

def demo(video_path ='/home/ubuntu/dcim/dataset/video_cluster ', summ_ratio=0.15):
    SUMM_RATIO = 0.1  # The maximum allowed ratio between the summary video and the full video.
    video_path = '/home/ubuntu/dcim/dataset/video_cluster'
    j = 0
    for file in glob.glob(os.path.join(video_path, "*.mp4")) :
        video_name  = file.split('/')[6]
        C = ffprob_shot_segmentation(video_path,video_name)
        print(C)
        video_id = video_name.split('_')[3]
        video_id = video_id.split('.')[0]
        output_path = os.path.join('/home/ubuntu/wanjun/m_script_collection/6_torch_work/ILS-SUMM/data', video_id)
  
        X = extract_features(output_path)
        # ffprob_shot_segmentation('data',video_name)
        # X = np.load(os.path.join('data', 'shots_features.npy'))  # Load n x d feature matrix. n - number of shots, d - feature dimension.
        # C = np.load(os.path.join('data', 'shots_durations.npy'))  # Load n x 1 shots duration array (number of frames per shot).

        # Calculate allowed budget
        budget = float(summ_ratio) * np.sum(C)

        # Use ILS_SUMM to obtain a representative subset which satisfies the knapsack constraint.
        representative_points, total_distance = ILS_SUMM(np.asarray(X),np.asarray(C), budget)
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
        video_file_path = os.path.join(video_path, video_name)
        video_clip = VideoFileClip(video_file_path)
        shotIdx = np.concatenate(([0], np.cumsum(C[:-1])))
        print(video_clip.duration)
        print(shotIdx)
        frames_per_seconds = np.sum(C)/ video_clip.duration
        chosen_shots_clips = []
        for i in range(len(representative_points)):
            curr_start_time = shotIdx[representative_points[i]] / frames_per_seconds  # [Sec]
            if representative_points[i] == (shotIdx.__len__() - 1):
                curr_end_time = video_clip.duration
            else:
                curr_end_time = (shotIdx[representative_points[i] + 1] - 1) / frames_per_seconds  # [Sec]
            chosen_shots_clips.append(VideoFileClip(video_file_path).subclip(curr_start_time, curr_end_time))
        if chosen_shots_clips == []:
            print("The length of the shortest shots exceeds the allotted summarization time")
        else:
            summ_clip = concatenate_videoclips(chosen_shots_clips)

            summ_clip.write_videofile(os.path.join(output_path, "video_summary.mp4"))
        j = j + 1
        if(j==5):
            break

if __name__ == "__main__":
    demo()
    # demo(sys.argv[1],float(sys.argv[2]))
