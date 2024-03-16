import numpy as np
import os
import subprocess
import cv2
from pathlib import Path
from cpd_nonlin import cpd_nonlin
from cpd_auto import cpd_auto

def ffprob_shot_segmentation(video_path='data',
                             video_name='Cosmus_Laundromat.mp4'):
    BINS_NUMBER_PER_CHANNEL = 32
    video_id = video_name.split('_')[3]
    video_id = video_id.split('.')[0]
    ffprobe_output_path = os.path.join('/home/ubuntu/wanjun/m_script_collection/5_NN/DCIM/key_frame/ILS-SUMM/data/ffprobe', video_id)
    # shot_seg_text_file = os.path.join(video_path, 'shot_segmentation.txt')
    
    
    output_path_in_linux_style = '/'.join(ffprobe_output_path.split('\\'))
    ouput_file = '/'.join([output_path_in_linux_style, 'shot_segmentation.txt'])
    if not os.path.isfile(ouput_file):
        print("Ffmpeg shot segmentation in action...")
        video_path_in_linux_style = '/'.join(video_path.split('\\'))
        full_video_path = '/'.join([video_path_in_linux_style, video_name])
        command = 'ffprobe -show_frames -of compact=p=0 -f lavfi "movie=' + full_video_path + ',select=gt(scene\,.4)" > ' + ouput_file
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        proc.communicate()
        print("Finished ffmpeg shot segmentation")
    print("Reading shot seg text file")
    with open(ouput_file) as f:
        content = f.readlines()
    final_C = []
    final_X = []
    shotIdx = []
    frames_per_second = getFramerate(os.path.join(video_path, video_name))
    if(len(content) > 0):
        shotIdx = [0]
        i = 0
        for line in content:
            shotIdx.append(
                np.int(
                    np.round(
                        float(
                            line.split(sep="pkt_pts_time=")[1].split(
                                sep="|pkt_dts")[0]) * frames_per_second)))
            i = i + 1
        # Impose a minimum (Lmin) and maximum (Lmax) shot length:
        Lmin = 25
        Lmax = 200
        cap = cv2.VideoCapture(os.path.join(video_path, video_name))
        total_num_of_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        C = np.subtract(np.append(shotIdx[1:], total_num_of_frames), shotIdx)
        # Consolidate a short shot with the following shot:
        C_without_short_shots = []
        for i in range(len(C) - 1):
            if C[i] >= Lmin:
                C_without_short_shots.append(C[i])
            else:
                C[i + 1] = C[i + 1] + C[i]
        if C[-1] >= Lmin:
            C_without_short_shots.append(C[-1])
        else:
            C_without_short_shots[-1] += C[-1]
        # Break long shot into smaller parts:
        
        for i in range(len(C_without_short_shots)):
            if C_without_short_shots[i] <= Lmax:
                final_C.append(C_without_short_shots[i])
            else:
                devide_factor = np.int((C_without_short_shots[i] // Lmax) + 1)
                length_of_each_part = C_without_short_shots[i] // devide_factor
                for j in range(devide_factor - 1):
                    final_C.append(length_of_each_part)
                final_C.append(C_without_short_shots[i] -
                            (devide_factor - 1) * length_of_each_part)
        images = len(final_C)
        final_X = np.zeros((images, BINS_NUMBER_PER_CHANNEL * 3), dtype=float)
        tempIdx = []
        tempIdx = np.concatenate(([0],np.cumsum(final_C[:-1])))
        success = True
        count = 0
        j = 0
        for i in range(len(tempIdx)):
            frame_gap = tempIdx[i]
            while(success):
                success,frame = cap.read()
                if(count == frame_gap):
                    r_values = frame[:,:,0].flatten()
                    g_values= frame[:, :, 1].flatten()
                    b_values = frame[:, :, 2].flatten()

                    r_hist, _ = np.histogram(r_values, BINS_NUMBER_PER_CHANNEL, [0, 256])
                    normalized_r_hist =  r_hist / np.sum(r_hist)
                    g_hist, _ = np.histogram(g_values, BINS_NUMBER_PER_CHANNEL, [0, 256])
                    normalized_g_hist =  g_hist / np.sum(g_hist)
                    b_hist, _ = np.histogram(b_values, BINS_NUMBER_PER_CHANNEL, [0, 256])
                    normalized_b_hist =  b_hist / np.sum(b_hist)

                    final_X[j,:] = np.concatenate((normalized_r_hist, normalized_g_hist, normalized_b_hist))
                    j = j + 1
                    break
                count  = count + 1
            if(count >= total_num_of_frames):
                break
            if(j >= images):
                break
        cap.release()
    return final_C,final_X,shotIdx,frames_per_second


def getFramerate(video_path):
    con = "ffprobe -v error -select_streams v:0 -show_entries stream=avg_frame_rate -of default=noprint_wrappers=1:nokey=1 " + video_path
    print('getFramerate:', con)
    proc = subprocess.Popen(con,
                            stdout=subprocess.PIPE,
                            stdin=subprocess.PIPE,
                            shell=True)
    framerateString = str(proc.stdout.read())[2:-3]
    print('framerateString:', framerateString)
    a = int(framerateString.split('/')[0])
    b = int(framerateString.split('/')[1])
    proc.kill()
    return int(np.round(np.divide(a, b)))


# if __name__ == "__main__":
#     result = ffprob_shot_segmentation()
#     print('ffprob_shot_segmentation:', result)
