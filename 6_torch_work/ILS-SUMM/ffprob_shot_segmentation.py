import numpy as np
import os
import subprocess
import cv2
from pathlib import Path

def ffprob_shot_segmentation(video_path='data',
                             video_name='Cosmus_Laundromat.mp4'):
    video_id = video_name.split('_')[3]
    video_id = video_id.split('.')[0]
    output_path = os.path.join('/home/ubuntu/wanjun/m_script_collection/6_torch_work/ILS-SUMM/data', video_id)
    # shot_seg_text_file = os.path.join(video_path, 'shot_segmentation.txt')
    if Path(output_path).exists():
        print("Path is exists !")
    else:
        os.makedirs(output_path)
    output_path_in_linux_style = '/'.join(output_path.split('\\'))
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
    shotIdx = [0]
    frames_per_second = getFramerate(os.path.join(video_path, video_name))
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
    final_C = []
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
    shotIdx = []
    shotIdx = np.concatenate(([0],np.cumsum(final_C[:-1])))
    success = True
    count = 0
    for i in range(len(shotIdx)):
        frame_gap = shotIdx[i]
        while(success):
            success,frame = cap.read()
            if(count == frame_gap):
                params = []
                params.append(int(cv2.IMWRITE_JPEG_QUALITY))
                params.append(95)
                cv2.imwrite(output_path_in_linux_style + '/' + video_id + '_%d.jpg' % count , frame , params)
                break
            count  = count + 1
    cap.release()
    return final_C


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
