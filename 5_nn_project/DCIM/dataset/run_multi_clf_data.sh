#########################################################################
# File Name: run_multi_clf_data.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu Aug  5 02:33:43 2021
#########################################################################
#!/bin/bash

dtm=$(date +"%Y%m%d%H" -d "0 days ago")
echo "dtm:$dtm"

package_fea_file="data/m_ftrl_feature_new.dat"
grep "^package" $package_fea_file > output/package_dmptag_ori.dat
grep "^dsp_pkg" $package_fea_file >> output/package_dmptag_ori.dat

#calculate the number of creatives for each package
python figure_pkg_sid.py

python extract.py


