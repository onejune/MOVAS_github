#########################################################################
# File Name: get_pkg.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Thu 09 Dec 2021 06:09:47 PM CST
#########################################################################
#!/bin/bash

sql='select * from adn_dsp.make_money_pkg;'

hive -e "$sql" > pkg_list
