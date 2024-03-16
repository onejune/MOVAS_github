#########################################################################
# File Name: hb_factor_get.sh
# Author: onejune
# mail: onejune@126.com
# Created Time: Tue 01 Sep 2020 11:02:41 AM CST
#########################################################################
#!/bin/bash
adx_hb_price_factor="./output/adx_hb_price_factor.txt"
#mysql -h adn-mysql-external.mobvista.com -P 3306 -u mob_adn_ro -pblueriver123 -Ne "desc mob_adn.adx_hb_price_factor ;" > ${adx_hb_price_factor}
mysql -h adn-mysql-external.mobvista.com -P 3306 -u mob_adn_ro -pblueriver123 -Ne "select unit_id, area, factor from mob_adn.adx_hb_price_factor where status=1;" > ${adx_hb_price_factor}

#awk '{if($2>=1.3) print $1}' $adx_hb_price_factor | tr "\n" "," | sed 's/.$//' > hb_factor_big
