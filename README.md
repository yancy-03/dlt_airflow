# dlt_airflow_demo
# dlt_airflow
# 先清除代理环境变量
unset http_proxy
unset https_proxy
unset all_proxy

# 再重新更新
sudo apt update

# 备份原文件
sudo cp /etc/apt/sources.list /etc/apt/sources.list.bak

# 写入阿里云镜像
cat << 'EOF' | sudo tee /etc/apt/sources.list
deb http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse
EOF

# 更新并安装基础包
sudo apt update && sudo apt install -y python3-venv python3-pip
# 安装 Python 环境
sudo apt update && sudo apt install -y python3-venv

# 创建环境
python3 -m venv myenv

# 激活环境
source myenv/bin/activate

# 安装 airflow + dlt
pip install apache-airflow==2.10.3 dlt[sqlalchemy] pandas pymysql

pip install apache-airflow==2.10.3 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.10.txt"
pip install dlt[sqlalchemy] pandas pymysql

# 第一步：回到你的用户主目录（环境在这里）
cd ~

# 第二步：激活 myenv310 环境
source myenv310/bin/activate

 airflow scheduler
  airflow standalone

cp dags/dag_pokemon_source_demo.py ~/airflow/dags/

cp expriment/pokemon.py ~/airflow/dags/





airflow users create \
  --username admin \
  --firstname yancy \
  --lastname li \
  --role Admin \
  --email 3268575247@qq.com \
  --password "yancy821566!"

sudo apt install net-tools
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ ip
Usage: ip [ OPTIONS ] OBJECT { COMMAND | help }
       ip [ -force ] -batch filename
where  OBJECT := { address | addrlabel | fou | help | ila | ioam | l2tp | link |
                   macsec | maddress | monitor | mptcp | mroute | mrule |
                   neighbor | neighbour | netconf | netns | nexthop | ntable |
                   ntbl | route | rule | sr | stats | tap | tcpmetrics |
                   token | tunnel | tuntap | vrf | xfrm }
       OPTIONS := { -V[ersion] | -s[tatistics] | -d[etails] | -r[esolve] |
                    -h[uman-readable] | -iec | -j[son] | -p[retty] |
                    -f[amily] { inet | inet6 | mpls | bridge | link } |
                    -4 | -6 | -M | -B | -0 |
                    -l[oops] { maximum-addr-flush-attempts } | -echo | -br[ief] |
                    -o[neline] | -t[imestamp] | -ts[hort] | -b[atch] [filename] |
                    -rc[vbuf] [size] | -n[etns] name | -N[umeric] | -a[ll] |
                    -c[olor]}
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ ip list
Object "list" is unknown, try "ip help".
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ ifconfig
Command 'ifconfig' not found, but can be installed with:
sudo apt install net-tools
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ ip help
Usage: ip [ OPTIONS ] OBJECT { COMMAND | help }
       ip [ -force ] -batch filename
where  OBJECT := { address | addrlabel | fou | help | ila | ioam | l2tp | link |
                   macsec | maddress | monitor | mptcp | mroute | mrule |
                   neighbor | neighbour | netconf | netns | nexthop | ntable |
                   ntbl | route | rule | sr | stats | tap | tcpmetrics |
                   token | tunnel | tuntap | vrf | xfrm }
       OPTIONS := { -V[ersion] | -s[tatistics] | -d[etails] | -r[esolve] |
                    -h[uman-readable] | -iec | -j[son] | -p[retty] |
                    -f[amily] { inet | inet6 | mpls | bridge | link } |
                    -4 | -6 | -M | -B | -0 |
                    -l[oops] { maximum-addr-flush-attempts } | -echo | -br[ief] |
                    -o[neline] | -t[imestamp] | -ts[hort] | -b[atch] [filename] |
                    -rc[vbuf] [size] | -n[etns] name | -N[umeric] | -a[ll] |
                    -c[olor]}
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ ip address
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
       valid_lft forever preferred_lft forever
    inet 10.255.255.254/32 brd 10.255.255.254 scope global lo
       valid_lft forever preferred_lft forever
    inet6 ::1/128 scope host proto kernel_lo
       valid_lft forever preferred_lft forever
2: eth0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1400 qdisc mq state UP group default qlen 1000
    link/ether 00:15:5d:3f:20:d1 brd ff:ff:ff:ff:ff:ff
    altname enx00155d3f20d1
    inet 172.29.197.44/20 brd 172.29.207.255 scope global eth0
       valid_lft forever preferred_lft forever
    inet6 fe80::215:5dff:fe3f:20d1/64 scope link proto kernel_ll
       valid_lft forever preferred_lft forever
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ telnet 172.29.192.1 3306
Command 'telnet' not found, but can be installed with:
sudo apt install inetutils-telnet  # version 2:2.7-2ubuntu1, or
sudo apt install telnet-ssl        # version 0.17.41+really0.17-7
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ apt install telnet
Error: Could not open lock file /var/lib/dpkg/lock-frontend - open (13: Permission denied)
Error: Unable to acquire the dpkg frontend lock (/var/lib/dpkg/lock-frontend), are you root?
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ sudo apt install telnet
Installing:
  telnet

Installing dependencies:
  inetutils-telnet

Summary:
  Upgrading: 0, Installing: 2, Removing: 0, Not Upgrading: 0
  Download size: 111 kB
  Space needed: 319 kB / 1023 GB available

Continue? [Y/n] Y
Get:1 http://archive.ubuntu.com/ubuntu resolute/main amd64 inetutils-telnet amd64 2:2.7-2ubuntu1 [107 kB]
Err:1 http://archive.ubuntu.com/ubuntu resolute/main amd64 inetutils-telnet amd64 2:2.7-2ubuntu1
  File has unexpected size (76 != 107320). Mirror sync in progress? [IP: 104.20.28.246 80]
  Hashes of expected file:
   - SHA512:f1dbf67232c3dc2611ce761e15b114f02f61b0eb8e2c8da54bfb611b71ef1e08be34853a6377be8bae995d1f7caaa5f9b3a9504503c384df17c9f5205bd75bac
   - SHA256:22c6360c8f4469aa2e19c0ba3eae2c0cf13ef4f0a0cff19ccb35cce69f332d84
   - SHA1:b1bba021e2b44ef1ba099c33d6294d0ea0a45a73 [weak]
   - MD5Sum:cc438ebd9a8dc591f56abbaf59250b6e [weak]
   - Filesize:107320 [weak]
Get:2 http://archive.ubuntu.com/ubuntu resolute/main amd64 telnet all 0.17+2.7-2ubuntu1 [3636 B]
Fetched 3636 B in 1s (3427 B/s)
Error: Failed to fetch http://archive.ubuntu.com/ubuntu/pool/main/i/inetutils/inetutils-telnet_2.7-2ubuntu1_amd64.deb  File has unexpected size (76 != 107320). Mirror sync in progress? [IP: 104.20.28.246 80]
   Hashes of expected file:
    - SHA512:f1dbf67232c3dc2611ce761e15b114f02f61b0eb8e2c8da54bfb611b71ef1e08be34853a6377be8bae995d1f7caaa5f9b3a9504503c384df17c9f5205bd75bac
    - SHA256:22c6360c8f4469aa2e19c0ba3eae2c0cf13ef4f0a0cff19ccb35cce69f332d84
    - SHA1:b1bba021e2b44ef1ba099c33d6294d0ea0a45a73 [weak]
    - MD5Sum:cc438ebd9a8dc591f56abbaf59250b6e [weak]
    - Filesize:107320 [weak]
Error: Unable to fetch some archives, maybe run apt update or try with --fix-missing?
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ sudo apt install telnet
Installing:
  telnet

Installing dependencies:
  inetutils-telnet

Summary:
  Upgrading: 0, Installing: 2, Removing: 0, Not Upgrading: 0
  Download size: 107 kB / 111 kB
  Space needed: 319 kB / 1023 GB available

Continue? [Y/n] Y
Get:1 http://archive.ubuntu.com/ubuntu resolute/main amd64 inetutils-telnet amd64 2:2.7-2ubuntu1 [107 kB]
Fetched 107 kB in 1s (148 kB/s)
Selecting previously unselected package inetutils-telnet.
(Reading database ... 44872 files and directories currently installed.)
Preparing to unpack .../inetutils-telnet_2%3a2.7-2ubuntu1_amd64.deb ...
Unpacking inetutils-telnet (2:2.7-2ubuntu1) ...
Selecting previously unselected package telnet.
Preparing to unpack .../telnet_0.17+2.7-2ubuntu1_all.deb ...
Unpacking telnet (0.17+2.7-2ubuntu1) ...
Setting up inetutils-telnet (2:2.7-2ubuntu1) ...
update-alternatives: using /usr/bin/inetutils-telnet to provide /usr/bin/telnet (telnet) in auto mode
Setting up telnet (0.17+2.7-2ubuntu1) ...
Processing triggers for man-db (2.13.1-1build1) ...
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ telnet 172.29.192.1 3306
Trying 172.29.192.1...
Connected to 172.29.192.1.
Escape character is '^]'.
]
J
8.0.36
);o"�>Dx(eH>}:Lcaching_sha2_password
!#08S01Got packets out of orderConnection closed by foreign host.
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ mysql -h 172.29.192.1 -uroot -p200302226lyx
mysql: [Warning] Using a password on the command line interface can be insecure.
ERROR 1045 (28000): Access denied for user 'root'@'172.29.197.44' (using password: YES)
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ mysql -h 172.29.192.1 -uroot -p200302226lyx
mysql: [Warning] Using a password on the command line interface can be insecure.
ERROR 1045 (28000): Access denied for user 'root'@'172.29.197.44' (using password: YES)
(myenv310) yancy@DESKTOP-G5G1IEQ:/mnt/d/桌面/DLT_AIRFLOW-MAIN/dlt_airflow-main$ mysql -h 172.29.192.1 -uroot -p20030226lyx
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 13
Server version: 8.0.36 MySQL Community Server - GPL

Copyright (c) 2000, 2026, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show databases;
+----------------------+
| Database             |
+----------------------+
| airflow              |
| dlt_db               |
| dm                   |
| dwh                  |
| employee_database    |
| employee_database_b  |
| esa_database         |
| information_schema   |
| my_data              |
| my_data_staging      |
| mysql                |
| pbi_builder_database |
| performance_schema   |
| rest_api_data        |
| scd_2                |
| scd_2_1              |
| sql_hr               |
| sql_inventory        |
| sql_store            |
| sys                  |
+----------------------+
20 rows in set (0.01 sec)

mysql>