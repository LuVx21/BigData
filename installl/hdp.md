<details>
<summary>点击展开目录</summary>
<!-- TOC -->

- [准备](#准备)
- [安装](#安装)
- [阅读](#阅读)

<!-- /TOC -->
</details>

## 准备

```bash
yum install -y yum-utils createrepo
```

更新yum源
```bash
mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup
wget -O /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo
yum clean all
yum makecache
```

修改主机名
```bash
sudo hostnamectl set-hostname test-bg-xx
sudo hostname test-bg-xx
```

修改时区
```bash
cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
```


同步集群时间

```bash
yum install –y ntp
ntpdate -u cn.pool.ntp.org
# 或:
ntpdate time1.aliyun.com

# 设置硬件时间和系统时间同步:
clock -w
# 配置自动同步:/etc/crontab写入
*/1 * * * * root ntpdate -u cn.pool.ntp.org >> /dev/null 2>&1
```

关闭防火墙

```bash
systemctl stop firewalld
systemctl disable firewalld
```

关闭selinux
```bash
# 临时关闭
setenforce 0
# 修改配置
sed -i 's#SELINUX=enforcing#SELINUX=disabled#g' /etc/sysconfig/selinux
```

```bash
sudo yum -y install httpd
sudo systemctl enable httpd
sudo systemctl start httpd
```

## 安装


| 名称                               | 地址                                                         |
| :--------------------------------- | :----------------------------------------------------------- |
| ambari-2.7.4.0-centos7.tar.gz      | [ambari/centos7/2.x/updates/2.7.4.0/ambari-2.7.4.0-centos7.tar.gz](http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.4.0/ambari-2.7.4.0-centos7.tar.gz) |
| ambari.repo                        | [ambari/centos7/2.x/updates/2.7.4.0/ambari.repo](http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.4.0/ambari.repo) |
| HDP-3.1.4.0-centos7-rpm.tar.gz     | [HDP/centos7/3.x/updates/3.1.4.0/HDP-3.1.4.0-centos7-rpm.tar.gz](http://public-repo-1.hortonworks.com/HDP/centos7/3.x/updates/3.1.4.0/HDP-3.1.4.0-centos7-rpm.tar.gz) |
| hdp.repo                           | [HDP/centos7/3.x/updates/3.1.4.0/hdp.repo](http://public-repo-1.hortonworks.com/HDP/centos7/3.x/updates/3.1.4.0/hdp.repo) |
| HDP-GPL-3.1.4.0-centos7-gpl.tar.gz | [HDP-GPL/centos7/3.x/updates/3.1.4.0/HDP-GPL-3.1.4.0-centos7-gpl.tar.gz](http://public-repo-1.hortonworks.com/HDP-GPL/centos7/3.x/updates/3.1.4.0/HDP-GPL-3.1.4.0-centos7-gpl.tar.gz) |
| hdp.gpl.repo                       | [HDP-GPL/centos7/3.x/updates/3.1.4.0/hdp.gpl.repo](http://public-repo-1.hortonworks.com/HDP-GPL/centos7/3.x/updates/3.1.4.0/hdp.gpl.repo) |
| HDP-UTILS-1.1.0.22-centos7.tar.gz  | [HDP-UTILS-1.1.0.22/repos/centos7/HDP-UTILS-1.1.0.22-centos7.tar.gz](http://public-repo-1.hortonworks.com/HDP-UTILS-1.1.0.22/repos/centos7/HDP-UTILS-1.1.0.22-centos7.tar.gz) |

[版本](https://supportmatrix.hortonworks.com/)


```bash
ambari-server setup
ambari-server start
```

## 阅读


[docs](https://docs.cloudera.com/HDPDocuments/Ambari/Ambari-2.7.5.0/index.html)
