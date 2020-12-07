<details>
<summary>点击展开目录</summary>
<!-- TOC -->


<!-- /TOC -->
</details>

```bash
// 在线安装
wget https://archive.cloudera.com/cm5/redhat/7/x86_64/cm/cloudera-manager.repo
mv cloudera-manager.repo /etc/yum.repos.d/
sudo yum install cloudera-manager-daemons cloudera-manager-server
sudo yum install cloudera-manager-agent cloudera-manager-daemons
```

```bash
// 离线安装
yum --nogpgcheck localinstall cloudera-manager-daemons-*.rpm cloudera-manager-server-*.rpm
yum --nogpgcheck localinstall cloudera-manager-agent-*.rpm cloudera-manager-daemons
```

```bash
// 启动
sudo service cloudera-scm-agent start
sudo service cloudera-scm-server start
```

```bash
cp mysql-connector-java-8.0.16.jar /usr/share/java/mysql-connector-java.jar
```

cat /var/log/cloudera-scm-server/*.log
