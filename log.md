# HKUST Independ Project

### Personal Log

### SONG Chen

---

- 2024.9.20, 为了给虚拟机扩容，删除所有快照，记录快照内容备用
  
  1. 2024.9.4 快照2：
     配置了虚拟机的 SSH，vim，apt 等基础库
  
  2. 2024.9.4 快照3：
     准备好了 TPC-H 的 dbgen 和 makefile，还没有开始编译
  
  3. 2024.9.8 快照6：
     在安装SQLServer过程中，遇到 *bad header line bad header data* 报错，无法运行 apt-get update 命令。
     
     解决办法：注释掉配置文件中SQLServer的内容后，成功运行 apt-get update，再将注释内容重新添加回文件
  
  4. 2024.9.9 快照8：
     通过dbeaver，成功向SQLServer中导入了TPC-H生成的数据
     
      遇到字段重复的 bug，更新 dbeaver， 解决
     
      遇到表格首行不显示的问题，通过向所有表格中插入一个空行，解决
  
  5. 2024.9.18 快照9：
     完成了 docker 的部署，拉取了 Flink 镜像，编写了 docker-compose.yml 文件，准备启动 Flink on docker 集群
     
     最新的启动指令似乎有变化。docker-compose up 命令改为了 docker compose up？

---

- 2024.9.27，启动flink集群，尝试用本地idea连接flink集群和SQLServer数据库

- 2024.10.16，启动docker compose时发生了/opt/flink文件夹所属文件系统只读的问题，在解决过程中，不够了解Linux的挂载机制，导致将/opt/flink所属的/dev/ads3文件系统挂载到了根目录 / 上，无法修复，只好选择重装虚拟机。

- 2024.10.25，配置了虚拟机到集群节点的挂载路径：/opt/cluster/share : /opt/src。
  
  进入docker compose的jobmanager node:
  
  docker exec -it jobmanager /bin/bash
  
  提交任务：
  
  flink run /path/to/your/file/flink-test.jar

- 遇到报错，虚拟机java版本与打包时的Maven的java版本不匹配，需要调整版本至一致。

---

- 11.10，继续调整版本问题，发现是pom的<properties>配置成了21，导致**覆盖了**后面所有的版本控制。在调整之后需要运行 mvn clean package
- 在GitHub找到了别人写的TPC-H Query3查询程序，对照代码学习

--- 

- 11.18，学习概念
- 算子：flink中用来处理数据流的基本构造块，是对数据流进行转换、处理和操作的函数或方法。例如转换算子map, filter, flatMap，聚合算子reduce，aggregate，连接算子connect，union，窗口算子window
- 每个算子有多个任务，这些任务可能分布在不同的实例上。经常需要记录数据流在不同实例上的状态。
- flink的状态是由算子的子任务来创建和管理的，算子子任务接受任务输入流，获取状态，更新状态，给出输出流

- 11.27，学习概念
- ValueState<set<payload>> aliveset语句，定义了一个状态集合aliveSet，其中存储的是payload的集合（set）。换句话说，被存储在aliveSet中的payload都拥有共同的状态alive（由open函数指定）
- keyBy函数的作用是将输入进来的元素按照key分组，**具有相同key的元素会访问同一个ValueState**。相当于借此实现了join的功能