# springboot + storm + kafka + python(小波分解、神经网络) + hbase + redis + socket实现流式计算
------

## 业务流程

1、storm消费kafka中的数据，通过sensorType这个字段进行fieldGroupping，确保同一类型的数据在一个bolt task中处理。

2、同一类型的数据累积一定数量后，调用python进程(wavelet和tensorflow)去计算结果。

3、拿到小波分析计算得到的数据列表后，会写入到hbase数据库中，然后让项目启动时创建的socket client把数据发送给socket server(参考springboot-socket这个项目)，socket server会把计算结果实时发送给浏览器客户端。

4、tensorflow计算得到结果后，或写入到redis缓存中，供其他服务查询。